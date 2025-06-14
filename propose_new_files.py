# propose_new_files.py

"""
Agent: propose_new_files.py

Triggered when user uploads new files  using the streamlit utility
- Computes metadata (pdf_id, pdf_file_name, etc.)
- Checks for duplicates in LIBRARY_UNIFIED
- If unique, appends row to LIBRARY_UNIFIED with status 'new_for_tagging'
- If duplicate, deletes file from PDF_TAGGING and logs event
"""


from datetime import datetime, timezone
import logging
import pandas as pd
from typing import List, Protocol, runtime_checkable
from gspread.client import Client as SheetsClient
from googleapiclient.discovery import Resource as DriveClient
from env_config import env_config
from library_utils import compute_pdf_id, find_duplicates_against_reference, validate_core_metadata_format, append_new_rows
from gcp_utils import upload_pdf, fetch_sheet_as_df
from log_writer import log_event

config = env_config()

@runtime_checkable
class FileLike(Protocol):
    """
    A protocol representing a file-like object with a binary read method and a name attribute.

    Any object implementing this interface must have:
    - a `read()` method that returns bytes
    - a `name` attribute (typically a filename as a string)

    This is designed to support flexible file input handling, including:
    - Streamlit's UploadedFile
    - io.BytesIO objects with a `.name` attribute
    - tempfile.NamedTemporaryFile
    - Other custom classes simulating file upload behavior
    """
    def read(self) -> bytes: ...
    name: str
    
    
def propose_new_files(drive_client: DriveClient, sheets_client: SheetsClient, uploaded_files: List[FileLike]):
    """
    Processes uploaded PDF files, checking for duplicates and appending metadata to the system.

    Args:
        drive_client (DriveClient): Authenticated Google Drive client.
        sheets_client (SheetsClient): Authenticated Google Sheets client.
        uploaded_files (List[FileLike]): List of file-like objects representing PDF uploads.
            Each object must have a `.read()` method that returns bytes and a `.name` attribute.
            Compatible with Streamlit's UploadedFile, io.BytesIO (with `name`), tempfile.NamedTemporaryFile, etc.

    Returns:
        tuple: A tuple of three elements:
            - new_rows_df (DataFrame): Metadata rows for newly proposed PDFs.
            - failed_files (List[FileLike]): Files that failed validation or upload.
            - duplicate_files (List[FileLike]): Files already present in the system.
    """

    try:
        library_unified_df = fetch_sheet_as_df(sheets_client, config["LIBRARY_UNIFIED"])
        library_unified_df['pdf_id'] = library_unified_df['pdf_id'].astype(str)
    except Exception as e:
        logging.error("❌ Failed to fetch or process LIBRARY_UNIFIED sheet: %s", e)
        return pd.DataFrame(), [file.name for file in uploaded_files], []

    failed_files = []
    duplicate_files = []
    collected_rows = []

    # Step 1: Precompute pdf_ids for all uploaded files
    file_map = {}
    for uploaded_file in uploaded_files:
        file_name = uploaded_file.name
        try:
            pdf_id = compute_pdf_id(uploaded_file)
            if not pdf_id:
                raise ValueError("Could not compute pdf_id")
            file_map[file_name] = (pdf_id, uploaded_file)
        except Exception as e:
            logging.warning("⚠️ %s for file: %s", e, file_name)
            failed_files.append(file_name)

    # Step 2: Batch check for duplicates
    fields_to_check = [{"pdf_id": pdf_id} for pdf_id, _ in file_map.values()]
    duplicate_pdf_ids = set()
    try:
        duplicate_rows = find_duplicates_against_reference(
            df_to_check=pd.DataFrame(fields_to_check),
            reference_df=library_unified_df
        )
        duplicate_pdf_ids = set(duplicate_rows["pdf_id"]) if not duplicate_rows.empty else set()
    except Exception as e:
        logging.error("❌ Failed during duplicate check: %s", e)
        return pd.DataFrame(), [file.name for file in uploaded_files], []

    # Step 3: Log duplicates
    for file_name, (pdf_id, _) in file_map.items():
        if pdf_id in duplicate_pdf_ids:
            reason = f"Duplicate detected: pdf_id '{pdf_id}' already exists in LIBRARY_UNIFIED ({file_name})"
            logging.warning(reason)
            try:
                log_event(
                    sheets_client,
                    "duplicate_skipped",
                    str(pdf_id),
                    str(file_name),
                    extra_columns=[reason],
                )
            except Exception as log_error:
                logging.error("⚠️ Failed to log duplicate_skipped event: %s", log_error)
            duplicate_files.append(file_name)

    # Step 4: Upload and collect new rows
    for file_name, (pdf_id, uploaded_file) in file_map.items():
        if pdf_id in duplicate_pdf_ids:
            continue  # Already handled

        try:
            uploaded_file.seek(0)
            file_id = upload_pdf(drive_client, uploaded_file, file_name, config["PDF_TAGGING"])
            file_link = f"https://drive.google.com/file/d/{file_id}/view"

            collected_rows.append({
                "pdf_id": pdf_id,
                "gcp_file_id": file_id,
                "link": file_link,
                "pdf_file_name": file_name,
                "status": "new_for_tagging",
                "status_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            })

            log_event(
                sheets_client,
                "new_pdf_to_PDF_TAGGING",
                str(pdf_id),
                str(file_name),
            )

        except Exception as e:
            logging.error("❌ Failed to process %s: %s", file_name, e)
            failed_files.append(file_name)

    # Step 5: Write to sheet
    new_rows_df = pd.DataFrame(collected_rows)
    if not new_rows_df.empty:
        try:
            append_new_rows(sheets_client, config["LIBRARY_UNIFIED"], new_rows_df)
        except Exception as e:
            logging.error("❌ Failed to append rows to LIBRARY_UNIFIED: %s", e)
            failed_files.extend(new_rows_df["pdf_file_name"].tolist())
            new_rows_df = pd.DataFrame()

    # Step 6: Check metadata formatting
    try:
        missing_columns = validate_core_metadata_format(new_rows_df)
        if missing_columns:
            logging.warning("⚠️ New rows are missing required metadata columns: %s. Please fix the sheet structure before continuing.", missing_columns)
        else:
            logging.info("✅ Metadata validation passed.")
    except Exception as e:
        logging.error("❌ Failed during metadata validation: %s", e)

    return new_rows_df, failed_files, duplicate_files
