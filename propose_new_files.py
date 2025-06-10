# propose_new_files.py

"""
Agent: propose_new_files.py

Triggered when user uploads new files  using the streamlit utility
- Computes metadata (pdf_id, pdf_file_name, etc.)
- Checks for duplicates in LIBRARY_UNIFIED
- If unique, appends row to LIBRARY_UNIFIED with status 'new_for_tagging'
- If duplicate, deletes file from PDF_TAGGING and logs event
"""


import os
from datetime import datetime, timezone
import logging
import pandas as pd
from typing import IO, List
from streamlit.runtime.uploaded_file_manager import UploadedFile
from gspread.client import Client as SheetsClient
from googleapiclient.discovery import Resource as DriveClient
from library_utils import compute_pdf_id, find_duplicates_against_reference, validate_core_metadata_format
from gcp_utils import upload_pdf, fetch_sheet_as_df
from log_writer import log_event


set_env_vars()


def propose_new_files(drive_client: DriveClient, sheets_client: SheetsClient, uploaded_files: List[IO[bytes]]):
    """
    Processes and uploads Streamlit-uploaded PDF files, checking for duplicates and appending metadata.

    Args:
        uploaded_files (List[UploadedFile]): List of uploaded PDF files. Uploaded_files must be objects with .name and .read() methods. Most file-like objects, including io.BytesIO, tempfile, and UploadedFile, and Streamlit's UploadedFile satisfy this.

    Returns:
        tuple: (new_rows_df, failed_files, duplicate_files)
    """

    try:
        library_unified_df = fetch_sheet_as_df(sheets_client, os.environ["LIBRARY_UNIFIED"])
        library_unified_df['pdf_id'] = library_unified_df['pdf_id'].astype(str)
    except Exception as e:
        logging.error(f"❌ Failed to fetch or process LIBRARY_UNIFIED sheet: {e}")
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
            logging.warning(f"⚠️ {e} for file: {file_name}")
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
        logging.error(f"❌ Failed during duplicate check: {e}")
        return pd.DataFrame(), [file.name for file in uploaded_files], []

    # Step 3: Log duplicates
    for file_name, (pdf_id, _) in file_map.items():
        if pdf_id in duplicate_pdf_ids:
            reason = f"Duplicate detected: pdf_id '{pdf_id}' already exists in LIBRARY_UNIFIED ({file_name})"
            logging.warning(reason)
            try:
                log_event(sheets_client, "duplicate_skipped", pdf_id, file_name, extra_columns=[reason])
            except Exception as log_error:
                logging.error(f"⚠️ Failed to log duplicate_skipped event: {log_error}")
            duplicate_files.append(file_name)

    # Step 4: Upload and collect new rows
    for file_name, (pdf_id, uploaded_file) in file_map.items():
        if pdf_id in duplicate_pdf_ids:
            continue  # Already handled

        try:
            uploaded_file.seek(0)
            file_id = upload_pdf(drive_client, uploaded_file, file_name, os.environ["PDF_TAGGING"])
            file_link = f"https://drive.google.com/file/d/{file_id}/view"

            collected_rows.append({
                "pdf_id": pdf_id,
                "google_id": file_id,
                "link": file_link,
                "pdf_file_name": file_name,
                "status": "new_for_tagging",
                "status_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            })

            log_event(sheets_client, "new_pdf_to_PDF_TAGGING", pdf_id, file_name)

        except Exception as e:
            logging.error(f"❌ Failed to process {file_name}: {e}")
            failed_files.append(file_name)

    # Step 5: Write to sheet
    new_rows_df = pd.DataFrame(collected_rows)
    if not new_rows_df.empty:
        try:
            safe_append_rows_to_sheet(sheets_client, os.environ["LIBRARY_UNIFIED"], new_rows_df)
        except Exception as e:
            logging.error(f"❌ Failed to append rows to LIBRARY_UNIFIED: {e}")
            failed_files.extend(new_rows_df["pdf_file_name"].tolist())
            new_rows_df = pd.DataFrame()

    # Step 6: Check metadata formatting
    try:
        if not validate_core_metadata_format(new_rows_df):
            logging.warning("⚠️ One or more rows have incomplete metadata in new_rows_df.")
    except Exception as e:
        logging.error(f"❌ Failed during metadata validation: {e}")

    return new_rows_df, failed_files, duplicate_files
