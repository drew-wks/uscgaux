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
import os
import pandas as pd
from env_config import set_env_vars
from library_utils import compute_pdf_id, safe_append_rows_to_sheet, validate_core_metadata
from google_utils import upload_pdf, fetch_sheet_as_df
from log_writer import log_event


set_env_vars()


def propose_new_files(drive_client, sheets_client, uploaded_files):
    """
    Processes and uploads Streamlit-uploaded PDF files, checking for duplicates and appending metadata.

    Args:
        uploaded_files (List[UploadedFile]): List of uploaded PDF files from Streamlit.

    Returns:
        tuple: (new_rows_df, failed_files, duplicate_files)
    """
    
    library_unified_df = fetch_sheet_as_df(sheets_client,os.environ["LIBRARY_UNIFIED"])
    library_unified_df['pdf_id'] = library_unified_df['pdf_id'].astype(str)

    failed_files = []
    duplicate_files = []
    collected_rows = []

    for uploaded_file in uploaded_files:
        file_name = uploaded_file.name
        pdf_id = compute_pdf_id(uploaded_file)

        if pdf_id is None:
            logging.warning(f"Could not compute pdf_id for: {file_name}")
            failed_files.append(file_name)
            continue

        if pdf_id in library_unified_df["pdf_id"].values:
            logging.warning(f"Duplicate PDF ID detected: {pdf_id} ({file_name})")
            log_event("duplicate_skipped", pdf_id, file_name)
            duplicate_files.append(file_name)
            continue
        
        # Upload to Drive
        uploaded_file.seek(0)  # Ensure pointer is at the start
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

        log_event("new_pdf_to_PDF_TAGGING", pdf_id, file_name)

    new_rows_df = pd.DataFrame(collected_rows)

    if not new_rows_df.empty:
        safe_append_rows_to_sheet(sheets_client, os.environ["LIBRARY_UNIFIED"], new_rows_df)

    if not validate_core_metadata(new_rows_df):
            logging.warning(f"Incomplete metadata for {file_name}  ID: {pdf_id}. Skipping promotion.")

    return new_rows_df, failed_files, duplicate_files

