# propose_new_files.py

"""
Agent: propose_new_files.py

Triggered when user uploads new files  using the streamlit utility
- Computes metadata (pdf_id, filename, etc.)
- Checks for duplicates in LIBRARY_UNIFIED
- If unique, appends row to LIBRARY_UNIFIED with status 'new_for_tagging'
- If duplicate, deletes file from PDF_TAGGING and logs event
"""


from datetime import datetime
import logging
import os
from dotenv import load_dotenv
import pandas as pd
from library_utils import compute_pdf_id, safe_append_rows_to_sheet
from google_utils import get_gcp_clients, upload_file_to_drive
from admin_config import load_qdrant_secrets, CONFIG, ENV_PATH
from log_writer import log_admin_event


load_dotenv(ENV_PATH)  # needed for local testing


def propose_new_files(uploaded_files):
    """
    Processes and uploads Streamlit-uploaded PDF files, checking for duplicates and appending metadata.

    Args:
        uploaded_files (List[UploadedFile]): List of uploaded PDF files from Streamlit.

    Returns:
        tuple: (new_rows_df, failed_files, duplicate_files)
    """
    
    drive_client, sheets_client = get_gcp_clients()
    
    df_library_unified = sheets_client.open_by_key(os.environ["LIBRARY_UNIFIED"]).worksheet("Sheet1").get_all_records()
    df_library_unified = pd.DataFrame(df_library_unified)
    df_library_unified['pdf_id'] = df_library_unified['pdf_id'].astype(str)

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

        if pdf_id in df_library_unified["pdf_id"].values:
            logging.warning(f"Duplicate PDF ID detected: {pdf_id} ({file_name})")
            log_admin_event("duplicate_skipped", pdf_id, file_name)
            duplicate_files.append(file_name)
            continue
        
        # Upload to Drive
        uploaded_file.seek(0)  # Ensure pointer is at the start
        file_id = upload_file_to_drive(drive_client, uploaded_file, file_name, os.environ["PDF_TAGGING"])
        file_link = f"https://drive.google.com/file/d/{file_id}/view"

        collected_rows.append({
            "pdf_id": pdf_id,
            "google_id": file_id,
            "link": file_link,
            "pdf_file_name": file_name,
            "status": "new_for_tagging",
            "status_timestamp": datetime.now().isoformat()
        })
        
        log_admin_event("new_pdf_to_PDF_TAGGING", pdf_id, file_name)
        
    new_rows_df = pd.DataFrame(collected_rows)


    if not new_rows_df.empty:
        safe_append_rows_to_sheet(sheets_client, os.environ["LIBRARY_UNIFIED"], new_rows_df)

    return new_rows_df, failed_files, duplicate_files

