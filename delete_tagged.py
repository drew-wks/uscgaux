
"""
Agent: delete_tagged.py

Removes files from system:
- Finds rows: LIBRARY_UNIFIED entries whose status contains "deletion"
- Finds files: PDFs in PDF_TAGGING or PDF_LIVE with matching pdf_ids in LIBRARY_UNIFIED
- Finds qdrant records: records in Qdrant with matching pdf_ids in LIBRARY_UNIFIED
- Deletes rows, files and records 
- Logs findings to ADMIN_EVENT_LOG
"""

import os
import pandas as pd
import logging
from env_config import RAG_CONFIG
from library_utils import fetch_rows_by_status, remove_rows
from gcp_utils import get_folder_name, fetch_sheet_as_df
from qdrant_utils import get_qdrant_client, delete_record_by_pdf_id
from log_writer import log_event


    
TARGET_STATUSES = ["deletion"]


def delete_tagged(drive_client: DriveClient, sheets_client: SheetsClient):

    df = fetch_sheet_as_df(sheets_client, os.environ["LIBRARY_UNIFIED"])
    df["pdf_id"] = df["pdf_id"].astype(str)
    
    
    # --- Find ROWS marked for deletion ---
    rows_to_delete = fetch_rows_by_status(df, TARGET_STATUSES)
    if rows_to_delete.empty:
        logging.info("No rows marked for deletion.")
        return

    pdf_ids_to_delete = rows_to_delete["pdf_id"].astype(str).str.strip().tolist()
    row_indices_to_delete = df[df["pdf_id"].isin(pdf_ids_to_delete)].index.tolist()


    # --- Find & Delete FILES ---
    for _, row in rows_to_delete.iterrows():
        file_id = row.get("google_id")
        pdf_id = row.get("pdf_id")
        filename = row.get("pdf_file_name", "unknown_file.pdf")

        if not file_id:
            logging.warning(f"Missing google_id for {pdf_id}. Skipping file deletion.")
            continue
        
        folder_name = get_folder_name(drive_client, file_id)

        try:
            drive_client.files().delete(fileId=file_id).execute()
            log_event(f"file_deleted from {folder_name}", pdf_id, filename)
        except Exception as e:
            logging.warning(f"Failed to delete file {filename} (ID: {file_id}): {e}")


    # --- Find & Delete RECORDS ---
    qdrant_client = get_qdrant_client(RAG_CONFIG["qdrant_location"])
    for pdf_id in pdf_ids_to_delete:
        delete_record_by_pdf_id(qdrant_client, RAG_CONFIG.get("qdrant_collection_name"), pdf_id)


    # --- DELETE ROWS ---
    remove_rows(sheets_client, os.environ["LIBRARY_UNIFIED"], row_indices_to_delete)

    for _, row in rows_to_delete.iterrows():
        pdf_id = row["pdf_id"]
        filename = row.get("pdf_file_name", "unknown_file.pdf")
        log_event("row_deleted", pdf_id, filename)

    return rows_to_delete

