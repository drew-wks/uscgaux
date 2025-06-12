
"""
Agent: archive_live.py

Archives rows with status = 'live_for_deletion':
- Moves PDF from PDF_LIVE to PDF_ARCHIVE
- Removes associated records from Qdrant
- Copies row from LIBRARY_UNIFIED to LIBRARY_ARCHIVE
- Removes row from LIBRARY_UNIFIED
- Logs all actions
"""

import os
import logging
from datetime import datetime, timezone
import pandas as pd
from gspread.client import Client as SheetsClient
from googleapiclient.discovery import Resource as DriveClient
from qdrant_client import QdrantClient
from env_config import rag_config, env_config
from gcp_utils import move_pdf, fetch_sheet_as_df
from qdrant_utils import delete_records_by_pdf_id
from library_utils import fetch_rows_by_status, remove_rows, append_new_rows
from log_writer import log_event

config = env_config()

def archive_tagged(
    drive_client: DriveClient,
    sheets_client: SheetsClient,
    qdrant_client: QdrantClient
) -> pd.DataFrame:
    """
    Archives files and rows marked for deletion. Moves PDFs to PDF_ARCHIVE,
    logs the action, removes Qdrant records, and removes rows from LIBRARY_UNIFIED.
    """
    TARGET_STATUSES = ["live_for_archive"]

    library_df = fetch_sheet_as_df(sheets_client, config["LIBRARY_UNIFIED"])
    
    # --- Find ROWS marked for archiving ---
    rows_to_archive = fetch_rows_by_status(library_df, TARGET_STATUSES)
    if rows_to_archive.empty:
        logging.info("No rows marked for archive. No further action taken.")
        return pd.DataFrame()

    archived_rows = []

    for i, row in rows_to_archive.iterrows():
        pdf_id = row.get("pdf_id", "[unknown]")
        file_id = row.get("google_id")
        filename = row.get("pdf_file_name", "unknown_file.pdf")
        row_index = library_df[library_df["pdf_id"] == pdf_id].index.tolist()

        # --- MOVE FILE ---
        move_pdf(drive_client, file_id, config["PDF_ARCHIVE"])

         # --- DELETE RECORD ---
        delete_records_by_pdf_id(qdrant_client, rag_config("qdrant_collection_name"), pdf_id)

        # --- MOVE ROW ---
        row["timestamp_archived"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        append_new_rows(
            sheets_client,
            spreadsheet_id=config["LIBRARY_ARCHIVE"],
            new_rows_df=pd.DataFrame([row]),
            sheet_name="Sheet1"
        )

        try:
            remove_rows(
                sheets_client,
                spreadsheet_id=config["LIBRARY_UNIFIED"],
                row_indices=row_index
            )
        except Exception as e:
            logging.error("Failed to remove row %s for %s: %s", i, pdf_id, e)

        archived_rows.append(row)
        log_event(sheets_client, "archived", pdf_id, filename)

    return pd.DataFrame(archived_rows)
