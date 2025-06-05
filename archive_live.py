
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
from google_utils import move_pdf, fetch_sheet
from library_utils import safe_append_rows_to_sheet, remove_row
from env_config import set_env_vars
from log_writer import log_event


set_env_vars() # needed for local testing


def archive_live(drive_client, sheets_client):

    try:
        library_unified_df = fetch_sheet(sheets_client, os.environ["LIBRARY_UNIFIED"])
        library_unified_df["pdf_id"] = library_unified_df["pdf_id"].astype(str)
    except Exception as e:
        logging.error(f"Failed to load LIBRARY_UNIFIED: {e}")
        return

    rows_to_archive = library_unified_df[library_unified_df["status"] == "live_for_deletion"]

    for i, row in rows_to_archive.iterrows():
        pdf_id = row.get("pdf_id", "[unknown]")
        file_id = row.get("google_id")
        filename = row.get("filename")

        try:
            move_pdf(drive_client, file_id, os.environ["PDF_ARCHIVE"])
        except Exception as e:
            logging.error(f"Failed to move file {file_id}: {e}")
            continue

        add_row(
            sheets_client,
            spreadsheet_id=os.environ["LIBRARY_ARCHIVE"],
            tab_name="Sheet1",
            row_data=row,
            extra_columns=[datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")]
        )

        remove_row(
            sheets_client,
            spreadsheet_id=os.environ["LIBRARY_UNIFIED"],
            tab_name="Sheet1",
            row_index=i
        )

        log_event("archived", pdf_id, filename)

