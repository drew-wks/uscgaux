import logging
from datetime import datetime, timezone
from typing import List

import pandas as pd
from gspread.client import Client as SheetsClient
from googleapiclient.discovery import Resource as DriveClient
from qdrant_client import QdrantClient

from env_config import rag_config, env_config
from utils.gcp_utils import move_file, fetch_sheet_as_df
from utils.qdrant_utils import delete_records_by_pdf_id
from utils.library_utils import fetch_rows_by_status, remove_rows, append_new_rows
from utils.log_writer import log_event


config = env_config()


def archive_tagged(
    drive_client: DriveClient,
    sheets_client: SheetsClient,
    qdrant_client: QdrantClient,
) -> pd.DataFrame:
    """Archive rows and PDFs marked for archiving.

    Moves the corresponding PDF to ``PDF_ARCHIVE``, deletes Qdrant records,
    appends the row to ``LIBRARY_ARCHIVE`` and removes it from ``LIBRARY_UNIFIED``.
    """
    target_statuses: List[str] = ["live_for_archive"]

    library_df = fetch_sheet_as_df(sheets_client, config["LIBRARY_UNIFIED"])
    rows_to_archive = fetch_rows_by_status(library_df, target_statuses)
    if rows_to_archive.empty:
        logging.info("No rows marked for archive. No further action taken.")
        return pd.DataFrame()

    archived_rows = []

    for i, row in rows_to_archive.iterrows():
        pdf_id = row.get("pdf_id", "[unknown]")
        file_id = row.get("gcp_file_id")
        filename = row.get("pdf_file_name", "unknown_file.pdf")
        row_index = library_df[library_df["pdf_id"] == pdf_id].index.tolist()

        move_file(drive_client, file_id, config["PDF_ARCHIVE"])
        delete_records_by_pdf_id(
            qdrant_client,
            rag_config("qdrant_collection_name"),
            pdf_id,
        )

        row["timestamp_archived"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        append_new_rows(
            sheets_client,
            spreadsheet_id=config["LIBRARY_ARCHIVE"],
            new_rows_df=pd.DataFrame([row]),
            sheet_name="Sheet1",
        )

        try:
            remove_rows(
                sheets_client,
                spreadsheet_id=config["LIBRARY_UNIFIED"],
                row_indices=row_index,
            )
        except Exception as e:  # pragma: no cover - log only
            logging.error("Failed to remove row %s for %s: %s", i, pdf_id, e)

        archived_rows.append(row)
        log_event(sheets_client, "archived", str(pdf_id), str(filename))

    return pd.DataFrame(archived_rows)
