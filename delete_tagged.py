import os
import logging
from gspread.client import Client as SheetsClient
from googleapiclient.discovery import Resource as DriveClient
from qdrant_client import QdrantClient
from env_config import rag_config
from library_utils import fetch_rows_by_status, remove_rows
from gcp_utils import get_folder_name, fetch_sheet_as_df
from qdrant_utils import delete_records_by_pdf_id
from log_writer import log_event
from env_config import env_config

config = env_config()



def delete_tagged(drive_client: DriveClient, sheets_client: SheetsClient, qdrant_client: QdrantClient):
    TARGET_STATUSES = ["deletion"]

    library_df = fetch_sheet_as_df(sheets_client, config["LIBRARY_UNIFIED"])
    rows_to_delete = fetch_rows_by_status(library_df, TARGET_STATUSES)

    if rows_to_delete.empty:
        logging.info("No rows marked for deletion. No further action taken.")
        return

    deleted_rows = []

    for i, row in rows_to_delete.iterrows():
        file_id = row.get("google_id")
        pdf_id = row.get("pdf_id")
        filename = row.get("pdf_file_name", "unknown_file.pdf")
        original_status = row.get("status", "unknown_status")
        row_index = library_df[library_df["pdf_id"] == pdf_id].index.tolist()

        # --- DELETE FILE ---
        try:
            folder_name = get_folder_name(drive_client, file_id)
            drive_client.files().delete(fileId=file_id).execute()
            log_event(sheets_client, f"file_deleted from {folder_name}", pdf_id, filename, extra_columns=[original_status])
        except Exception as e:
            logging.warning("Failed to delete file %s (ID: %s): %s", filename, file_id, e)
            folder_name = "unknown_folder"

        # --- DELETE QDRANT RECORD ---
        if original_status.startswith("new_for_deletion"):
            logging.info("Skipping Qdrant deletion for new record: %s", pdf_id)
        else:
            try:
                delete_records_by_pdf_id(qdrant_client, rag_config("qdrant_collection_name"), pdf_id)
            except Exception as e:
                logging.warning("Failed to delete Qdrant record for %s: %s", pdf_id, e)

        # --- DELETE ROW FROM SHEET ---
        try:
            remove_rows(sheets_client, config["LIBRARY_UNIFIED"], row_indices=row_index)
            log_event(sheets_client, "deleted", pdf_id, filename, extra_columns=[original_status, folder_name])
        except Exception as e:
            logging.error("Failed to remove row for %s: %s", pdf_id, e)

        deleted_rows.append(row)

    return rows_to_delete


