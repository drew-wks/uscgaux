import logging
from gspread.client import Client as SheetsClient
from googleapiclient.discovery import Resource as DriveClient
from qdrant_client import QdrantClient
from env_config import rag_config, env_config
from library_utils import fetch_rows_by_status, remove_rows
from gcp_utils import get_folder_name, fetch_sheet_as_df, file_exists
from qdrant_utils import delete_records_by_pdf_id, in_qdrant
from log_writer import log_event

config = env_config()



def delete_tagged(drive_client: DriveClient, sheets_client: SheetsClient, qdrant_client: QdrantClient):
    TARGET_STATUSES = ["deletion"]

    library_df = fetch_sheet_as_df(sheets_client, config["LIBRARY_UNIFIED"])
    rows_to_delete = fetch_rows_by_status(library_df, TARGET_STATUSES)

    if rows_to_delete.empty:
        logging.info("No rows marked for deletion. No further action taken.")
        return

    duplicates_df = library_df[library_df.duplicated(subset="pdf_id", keep=False)]
    
    if not duplicates_df.empty:
        logging.error(f"❌ Rows with duplicate pdf_ids found. Handle/remove duplicates before attempting deletion. No further action taken.{duplicates_df}")
        return
    
    deleted_rows = []

    for i, row in rows_to_delete.iterrows():
        file_id = str(row.get("gcp_file_id", ""))
        pdf_id = str(row.get("pdf_id", ""))
        filename = str(row.get("pdf_file_name", "unknown_file"))
        original_status = str(row.get("status", "unknown_status"))

        # --- DELETE FILE ---
        folder_name = "unknown_folder"
        if file_exists(drive_client, file_id):
            folder_name = get_folder_name(drive_client, file_id)
            try:
                drive_client.files().delete(fileId=file_id).execute()
                log_event(
                    sheets_client,
                    f"file_deleted from {folder_name}",
                    pdf_id,
                    filename,
                    extra_columns=[original_status],
                )
            except Exception as e:
                logging.warning(
                    "Failed to delete file %s (ID: %s): %s", filename, file_id, e
                )
        else:
            logging.info("File ID %s not found in Drive. Skipping deletion.", file_id)

        # --- DELETE QDRANT RECORD ---
        collection_name = rag_config("qdrant_collection_name")
        if original_status.startswith("new_for_deletion"):
            logging.info("Skipping Qdrant deletion for new record: %s", pdf_id)
        else:
            if in_qdrant(qdrant_client, collection_name, pdf_id):
                try:
                    delete_records_by_pdf_id(qdrant_client, [pdf_id], collection_name)
                except Exception as e:
                    logging.warning(
                        "Failed to delete Qdrant record for %s: %s", pdf_id, e
                    )
            else:
                logging.info(
                    "PDF ID %s not found in Qdrant. Skipping deletion.", pdf_id
                )

        # --- DELETE ROW FROM SHEET ---
        try:
            row_indices = library_df[library_df["pdf_id"] == pdf_id].index.tolist()

            if not row_indices:
                logging.warning(f"No matching row index found for pdf_id {pdf_id}. Skipping row deletion.")
                continue
           
            remove_rows(sheets_client, config["LIBRARY_UNIFIED"], row_indices=row_indices)
            log_event(sheets_client, "deleted", pdf_id, filename, extra_columns=[original_status, folder_name])
        except Exception as e:
            logging.error("Failed to remove row for %s: %s", pdf_id, e)

        deleted_rows.append(row)

    return rows_to_delete
