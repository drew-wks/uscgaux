import logging
import os  # needed for local testing
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from datetime import datetime

# utilities
from env_config import *
import gcp_utils as goo_utils
import library_utils as lib_utils

load_dotenv(ENV_PATH)  # needed for local testing


# --- SETUP LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    
    success_count = 0
    fail_count = 0
    skip_count = 0
    
    logging.info("Starting controlled document deletion.")

    # Connect to APIs
    drive_client, sheets_client = goo_utils.get_gcp_clients()
    
    qdrant_client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
    lib_utils.which_qdrant(qdrant_client)
    lib_utils.list_collections(qdrant_client)

    # Fetch catalog
    catalog_df = goo_utils.fetch_sheet_as_dataframe(sheets_client, LIBRARY_CATALOG_ID)
    if catalog_df.empty:
        logging.warning("Catalog sheet is empty.")
        return

    # Find rows marked for deletion
    rows_for_deletion = lib_utils.fetch_rows_marked_for_deletion(catalog_df)
    if rows_for_deletion.empty:
        logging.info("No rows marked for deletion.")
        return


    # --- Process each deletion
    for idx, row in rows_for_deletion.iterrows():
        pdf_id = str(row.get("pdf_id", "")).strip()
        file_id = str(row.get("gdrive_file_id", "")).strip()
        filename = str(row.get("pdf_file_name", "")).strip()

        if not pdf_id or not file_id:
            logging.warning(f"Missing pdf_id or gdrive_file_id for row index {idx}. Skipping.")
            skip_count += 1
            continue

        try:
            # Delete from Qdrant
            lib_utils.delete_qdr_by_pdf_id(qdrant_client, CONFIG["qdrant_collection_name"], pdf_id)

            # Move file from Live ➔ Deleted
            moved = goo_utils.move_file_between_folders(drive_client, file_id, PDF_ARCHIVE_FOLDER_ID)
            if not moved:
                logging.warning(f"Failed to move file {filename}.")
                fail_count += 1
                continue

            # Archive row
            lib_utils.archive_row_to_tab(sheets_client, LIBRARY_CATALOG_ID, row, archived_date=datetime.utcnow())
            lib_utils.remove_row_from_active_tab(sheets_client, LIBRARY_CATALOG_ID, idx)

            logging.info(f"Successfully deleted and archived {filename}.")
            success_count += 1

        except Exception as e:
            logging.error(f"Error processing deletion for {filename}: {e}")
            fail_count += 1

    # --- Final log summary ---
    logging.info(f"--- Deletion Summary ---")
    logging.info(f"Total marked for deletion: {len(rows_for_deletion)}")
    logging.info(f"Successfully deleted and archived: {success_count}")
    logging.info(f"Failed deletions: {fail_count}")
    logging.info(f"Skipped (missing IDs): {skip_count}")

if __name__ == "__main__":
    main()