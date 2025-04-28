import logging
from dotenv import load_dotenv
import os  # needed for local testing
from qdrant_client import QdrantClient
import pandas as pd
from datetime import datetime

# utilities
from admin_config import *
import google_utils as goo_utils
import library_utils as lib_utils

load_dotenv(ENV_PATH)  # needed for local testing


# --- SETUP LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    logging.info("Starting controlled document deletion.")

    # Connect to APIs
    sheets_client, drive_client = goo_utils.get_gcp_clients()
    qdrant_client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)

    qdrant_type = lib_utils.which_qdrant(qdrant_client)
    collections = lib_utils.list_collections(qdrant_client)

    # Fetch catalog
    catalog_df = goo_utils.fetch_sheet_as_dataframe(sheets_client, LIBRARY_CATALOG_ID)
    if catalog_df.empty:
        logging.warning("Catalog sheet is empty.")
        return

    # Detect rows marked for deletion
    rows_for_deletion = catalog_df[catalog_df.get("delete", False) == True]  # Assuming you mark "delete=True" manually

    if rows_for_deletion.empty:
        logging.info("No rows marked for deletion.")
        return

    for idx, row in rows_for_deletion.iterrows():
        pdf_id = row["pdf_id"]
        filename = row["pdf_file_name"]

        # Delete from Qdrant
        lib_utils.delete_qdrant_by_pdf_id(qdrant_client, CONFIG["qdrant_collection_name"], pdf_id)
        logging.info(f"Deleted pdf_id {pdf_id} from Qdrant.")

        # Move PDF from live to deleted
        goo_utils.move_file_between_folders(drive_client, pdf_id, PDF_DELETED_FOLDER_ID)
        logging.info(f"Moved {filename} from live to deleted folder.")

        # Archive row
        lib_utils.archive_row_to_tab(sheets_client, LIBRARY_CATALOG_ID, row, archived_date=datetime.utcnow())
        lib_utils.remove_row_from_active_tab(sheets_client, LIBRARY_CATALOG_ID, idx)

        logging.info(f"Archived catalog entry for {filename}.")

    logging.info(f"Completed deletion of {len(rows_for_deletion)} documents.")

if __name__ == "__main__":
    main()