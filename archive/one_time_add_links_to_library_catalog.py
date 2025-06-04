# one_time_add_links_to_library_catalog.py

import logging
import os
from dotenv import load_dotenv

# --- Imports ---
from app_config import *
import google_utils as goo_utils
from library_utils import append_rows_to_sheet
import pandas as pd

goo_utils.init_auth()

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Load environment variables
load_dotenv(ENV_PATH)

# --- Constants
BASE_DRIVE_URL = "https://drive.google.com/file/d/"

def main():
    logging.info("Starting one-time population of google_id and link fields.")

    # Connect to Google APIs
    drive_client, sheets_client = goo_utils.get_gcp_clients()
    
    sheet = sheets_client.open_by_key(LIBRARY_CATALOG_ID).sheet1

    # Fetch existing catalog
    records = sheet.get_all_records()
    catalog_df = pd.DataFrame(records)

    # Fetch PDFs from both backlog and live folders
    backlog_pdfs = goo_utils.list_pdfs_in_drive_folder(drive_client, PDF_TAGGING_FOLDER)
    live_pdfs = goo_utils.list_pdfs_in_drive_folder(drive_client, PDF_LIVE_FOLDER_ID)

    all_pdfs = pd.concat([backlog_pdfs, live_pdfs], ignore_index=True)
    logging.info(f"Found {len(all_pdfs)} total PDFs across Backlog and Live folders.")

    # Create a lookup dictionary by filename (case-insensitive match)
    pdf_lookup = {row["Name"].strip().lower(): row["ID"] for idx, row in all_pdfs.iterrows()}

    # Track update results
    updated_rows = 0
    missing_files = 0

    # Update catalog DataFrame
    for idx, row in catalog_df.iterrows():
        filename = str(row.get("pdf_file_name", "")).strip().lower()

        if not filename:
            logging.warning(f"No filename for catalog row index {idx}. Skipping.")
            continue

        matched_id = pdf_lookup.get(filename)
        if matched_id:
            catalog_df.at[idx, "google_id"] = matched_id
            catalog_df.at[idx, "link"] = BASE_DRIVE_URL + matched_id
            updated_rows += 1
        else:
            logging.warning(f"No matching Google Drive file for catalog filename: {filename}")
            missing_files += 1

    logging.info(f"Updated {updated_rows} rows. {missing_files} files were missing matches.")

    # --- Write updated catalog back
    try:
        # WARNING: This will overwrite the sheet with the new data
        # Be careful. You might want to back up first!
        data = [catalog_df.columns.tolist()] + catalog_df.values.tolist()
        sheet.update('A1', data)

        logging.info("Successfully updated the Library Catalog.")
    except Exception as e:
        logging.error(f"Error writing updated catalog to Google Sheets: {e}")


if __name__ == "__main__":
    main()