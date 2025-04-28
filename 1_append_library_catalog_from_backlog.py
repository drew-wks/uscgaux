import logging
import os  # needed for local testing
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime


# Utilities
import google_utils as goo_utils
import library_utils as lib_utils
from admin_config import *


load_dotenv(ENV_PATH) # needed for local testing

# --- SETUP LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    logging.info("Starting catalog append from PDF backlog.")

    # Connect to Google APIs
    sheets_client, drive_client = goo_utils.get_gcp_clients()

    # Fetch PDFs in backlog
    backlog_df = goo_utils.fetch_pdfs(drive_client, PDF_BACKLOG_FOLDER_ID)
    if backlog_df.empty:
        logging.warning("No PDFs found in backlog folder.")
        return

    # Fetch current catalog
    catalog_df = goo_utils.fetch_sheet_as_dataframe(sheets_client, LIBRARY_CATALOG_ID)
    if catalog_df.empty:
        logging.warning("Catalog sheet is empty.")
        return

    # Determine missing files
    existing_filenames = set(catalog_df["pdf_file_name"].dropna().str.strip().str.lower())
    new_entries = []

    for idx, row in backlog_df.iterrows():
        filename = row["Name"].strip()
        if filename.lower() not in existing_filenames:
            logging.info(f"New PDF detected: {filename}")

            # Download PDF temporarily to memory
            pdf_bytes = drive_client.files().get_media(fileId=row["ID"]).execute()
            pdf_id = lib_utils.compute_pdf_id(pdf_bytes)

            new_entry = {
                "pdf_id": pdf_id,
                "pdf_file_name": filename,
                # Leave other fields blank for manual fill
            }
            new_entries.append(new_entry)

    if new_entries:
        # Append new rows to the sheet
        new_df = pd.DataFrame(new_entries)
        lib_utils.append_rows_to_sheet(sheets_client, LIBRARY_CATALOG_ID, new_df)
        logging.info(f"Appended {len(new_entries)} new entries to library catalog.")
    else:
        logging.info("No new PDFs to append.")

if __name__ == "__main__":
    main()