
"""
Agent: promote_files.py

Promotes rows in LIBRARY_UNIFIED with status in ['new_validated', 'clonedlive_validated']:
- Validates metadata completeness and uniqueness
- Pushes content to Qdrant
- Changes status to 'live'
- Moves PDF from PDF_TAGGING to PDF_LIVE
- Logs all actions
"""

import os
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import logging
from google_utils import get_gcp_clients, fetch_pdf_from_drive, move_file_between_folders
from library_utils import pdf_to_Docs_via_pypdf, validate_catalog_structure, is_pdf_id_in_qdrant
from admin_config import load_qdrant_secrets, CONFIG, ENV_PATH
from log_writer import log_admin_event
from qdrant_client import QdrantClient

load_dotenv(ENV_PATH)  # needed for local testing
load_qdrant_secrets()


TARGET_STATUSES = ["new_validated", "clonedlive_validated"]

def promote_files():
    drive_client, sheets_client = get_gcp_clients()
    sheet = sheets_client.open_by_key(os.environ["LIBRARY_UNIFIED"])
    rows = sheet.worksheet("Sheet1").get_all_records()
    df = pd.DataFrame(rows)

    for _, row in df.iterrows():
        if row.get("status") not in TARGET_STATUSES:
            continue

        pdf_id = row.get("pdf_id")
        file_id = row.get("google_id")
        filename = row.get("filename")

        # Ensure only one validated row exists for this pdf_id
        validated_count = df[(df["pdf_id"] == pdf_id) & (df["status"].isin(TARGET_STATUSES))].shape[0]
        if validated_count > 1:
            logging.warning(f"Multiple validated rows found for {pdf_id}. Skipping promotion.")
            continue

        # Validate row structure
        if not validate_catalog_structure(row):
            logging.warning(f"Incomplete metadata for {pdf_id}. Skipping promotion.")
            continue

        # Confirm pdf_id is not already in Qdrant
        if is_pdf_id_in_qdrant(pdf_id):
            logging.warning(f"{pdf_id} already exists in Qdrant. Skipping promotion.")
            continue

        # Fetch PDF, extract docs, and send to Qdrant
        pdf_io = fetch_pdf_from_drive(drive_client, file_id)
        docs = pdf_to_Docs_via_pypdf(pdf_io, pdf_id)
        if not docs:
            logging.warning(f"Failed to extract docs for {pdf_id}. Skipping.")
            continue

        client = QdrantClient(QDRANT_URL, QDRANT_API_KEY)
        # TODO Turn this into a real function. Chat GPT made this up.
        client.upload_collection_batch(collection_name=CONFIG["qdrant_collection_name"], documents=docs)

        # Move file to LIVE
        move_file_between_folders(drive_client, file_id, os.environ["PDF_LIVE"])

        # Update spreadsheet: status → live
        row["status"] = "live"
        row["timestamp_promoted"] = datetime.utcnow().isoformat()
        sheet.values_update(
            "Sheet1" + f"!A{_+2}",
            params={"valueInputOption": "RAW"},
            body={"values": [list(row.values())]},
        )

        log_admin_event("promoted_to_live", pdf_id, filename)


if __name__ == "__main__":
    promote_files()
