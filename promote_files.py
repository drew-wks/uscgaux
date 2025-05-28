
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
from datetime import datetime, timezone
import logging
from admin_config import set_env_vars, RAG_CONFIG
from google_utils import get_gcp_clients, fetch_pdf_from_drive, move_file_between_folders, fetch_sheet
from library_utils import validate_core_metadata, validaterows, init_qdrant, is_pdf_id_in_qdrant, pdf_to_Docs_via_pypdf, which_qdrant
from log_writer import log_admin_event


set_env_vars()
drive_client, sheets_client = get_gcp_clients()

TARGET_STATUSES = ["new_validated", "clonedlive_validated"]

def promote_files():
    library_unified_df = fetch_sheet(os.environ["LIBRARY_UNIFIED"])
    
    validaterows(sheets_client, library_unified_df)

    for _, row in library_unified_df.iterrows():
        if row.get("status") not in TARGET_STATUSES:
            continue

        pdf_id = row.get("pdf_id")
        file_id = row.get("google_id")
        filename = row.get("filename")

        # Ensure only one validated row exists for this pdf_id
        validated_count = library_unified_df[(library_unified_df["pdf_id"] == pdf_id) & (library_unified_df["status"].isin(TARGET_STATUSES))].shape[0]
        if validated_count > 1:
            logging.warning(f"Multiple validated rows found for {pdf_id}. Skipping promotion.")
            continue

        # Validate row structure
        if not validate_core_metadata(row):
            logging.warning(f"Incomplete metadata for {pdf_id}. Skipping promotion.")
            continue
        
        qdrant_client = init_qdrant(RAG_CONFIG["qdrant_location"])

        # Confirm pdf_id is not already in Qdrant
        if is_pdf_id_in_qdrant(qdrant_client, RAG_CONFIG, pdf_id):
            logging.warning(f"{pdf_id} already exists in Qdrant. Skipping promotion.")
            continue

        # Fetch PDF, extract docs, and send to Qdrant
        pdf_io = fetch_pdf_from_drive(drive_client, file_id)
        docs = pdf_to_Docs_via_pypdf(pdf_io, pdf_id)
        if not docs:
            logging.warning(f"Failed to extract docs for {pdf_id}. Skipping.")
            continue

        # TODO Turn this into a real function. Chat GPT made this up.
        qdrant_client.upload_collection_batch(collection_name=RAG_CONFIG["qdrant_collection_name"], documents=docs)

        # Move file to LIVE
        move_file_between_folders(drive_client, file_id, os.environ["PDF_LIVE"])

        # Update spreadsheet: status → live
        row["status"] = "live"
        row["timestamp_promoted"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        sheet.values_update(
            "Sheet1" + f"!A{_+2}",
            params={"valueInputOption": "RAW"},
            body={"values": [list(row.values())]},
        )

        log_admin_event("promoted_to_live", pdf_id, filename)


if __name__ == "__main__":
    promote_files()
    
