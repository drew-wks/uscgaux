
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
from env_config import RAG_CONFIG
from gcp_utils import fetch_pdf, move_pdf, fetch_sheet_as_df
from library_utils import validate_core_metadata, validate_rows, pdf_to_Docs_via_pypdf
from qdrant_utils import get_qdrant_client, which_qdrant, in_qdrant
from log_writer import log_event


TARGET_STATUSES = ["new_validated", "clonedlive_validated"]

def promote_files(drive_client, sheets_client):
    library_unified_df = fetch_sheet_as_df(sheets_client, os.environ["LIBRARY_UNIFIED"])
    
    validaterows(sheets_client, library_unified_df)

    for _, row in library_unified_df.iterrows():
        if row.get("status") not in TARGET_STATUSES:
            continue

        pdf_id = row.get("pdf_id")
        file_id = row.get("google_id")
        filename = row.get("pdf_file_name")

        # Ensure only one validated row exists for this pdf_id
        validated_count = library_unified_df[(library_unified_df["pdf_id"] == pdf_id) & (library_unified_df["status"].isin(TARGET_STATUSES))].shape[0]
        if validated_count > 1:
            logging.warning(f"Multiple validated rows found for {pdf_id}. Skipping promotion.")
            continue

        # Validate row structure
        if not validate_core_metadata(row):
            logging.warning(f"Incomplete metadata for {pdf_id}. Skipping promotion.")
            continue
        
        qdrant_client = get_qdrant_client(RAG_CONFIG["qdrant_location"])

        # Confirm pdf_id is not already in Qdrant
        if in_qdrant(qdrant_client, RAG_CONFIG, pdf_id):
            logging.warning(f"{pdf_id} already exists in Qdrant. Skipping promotion.")
            continue

        # Fetch PDF, extract docs, and send to Qdrant
        pdf_io = fetch_pdf(drive_client, file_id)
        docs = pdf_to_Docs_via_pypdf(pdf_io, pdf_id)
        if not docs:
            logging.warning(f"Failed to extract docs for {pdf_id}. Skipping.")
            continue

        # TODO Turn this into a real function. Chat GPT made this up.
        qdrant_client.upload_collection_batch(collection_name=RAG_CONFIG["qdrant_collection_name"], documents=docs)

        # Move file to LIVE
        move_pdf(drive_client, file_id, os.environ["PDF_LIVE"])

        # Update spreadsheet: status → live
        row["status"] = "live"
        row["timestamp_promoted"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        sheet.values_update(
            "Sheet1" + f"!A{_+2}",
            params={"valueInputOption": "RAW"},
            body={"values": [list(row.values())]},
        )

        log_event("promoted_to_live", pdf_id, filename)

    
