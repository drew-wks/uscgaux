import os
from datetime import datetime, timezone
import logging
from gspread.client import Client as SheetsClient
from googleapiclient.discovery import Resource as DriveClient
from qdrant_client import QdrantClient
from env_config import rag_config
from gcp_utils import move_pdf, fetch_sheet_as_df, fetch_sheet
from library_utils import validate_core_metadata_format, find_duplicates_against_reference, validate_all_rows_format
from qdrant_utils import in_qdrant
from langchain_utils import pdf_to_Docs_via_Drive
from log_writer import log_event

from env_config import env_config

config = env_config()

def promote_files(drive_client: DriveClient, sheets_client: SheetsClient, qdrant_client: QdrantClient):
    
    # VALIDATE all rows
    library_df = fetch_sheet_as_df(sheets_client, config["LIBRARY_UNIFIED"])
    valid_df, invalid_df, log_df = validate_all_rows_format(library_df)

    if not invalid_df.empty:
        logging.error("LIBRARY_UNIFIED validation failed: %s invalid row(s) found. Promotion halted.", len(invalid_df))
        return
    

    TARGET_STATUSES = ["new_validated", "clonedlive_validated"]
    
    # Ensure only one row in TARGET_STATUS exists for this pdf_id
    to_promote_df = valid_df[valid_df["status"].isin(TARGET_STATUSES)]
    duplicate_rows = find_duplicates_against_reference(
    df_to_check=to_promote_df,
    fields_to_check=[{"pdf_id": pdf_id} for pdf_id in to_promote_df["pdf_id"].dropna().unique()]
)
    if not duplicate_rows.empty:
        logging.error("%s duplicate validated pdf_id(s) found. Promotion halted.", len(duplicate_rows))
        return

    to_promote_df = to_promote_df.reset_index(drop=True)
    
    # Push to LIVE
    for idx, row in to_promote_df.iterrows():
        if row.get("status") not in TARGET_STATUSES:
            continue

        pdf_id = row.get("pdf_id")
        filename = row.get("pdf_file_name")
        file_id = row.get("google_id")

        # Confirm pdf_id is not already in Qdrant
        if in_qdrant(qdrant_client, rag_config("qdrant_collection_name"), pdf_id):
            logging.warning("%s already exists in Qdrant. Skipping promotion.", pdf_id)
            continue

        # Fetch PDF, extract Docs, inject metadata, chunk, and send to Qdrant
        # TODO THIS IS A A PLACEHOLDER
        docs = pdf_to_Docs_via_Drive(drive_client, pdf_id, planned_validated_metadata)
        if not docs:
            logging.warning("Failed to extract docs for %s: %s. Skipping.", filename, pdf_id)
            continue

        qdrant_client.upload_collection_batch(collection_name=rag_config("qdrant_collection_name"), documents=docs)

        # Move PDF to PDF_LIVE folder
        move_pdf(drive_client, file_id, config["PDF_LIVE"])

        # Change status in LIBRARY_UNIFIED → live
        row["status"] = "live"
        row["status_timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        sheet = fetch_sheet(sheets_client, config["LIBRARY_UNIFIED"])
        sheet.update(f"A{idx+2}", [row.tolist()])

        log_event(sheets_client, "promoted_to_live", pdf_id, filename)

