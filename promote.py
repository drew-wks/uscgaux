from datetime import datetime, timezone
import logging
from gspread.client import Client as SheetsClient
from googleapiclient.discovery import Resource as DriveClient
from qdrant_client import QdrantClient
from env_config import env_config, rag_config, RAG_CONFIG
from utils.gcp_utils import file_exists, move_file, fetch_sheet_as_df, fetch_sheet
from utils.library_utils import (
    find_duplicates_against_reference,
    validate_all_rows_format,
    remove_rows,
)
from utils.qdrant_utils import in_qdrant
from utils.langchain_utils import init_vectorstore, pdf_to_Docs_via_Drive, chunk_Docs
from utils.log_writer import log_event


config = env_config()

def upsert_single_file(drive_client: DriveClient, sheets_client: SheetsClient, qdrant_client: QdrantClient, row, idx):
    """
    Process and upsert a single document row into the vector database and update the associated metadata.

    This function performs the following steps:
    - Checks if the given `pdf_id` already exists in Qdrant and skips it if so.
    - Downloads the corresponding PDF from Google Drive.
    - Extracts and chunks the document content for embedding.
    - Uploads the document chunks to Qdrant.
    - Moves the PDF file to the LIVE folder in Drive.
    - Updates the row's status in the spreadsheet and logs the event.

    Args:
        drive_client (DriveClient): Authenticated Google Drive client.
        sheets_client (SheetsClient): Authenticated Google Sheets client.
        qdrant_client (QdrantClient): Qdrant client for vector operations.
        row (pd.Series): A row from the LIBRARY_UNIFIED dataframe representing the document metadata.
        idx (int): Index of the row in the spreadsheet (used for updating the correct cell range).

    Returns:
        Tuple[str, str]: A tuple containing:
            - status string: "uploaded", "rejected", or "failed"
            - pdf_id (str): Identifier of the processed document
    """
    pdf_id = str(row.get("pdf_id", ""))
    filename = str(row.get("pdf_file_name", ""))
    file_id = str(row.get("gcp_file_id", ""))

    # Confirm gcp_file_id exists and file is present in Drive
    if not file_id:
        logging.warning("Missing gcp_file_id for %s. Skipping promotion.", pdf_id)
        return "failed", pdf_id
    if not file_exists(drive_client, file_id):
        logging.warning("File ID %s for %s not found in Drive. Skipping.", file_id, pdf_id)
        return "failed", pdf_id
    
    # Confirm pdf_id is not already in Qdrant
    if in_qdrant(qdrant_client, rag_config("qdrant_collection_name"), pdf_id):
        logging.warning("%s already exists in Qdrant. Skipping promotion.", pdf_id)
        return "rejected", pdf_id

    # Fetch PDF, extract Docs, inject metadata, chunk, and send to Qdrant
    docs = pdf_to_Docs_via_Drive(drive_client, file_id, row.to_frame().T)
    
    if not docs:
        logging.warning("Failed to extract docs for %s: %s. Skipping.", filename, pdf_id)
        return "failed", pdf_id

    docs_chunks = chunk_Docs(docs, RAG_CONFIG)
    qdrant = init_vectorstore(qdrant_client)
    qdrant.add_documents(docs_chunks)

    # Move PDF to PDF_LIVE folder
    move_file(drive_client, file_id, config["PDF_LIVE"])

    # Change status in LIBRARY_UNIFIED → live
    row["status"] = "live"
    row["status_timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    # Set the upsert date in LIBRARY_UNIFIED
    row['upsert_date'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    logging.info("Set status to live and status_timestamp, upsert_date to  for pdf_id: %s", pdf_id)
    sheet = fetch_sheet(sheets_client, config["LIBRARY_UNIFIED"])
    sheet.update(f"A{idx+2}", [row.tolist()])

    # Remove any other rows with the same pdf_id
    try:
        current_df = fetch_sheet_as_df(sheets_client, config["LIBRARY_UNIFIED"])
        all_indices = current_df.index[current_df["pdf_id"].astype(str) == pdf_id].tolist()
        if len(all_indices) > 1:
            keep_index = min(all_indices)
            rows_to_remove = [i for i in all_indices if i != keep_index]
            remove_rows(sheets_client, config["LIBRARY_UNIFIED"], rows_to_remove)
            logging.info(
                "Removed %s duplicate row(s) for pdf_id %s", len(rows_to_remove), pdf_id
            )
    except Exception as dup_err:
        logging.error("Failed to remove duplicate rows for %s: %s", pdf_id, dup_err)

    log_event(sheets_client, "promoted_to_live", str(pdf_id), str(filename))
    return "uploaded", pdf_id



def promote_files(drive_client: DriveClient, sheets_client: SheetsClient, qdrant_client: QdrantClient):
    """
    Validates and prepares rows from the LIBRARY_UNIFIED Google Sheet for promotion.

    This function retrieves all rows from the LIBRARY_UNIFIED sheet, validates their
    format, and halts execution if any invalid rows are found. It is intended to be 
    the first step in a larger workflow that promotes validated documents to Qdrant 
    and updates Google Drive metadata accordingly.

    Args:
        drive_client (DriveClient): An authenticated Google Drive client.
        sheets_client (SheetsClient): An authenticated Google Sheets client.
        qdrant_client (QdrantClient): An initialized Qdrant vector store client.

    Returns:
        None. The function exits early if validation fails.
    """
    # VALIDATE all rows
    logging.info("Promoting new files.")
    library_df = fetch_sheet_as_df(sheets_client, config["LIBRARY_UNIFIED"])
    logging.info("Validating row formatsin LIBRARY_UNIFIED.")
    valid_df, invalid_df, log_df = validate_all_rows_format(library_df)

    if not invalid_df.empty:
        logging.error("LIBRARY_UNIFIED validation failed: %s invalid row(s) found. Promotion halted.", len(invalid_df))
        return
    

    TARGET_STATUSES = ["new_tagged", "clonedlive_tagged"]
    
    # Ensure only one row in TARGET_STATUS exists for this pdf_id
    to_promote_df = valid_df[valid_df["status"].isin(TARGET_STATUSES)]
    logging.info("Checking for duplicates.")
    duplicate_rows = find_duplicates_against_reference(
    df_to_check=to_promote_df,
    fields_to_check=[{"pdf_id": pdf_id} for pdf_id in to_promote_df["pdf_id"].dropna().unique()]
)
    if not duplicate_rows.empty:
        logging.error("%s duplicate pdf_id(s) found in promoted rows. Promotion halted.", len(duplicate_rows))
        return
    
    logging.info("Uploading to Qdrant.")
    uploaded_files = []
    rejected_files = []
    failed_files = []
    
    for idx, row in to_promote_df.iterrows():
        if row.get("status") not in TARGET_STATUSES:
            continue

        result, pdf_id = upsert_single_file(drive_client, sheets_client, qdrant_client, row, row.name)
        if result == "uploaded":
            uploaded_files.append(pdf_id)
        elif result == "rejected":
            rejected_files.append(pdf_id)
        elif result == "failed":
            failed_files.append(pdf_id)

    logging.info("\n✅ Uploaded files:")
    for item in uploaded_files:
        logging.info(item)
    logging.info("\n😈 Failed files:")
    for item in failed_files:
        logging.info(item)
    logging.info("\n💥 Rejected files:")
    for item in rejected_files:
        logging.info(item)
    logging.info("\n\n✅ Number of files successfully uploaded: %d", len(uploaded_files))
    logging.info("😈 Number of files failed during processing: %d", len(failed_files))
    logging.info("💥 Number of files rejected as duplicate: %d", len(rejected_files))

    return uploaded_files, failed_files, rejected_files
