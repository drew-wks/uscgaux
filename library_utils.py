import os
import logging
import uuid
from datetime import datetime, timezone
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from qdrant_client import QdrantClient
from qdrant_client.http import models, exceptions as qdrant_exceptions
from google_utils import fetch_sheet
from log_writer import log_admin_event
import pandas as pd
from pypdf import PdfReader



def compute_pdf_id(pdf_bytes_io):
    """
    Compute a unique identifier for a PDF based on its full text content.

    Args:
        pdf_bytes_io (BytesIO): In-memory bytes buffer containing the PDF data.

    Returns:
        str: A UUID string representing the PDF content.
    """
    try:
        logging.getLogger("pypdf").setLevel(logging.ERROR)
        pdf_bytes_io.seek(0)
        reader = PdfReader(pdf_bytes_io)
        full_text = ""
        for page in reader.pages:
            full_text += page.extract_text() or ""
        pdf_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, full_text))
        return pdf_uuid
    except Exception as e:
        logging.warning(f"Error computing PDF ID: {e}")
        return None


def pdf_to_Docs_via_pypdf(pdf_bytes_io, pdf_id=None):
    """
    Extract text documents from an in-memory PDF using pypdf.

    Args:
        pdf_bytes_io (BytesIO): In-memory bytes buffer containing the PDF data.
        pdf_id (str, optional): An identifier for the PDF.

    Returns:
        list of dict: List of documents extracted from the PDF, each with text and metadata.
    """
    try:
        logging.getLogger("pypdf").setLevel(logging.ERROR)
        pdf_bytes_io.seek(0)
        reader = PdfReader(pdf_bytes_io)
        docs = []
        for i, page in enumerate(reader.pages):
            text = page.extract_text() or ""
            doc = {
                "page_number": i + 1,
                "text": text,
                "pdf_id": pdf_id
            }
            docs.append(doc)
        return docs
    except Exception as e:
        logging.warning(f"Error extracting documents from PDF: {e}")
        return []


def init_qdrant(mode: str = "cloud") -> QdrantClient:
    mode = mode.lower()
    print(mode)
    if mode == "cloud":
        return QdrantClient(
            url=os.environ["QDRANT_URL"],
            api_key=os.environ["QDRANT_API_KEY"],
            prefer_grpc=True
        )
    elif mode == "local":
        return QdrantClient(
            path=os.environ["QDRANT_PATH"]
        )
    else:
        raise ValueError(f"Invalid mode '{mode}'. Must be 'cloud' or 'local'.")


def which_qdrant(client):
    """
    Detect whether the Qdrant client is connected to a local or cloud instance.
    Inspects the internal _client type string for clues.

    Args:
        client: Qdrant client instance.

    Returns:
        str: 'local', 'cloud', or 'unknown'.
    """
    try:
        client_type = str(type(client._client)).lower()

        if "qdrant_local" in client_type:
            qdrant_location = "local"
        elif "qdrant_remote" in client_type:
            qdrant_location = "cloud"
        else:
            qdrant_location = "unknown"

        logging.info(f"Qdrant location detected: {qdrant_location}")

    except qdrant_exceptions.UnexpectedResponse as e:
        if "404" in str(e):
            logging.warning("The server returned a 404 Not Found error — server active but wrong URL or endpoint.")
        else:
            raise
        qdrant_location = "unknown"
    except Exception as e:
        logging.warning(f"An unexpected error occurred while detecting Qdrant location: {e}")
        qdrant_location = "unknown"

    return qdrant_location


def list_collections(client):
    """
    List all collections in the Qdrant instance.

    Args:
        client: Qdrant client instance.

    Returns:
        list: List of collection names.
    """
    try:
        collections = client.get_collections()
        collection_names = [col.name for col in collections.collections]
        logging.info(f"Collections found: {collection_names}")
        return collection_names
    except Exception as e:
        logging.warning(f"Error listing collections: {e}")
        return []


def is_pdf_id_in_qdrant(client, RAG_CONFIG, pdf_id):
    """
    Check if a document with a specific pdf_id exists in the Qdrant collection.

    Args:
        client: Qdrant client instance.
        CONFIG (dict): Configuration dictionary containing collection name.
        pdf_id (str): The PDF ID to check.

    Returns:
        bool: True if the document exists, False otherwise.
    """
    collection_name = RAG_CONFIG.get('qdrant_collection_name')
    if not collection_name:
        logging.warning("Collection name not specified in CONFIG.")
        return False
    try:
        search_result = client.search(
            collection_name=collection_name,
            query_vector=[0]*1536,  # Dummy vector for filtering only
            filter={
                "must": [
                    {
                        "key": "pdf_id",
                        "match": {"value": pdf_id}
                    }
                ]
            },
            limit=1
        )
        exists = len(search_result) > 0
        logging.info(f"PDF ID '{pdf_id}' existence in collection '{collection_name}': {exists}")
        return exists
    except Exception as e:
        logging.warning(f"Error checking PDF ID in Qdrant: {e}")
        return False


def check_qdrant_record_exists(record_id, qdrant, collection_name):
    """
    Check if a record with the given ID exists in the specified Qdrant collection.

    Args:
        record_id (str or int): The ID of the record to check.
        qdrant: Qdrant client instance.
        collection_name (str): The name of the collection.

    Returns:
        bool: True if the record exists, False otherwise.
    """
    try:
        point = qdrant.get_point(collection_name=collection_name, point_id=record_id)
        exists = point is not None
        logging.info(f"Record ID '{record_id}' existence in collection '{collection_name}': {exists}")
        return exists
    except Exception as e:
        logging.warning(f"Error checking record existence in Qdrant: {e}")
        return False


def get_planned_metadata_for_single_record(metadata_df, pdf_id):
    """
    Given a DataFrame of metadata and a pdf_id,
    returns a dictionary of strings representing the document's metadata.
    """
    try:
        # Find the metadata row that corresponds to this pdf_id
        pdf_metadata = metadata_df[metadata_df['pdf_id'].str.strip().astype(str).str.lower() == pdf_id.lower()]

        if pdf_metadata.empty:
            raise ValueError(f"No metadata found for pdf_id: {pdf_id}")

        # Confirm no duplicate pdf_ids
        if len(pdf_metadata) > 1:
            raise ValueError(f"Found duplicates for pdf_id '{pdf_id}' in metadata sheet.")

        pdf_metadata = pdf_metadata.iloc[0].copy()
        logging.info(f"Successfully accessed metadata for pdf_id: {pdf_id}")

        # Confirm no duplicate publication_numbers
        publication_number = pdf_metadata.get('publication_number', '')
        if publication_number and metadata_df[metadata_df['publication_number'] == publication_number].shape[0] > 1:
            raise ValueError(f"Found duplicates for publication_number: '{publication_number}'")

        # Confirm no existing upsert date
        if pd.notna(pdf_metadata.get('upsert_date')):
            raise ValueError(f"Existing upsert_date found for pdf_id: {pdf_id}")

        # Set the upsert date
        pdf_metadata['upsert_date'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        logging.info(f"Set upsert_date to {pdf_metadata['upsert_date']} for pdf_id: {pdf_id}")

        # Format datetime fields correctly
        for col in ['upsert_date', 'effective_date', 'expiration_date']:
            if col in pdf_metadata.index:
                try:
                    parsed = pd.to_datetime(pdf_metadata[col], errors='raise', utc=True)
                    pdf_metadata[col] = parsed.strftime('%Y-%m-%dT%H:%M:%SZ')
                except Exception as e:
                    raise ValueError(f"Invalid date format in '{col}' for pdf_id '{pdf_id}': {e}")

        # Replace NaN with empty strings
        pdf_metadata = pdf_metadata.fillna('')

        # Cast everything to string (except booleans)
        document_metadata = {
            key: value if isinstance(value, bool) else str(value) if value is not None else ''
            for key, value in pdf_metadata.to_dict().items()
        }

        # Confirm required fields
        required_fields = ['title', 'scope', 'aux_specific', 'public_release', 'pdf_file_name', 'embedding']
        for field in required_fields:
            if field not in pdf_metadata or pd.isna(pdf_metadata[field]) or pdf_metadata[field] == '':
                raise ValueError(f"Required field '{field}' is empty or missing for pdf_id '{pdf_id}'")

        return document_metadata

    except Exception as e:
        logging.error(f"Error retrieving metadata for pdf_id {pdf_id}: {e}")
        return None
    

def safe_append_rows_to_sheet(sheet_client, spreadsheet_id, new_rows_df, sheet_name="Sheet1"):
    """
    Safely append new rows to a Google Sheets tab,
    skipping duplicates based on pdf_id, and return inserted entries.

    Args:
        sheet_client: Authenticated Google Sheets API client.
        spreadsheet_id: ID of the target spreadsheet.
        new_rows_df: Pandas DataFrame containing new rows to append.
        sheet_name: Name of the tab (default = "Sheet1").

    Returns:
        list of dicts: Each dict has 'pdf_id' and 'pdf_file_name' of appended rows.
    """
    try:
        if new_rows_df.empty:
            logging.warning("No rows to append — DataFrame is empty.")
            return []
        
        worksheet = sheet_client.open_by_key(spreadsheet_id).worksheet(sheet_name)

        # Fetch existing rows
        existing_rows = worksheet.get_all_values()
        headers = existing_rows[0]
        existing_pdf_ids = {
            row[headers.index("pdf_id")].strip().lower()
            for row in existing_rows[1:]
            if len(row) > headers.index("pdf_id")
        }

        # Step 2: Filter new rows to exclude duplicates
        safe_rows = []
        safe_entries = []
        for _, row in new_rows_df.iterrows():
            pdf_id = str(row.get("pdf_id", "")).strip().lower()
            pdf_file_name = row.get("pdf_file_name", "")
            if pdf_id not in existing_pdf_ids:
                row_ordered = {col: row.get(col, "") for col in headers}
                safe_rows.append([row_ordered[col] for col in headers])
                safe_entries.append(row_ordered)
            else:
                logging.warning(f"Duplicate detected, skipping pdf_id: {pdf_id}")

        if safe_rows:
            worksheet.append_rows(safe_rows, value_input_option="USER_ENTERED")
            logging.info(f"Appended {len(safe_rows)} new unique rows to {sheet_name}")
        else:
            logging.info("No unique rows to append after duplicate check.")

        return safe_entries

    except Exception as e:
        logging.error(f"Error appending rows to Google Sheet: {e}")
        return []



def validate_core_metadata(df):
    """
    Validates that the catalog DataFrame contains required columns.

    Args:
        df (pd.DataFrame): The catalog DataFrame to validate.

    Raises:
        ValueError: If any required columns are missing.
    """
    required_columns = {"pdf_id", "pdf_file_name", "google_id", "link"}

    existing_columns = set(df.columns.str.strip().str.lower())

    missing_columns = required_columns - existing_columns

    if missing_columns:
        raise ValueError(
            f"The catalog is missing required columns: {missing_columns}. Please fix the sheet structure before continuing."
        )


    logging.info("Catalog structure validated successfully.")



def validaterows(sheets_client, spreadsheet_id):
    """
    Validates rows with status = 'new_for_validation' or 'clonedlive_for_validation'
    and updates their status to 'new_validated' or 'clonedlive_validated'.
    """
    VALIDATION_STATUSES = ["new_for_validation", "clonedlive_for_validation"]

    df = fetch_sheet(spreadsheet_id)  # ✅ Use your helper to read the data

    try:
        worksheet = sheets_client.open_by_key(spreadsheet_id).sheet1  # ✅ Only needed for updating
    except Exception as e:
        logging.error(f"Unable to open worksheet for updates: {e}")
        return

    headers = df.columns.tolist()

    for idx, row in df.iterrows():
        current_status = str(row.get("status", "")).strip().lower()
        if current_status not in VALIDATION_STATUSES:
            continue

        pdf_id = row.get("pdf_id")
        if not pdf_id:
            logging.warning(f"Row {idx + 2} missing pdf_id. Skipping validation.")
            continue

        try:
            if validate_core_metadata(row):
                new_status = (
                    "new_validated" if current_status == "new_for_validation"
                    else "clonedlive_validated"
                )
                row["status"] = new_status
                row["timestamp_validated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                updated_row = [row.get(col, "") for col in headers]

                # Update sheet row (Google Sheets is 1-indexed and row 1 is headers)
                worksheet.update(f"A{idx + 2}", [updated_row])

                log_admin_event("validated", pdf_id, row.get("filename"))

        except Exception as e:
            logging.warning(f"Validation failed for {pdf_id} at row {idx + 2}: {e}")



def fetch_rows_by_status(catalog_df, keywords):
    """
    Fetch rows from a catalog DataFrame where the 'status' column contains 
    any of the given keywords (case-insensitive partial match).

    Args:
        catalog_df (pd.DataFrame): The catalog DataFrame.
        keywords (str or list of str): Keyword(s) to match in the 'status' column.

    Returns:
        pd.DataFrame: Filtered rows matching any of the keywords.
    """
    if "status" not in catalog_df.columns:
        logging.warning("'status' column not found in catalog.")
        return pd.DataFrame()

    # Normalize to lowercase string
    status_series = catalog_df["status"].astype(str).str.lower()

    # Ensure keywords is a list
    if isinstance(keywords, str):
        keywords = [keywords]

    # Build combined match mask
    match_mask = status_series.apply(lambda x: any(kw.lower() in x for kw in keywords))
    matched_rows = catalog_df[match_mask]

    logging.info(f"Found {len(matched_rows)} rows matching keywords: {keywords}")
    return matched_rows





def delete_qdrant_by_pdf_id(qdrant_client, collection_name, pdf_id):
    """
    Delete all vectors in a Qdrant collection that match a given pdf_id.

    Args:
        qdrant_client: Qdrant client.
        collection_name: Name of the collection.
        pdf_id: The UUID of the PDF.
    """
    try:
        filter_condition = models.Filter(
            must=[
                models.FieldCondition(
                    key="metadata.pdf_id",
                    match=models.MatchText(text=pdf_id)
                )
            ]
        )
        result = qdrant_client.delete(collection_name=collection_name, points_selector=filter_condition)
        logging.info(f"Deleted points for pdf_id {pdf_id} from {collection_name}. Operation ID: {result.operation_id}")
    except Exception as e:
        logging.error(f"Error deleting points for pdf_id {pdf_id}: {e}")



def remove_rows_from_sheet(sheets_client, spreadsheet_id, row_indices, sheet_name="Sheet1"):
    """
    Delete multiple rows from a specific sheet tab.

    Args:
        sheets_client: gspread client.
        spreadsheet_id: ID of the source spreadsheet.
        row_indices: List of 0-based row indices (relative to a DataFrame).
        sheet_name: Name of the worksheet/tab (default = "Sheet1").
    """
    try:
        ws = sheets_client.open_by_key(spreadsheet_id).worksheet(sheet_name)

        # Sort in reverse to prevent index shifting
        for row_index in sorted(row_indices, reverse=True):
            gsheet_row = row_index + 2  # account for 1-indexing + header row
            ws.delete_rows(gsheet_row)
            logging.info(f"Deleted row {row_index} (sheet row {gsheet_row}) from {sheet_name}")

    except Exception as e:
        logging.error(f"Failed to delete rows from {sheet_name}: {e}")

