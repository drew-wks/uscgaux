import io
import logging
from datetime import datetime, timezone
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from qdrant_client.http import models, exceptions as qdrant_exceptions
import pandas as pd
import uuid
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


def download_pdf_from_drive(drive_client, file_id):
    """
    Downloads a PDF file from Google Drive into memory.

    Args:
        drive_client: Authenticated Google Drive API client.
        file_id (str): The ID of the file to download.

    Returns:
        BytesIO: In-memory bytes buffer containing the PDF data, or None if download fails.
    """
    try:
        request = drive_client.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        return fh
    except HttpError as error:
        if error.resp.status == 404:
            logging.warning(f"File with ID {file_id} not found.")
            return None
        else:
            raise


def fetch_sheet_as_dataframe(sheets_client, spreadsheet_id):
    """
    Fetches the first worksheet of a Google Sheets spreadsheet as a Pandas DataFrame.

    Args:
        sheets_client: Authenticated Google Sheets API client.
        spreadsheet_id (str): The ID of the spreadsheet.

    Returns:
        pd.DataFrame: DataFrame of the first worksheet.
    """
    try:
        spreadsheet = sheets_client.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_id = spreadsheet['sheets'][0]['properties']['title']
        result = sheets_client.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=sheet_id).execute()
        values = result.get('values', [])
        if not values:
            return pd.DataFrame()
        return pd.DataFrame(values[1:], columns=values[0])
    except Exception as e:
        logging.warning(f"Error fetching sheet data: {e}")
        return pd.DataFrame()


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


def is_pdf_id_in_qdrant(client, CONFIG, pdf_id):
    """
    Check if a document with a specific pdf_id exists in the Qdrant collection.

    Args:
        client: Qdrant client instance.
        CONFIG (dict): Configuration dictionary containing collection name.
        pdf_id (str): The PDF ID to check.

    Returns:
        bool: True if the document exists, False otherwise.
    """
    collection_name = CONFIG.get('qdrant_collection_name')
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
        pdf_metadata['upsert_date'] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
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
    

def append_rows_to_sheet(sheet_client, spreadsheet_id, new_rows_df, sheet_name="Sheet1"):
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

        # Step 1: Fetch existing catalog
        existing_sheet = sheet_client.values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}"
        ).execute()

        existing_rows = existing_sheet.get('values', [])
        existing_pdf_ids = set()

        if existing_rows:
            headers = existing_rows[0]  # first row = column headers
            if "pdf_id" in headers:
                pdf_id_index = headers.index("pdf_id")
                for row in existing_rows[1:]:  # Skip header row
                    if len(row) > pdf_id_index:
                        existing_pdf_ids.add(row[pdf_id_index].strip().lower())

        # Step 2: Filter new rows to exclude duplicates
        safe_rows = []
        safe_entries = []
        for _, row in new_rows_df.iterrows():
            pdf_id = str(row.get("pdf_id", "")).strip().lower()
            pdf_file_name = row.get("pdf_file_name", "")
            if pdf_id and pdf_id not in existing_pdf_ids:
                safe_rows.append(row.values.tolist())
                safe_entries.append({"pdf_id": pdf_id, "pdf_file_name": pdf_file_name})
            else:
                logging.warning(f"Duplicate detected, skipping pdf_id: {pdf_id}")

        if not safe_rows:
            logging.info("No unique rows to append after duplicate check.")
            return []

        # Step 3: Append safe new rows
        sheet_client.values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": safe_rows}
        ).execute()

        logging.info(f"Appended {len(safe_rows)} new unique rows to {sheet_name} in spreadsheet {spreadsheet_id}.")
        return safe_entries

    except Exception as e:
        logging.error(f"Error appending rows to Google Sheet: {e}")
        return []
    
    
# TODO  FIGURE OUT WHERE VALIDATION GOES
def validate_catalog_structure(df):
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


def fetch_rows_marked_for_deletion(catalog_df):
    """
    Fetch rows marked for deletion in the library catalog.

    Args:
        catalog_df: Pandas DataFrame of the catalog.

    Returns:
        DataFrame: Rows marked for deletion.
    """
    if "delete" not in catalog_df.columns:
        logging.warning("'delete' column not found in catalog.")
        return pd.DataFrame()

    deletion_rows = catalog_df[catalog_df["delete"] == True]
    logging.info(f"Found {len(deletion_rows)} rows marked for deletion.")
    return deletion_rows


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


def archive_row_to_tab(sheet_client, spreadsheet_id, row_data, archived_date):
    """
    Archive a row to the second tab of the sheet with an archived date.

    Args:
        sheet_client: Google Sheets client.
        spreadsheet_id: ID of the spreadsheet.
        row_data: Series or dict of the row to move.
        archived_date: UTC datetime to set as 'archived_date'.
    """
    try:
        # Assume Archive sheet is the second tab
        archive_sheet_name = "Archived"
        
        # Convert row_data to a list, appending archived_date
        archive_row = list(row_data.values)
        archive_row.append(archived_date.strftime("%Y-%m-%dT%H:%M:%SZ"))

        # Append to archive sheet
        sheet_client.values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{archive_sheet_name}!A1",
            valueInputOption="USER_ENTERED",
            body={"values": [archive_row]}
        ).execute()

        logging.info(f"Archived row for pdf_id {row_data['pdf_id'] if 'pdf_id' in row_data else '[unknown]'}.")

    except Exception as e:
        logging.error(f"Failed to archive row: {e}")


def remove_row_from_active_tab(sheet_client, spreadsheet_id, row_index):
    """
    Remove a row from the active sheet (main catalog) after archiving.

    Args:
        sheet_client: Google Sheets client.
        spreadsheet_id: ID of the spreadsheet.
        row_index: Row number (0-indexed).
    """
    try:
        sheet_id = 0  # Assume the first sheet/tab
        body = {
            "requests": [
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": row_index,
                            "endIndex": row_index + 1
                        }
                    }
                }
            ]
        }
        sheet_client.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        logging.info(f"Removed row at index {row_index} from active catalog.")
    except Exception as e:
        logging.error(f"Failed to remove row {row_index}: {e}")