import io
import logging
from datetime import datetime, timezone
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
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
    Detect whether the Qdrant client is connected to a cloud or local instance.

    Args:
        client: Qdrant client instance.

    Returns:
        str: 'cloud' if connected to Qdrant Cloud, 'local' otherwise.
    """
    try:
        info = client.get_status()
        if hasattr(client, 'api_key') and client.api_key:
            logging.info("Detected Qdrant Cloud client.")
            return 'cloud'
        else:
            logging.info("Detected local Qdrant client.")
            return 'local'
    except Exception as e:
        logging.warning(f"Error detecting Qdrant client type: {e}")
        return 'unknown'


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