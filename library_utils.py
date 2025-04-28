import os, warnings, uuid, logging
from pypdf import PdfReader, errors
import pandas as pd
from qdrant_client.http import exceptions as qdrant_exceptions



def check_directory_exists(directory_path, create_if_not_exists=False):
    """
    Check if a directory exists. Optionally, create the directory if it does not exist.

    :param directory_path: Path of the directory to check.
    :param create_if_not_exists: If True, creates the directory if it does not exist.
    :return: True if the directory exists or was created, False otherwise.
    """
    if not os.path.isdir(directory_path):
        if create_if_not_exists:
            try:
                os.write(1,f"Directory does not exist: {directory_path}. Creating it.".encode())
                os.makedirs(directory_path)
                return True
            except OSError as error:
                os.write(1,f"Error creating directory {directory_path}: {error}".encode())
                return False
        else:
            os.write(1,f"Directory does not exist: {directory_path}".encode())
            return False
    return True



def check_duplicates_in_xlsx(file_path, cols):
    """
    Function to check for duplicates in specified columns of an Excel file.
    
    Args:
        file_path (str): The path to the Excel file containing metadata.
        cols (list): List of columns to check for duplicates.
    
    Example usage:
        file_path = "./docs/metadata/metadata.xlsx"
        cols = ['title', 'publication_number', 'pdf_id', 'file_name']
        
        check_duplicates_in_xlsx(metadata_file_path, cols)
    
    Returns:
        None
    """
    try:
        # Read Excel into a DataFrame
        df = pd.read_excel(file_path)

        # Iterate over each column and check for duplicates
        for column in cols:
            if column in df.columns:
                # Drop rows with NaN values before checking for duplicates
                non_null_df = df.dropna(subset=[column])
                duplicates = non_null_df[non_null_df.duplicated(subset=column, keep=False)]

                if not duplicates.empty:
                    print(f"Duplicate found in '{column}':")
                    print(duplicates[[column]])
                else:
                    print(f"No duplicates in '{column}'.")
    except Exception as e:
        print(f"An error occurred: {e}")


def compute_pdf_id(pdf_path):
    '''
    Generates a unique ID from the content of the PDF file.

    The function extracts text from all pages of the PDF--ignoring metadata-- and 
    generates a unique ID that is deterministic using UUID v5, so the same content
    will always generate the same ID.
    example ID:  3b845a10-cb3a-5014-96d8-360c8f1bf63f 
    If the document is empty, then it sets the UUID to "EMPTY_DOCUMENT". 
    
    Args:
        pdf_path (str): Path to the PDF file.
    
    Returns:
        str: UUID for the PDF content or "EMPTY_DOCUMENT" if the PDF is empty.
    '''
    
    # Suppress warnings such as: wrong pointing object 12 0 (offset 0)
    logging.getLogger("pypdf").setLevel(logging.ERROR)

    reader = PdfReader(pdf_path)
    num_pages = len(reader.pages)

    # Extract text from all pages and concatenate
    full_text = ""
    for page_num in range(num_pages):
        try:
            page_text = reader.pages[page_num].extract_text()
            if page_text:
                full_text += page_text
        except Exception as e:
            logging.warning(f"Failed to extract text from page {page_num} of {pdf_path}: {e}")

    if not full_text.strip():
        return "EMPTY_DOCUMENT"

    pdf_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, full_text)

    return pdf_uuid



def get_pdf_id(pdf_path):
    pdf_uuid = compute_pdf_id(pdf_path)
    pdf_id = str(pdf_uuid)
    return pdf_id




def which_qdrant(client):
    try:
        if "qdrant_local" in str(type(client._client)):
            qdrant_location = "local"
        elif "qdrant_remote" in str(type(client._client)):
            qdrant_location = "cloud"
        else:
            qdrant_location = "unknown"
        print(f"qdrant location: {qdrant_location}")
    except qdrant_exceptions.UnexpectedResponse as e:
        if "404" in str(e):
            print("The server returned a 404 Not Found error, indicating the server is active but could not find the requested URL or endpoint. This might be due to a wrong URL, an incorrect path, or a resource that doesn't exist.")
        else:
            raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return qdrant_location
    

from qdrant_client.http import models


def list_collections(client):
    collections_response = client.get_collections()
    collection_names = [c.name for c in collections_response.collections]
    print("\nAvailable collections:")
    for name in collection_names:
        print(name)


def close_qdrant(client):
    if 'qdrant' in globals():
        print('deleting qdrant')
        del qdrant

    if 'client' in globals():
        print('closing client')
        client.close()    # Release the database from this process
        del client

def is_pdf_id_in_qdrant(client, CONFIG, pdf_id: str) -> bool:
    '''Helper function checks if pdf_id is already in Qdrant'''

    response = client.count(
        collection_name=CONFIG["qdrant_collection_name"],
        count_filter=models.Filter(
            must=[
                models.FieldCondition(
                    key="metadata.pdf_id",
                    match=models.MatchText(text=pdf_id),
                ),
            ]
        ),
        exact=True,  # Ensures accurate count
    )

    return response.count > 0


def check_qdrant_record_exists(record_id, qdrant, pdfs_collection_name):
    try:
        result = qdrant.retrieve(
            collection_name=pdfs_collection_name,
            ids=[record_id],  
            with_payload=False,
        )
        if result:
            return True
        else:
            return False
    except Exception as e:
        print(f"Error occurred while checking record existence for ID {record_id}: {e}")
        return False



from datetime import datetime, timezone
import pandas as pd


def get_planned_metadata_for_single_record(pdf_id, metadata_source_path):
    """returns a dictionary of strings"""
    # addresses entire metadata file as well as single record items together due to efficiency of logic

    try:
        # Reads excel, ensuring that unit is brough in as a string
        df = pd.read_excel(metadata_source_path, dtype={"unit": str})

        # Find the metadata row in df that corresponds to this pdf_id
        pdf_metadata = df[df['pdf_id'].str.strip().astype(
            str).str.lower() == pdf_id.lower()]

        if pdf_metadata.empty:
            raise ValueError(f"No metadata found for pdf: {pdf_id} in {metadata_source_path}")

            
        # Confirm no duplicate pdf_ids in the metadata
        if len(pdf_metadata) > 1:
            raise ValueError(
                f"Found duplicates for pdf_id: '{pdf_id}', number of results: {len(pdf_metadata)}")
        
        pdf_metadata = pdf_metadata.iloc[0].copy()
        print(f"Successfully accessed metadata for pdf: {pdf_id}")
        
        # Confirm no duplicate publication_numbers in the metadata
        publication_number = pdf_metadata['publication_number']
        if df[df['publication_number'] == publication_number].shape[0] > 1:
            raise ValueError(
                f"Found duplicates for publication_number: '{publication_number}'")
            
        # Confirm no existing upsert date
        if pd.notna(pdf_metadata['upsert_date']):
            raise ValueError(
                f"Existing upsert_date found")

        # Set the upsert date for single record
        pdf_metadata['upsert_date'] = datetime.now(
            timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        print(
            f"Successfully added upsert date {pdf_metadata['upsert_date']} to metadata")
        
        
        # Confirm proper time format that is filterable in Qdrant
        # As long as values are clean ISO 8601 strings, Qdrant will parse correctly
        for col in ['upsert_date', 'effective_date', 'expiration_date']:
            if col in pdf_metadata.index:
                try:
                    parsed = pd.to_datetime(pdf_metadata[col], errors='raise', utc=True)
                    pdf_metadata[col] = parsed.strftime('%Y-%m-%dT%H:%M:%SZ')
                except Exception as e:
                    raise ValueError(f"Invalid date format in column '{col}' for pdf_id '{pdf_id}':\n{pdf_metadata[col]}\n\nOriginal error: {e}")

                
        # Replace NaN with empty strings across all metadata
        pdf_metadata = pdf_metadata.fillna('')
        
        # Set data types to string except booleans across all metadata
        document_metadata = {
            key: value if isinstance(value, bool) else str(
                value) if value is not None else ''
            for key, value in pdf_metadata.to_dict().items()
        }

        # Confirm required fields not blank
        required_fields = ['title', 'scope', 'aux_specific',
                            'public_release', 'pdf_file_name', 'embedding']
        for field in required_fields:
            if field not in pdf_metadata or pd.isna(pdf_metadata[field]) or pdf_metadata[field] == '':
                raise ValueError(
                    f"Required field: '{field}' is empty or missing")
                

        return document_metadata

    except Exception as e:
        print(f"Error retrieving metadata for {pdf_id}: {e}")
        return None  # Return None if an error occurs to continue with the loop

