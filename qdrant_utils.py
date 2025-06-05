import os
import logging
from typing import List, Dict, Union
from env_config import RAG_CONFIG
from qdrant_client import QdrantClient
from qdrant_client.http import models, exceptions as qdrant_exceptions
from log_writer import log_event



def init_qdrant(mode: str = "cloud") -> QdrantClient:
    mode = mode.lower()
    logging.info(mode)
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


def which_qdrant(client: QdrantClient) -> str:
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


def list_collections(client: QdrantClient) -> List[str]:
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


def in_qdrant(client: QdrantClient, RAG_CONFIG: Dict[str, str], pdf_id: str) -> bool:
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


def check_record_exists(record_id: Union[str, int], client: QdrantClient, collection_name: str) -> bool:

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
        point = client.get_point(collection_name=collection_name, point_id=record_id)
        exists = point is not None
        logging.info(f"Record ID '{record_id}' existence in collection '{collection_name}': {exists}")
        return exists
    except Exception as e:
        logging.warning(f"Error checking record existence in Qdrant: {e}")
        return False



def delete_record_by_pdf_id(client: QdrantClient, collection_name: str, pdf_id: str):
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
        result = client.delete(collection_name=collection_name, points_selector=filter_condition)
        logging.info(f"Deleted points for pdf_id {pdf_id} from {collection_name}. Operation ID: {result.operation_id}")
    except Exception as e:
        logging.error(f"Error deleting points for pdf_id {pdf_id}: {e}")
