import os
import logging
from typing import List, Dict, Union
import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.http import models, exceptions as qdrant_exceptions
from env_config import RAG_CONFIG



def init_qdrant_client(mode: str = "cloud") -> QdrantClient:
    """
    Initialize a Qdrant client (local or cloud), confirm connectivity, 
    and verify the presence of the configured collection.

    Args:
        mode (str): 'cloud' or 'local'

    Returns:
        QdrantClient: Initialized and verified client instance
    """
    mode = mode.lower()
    logging.info(f"Initializing Qdrant client in '{mode}' mode...")

    try:
        if mode == "cloud":
            client = QdrantClient(
                url=os.environ["QDRANT_URL"],
                api_key=os.environ["QDRANT_API_KEY"]
            )
        elif mode == "local":
            client = QdrantClient(
                path=os.environ["QDRANT_PATH"]
            )
        else:
            raise ValueError(f"Invalid mode '{mode}'. Qdrant client must be 'cloud' or 'local'.")
    except Exception as e:
        logging.error(f"Failed to initialize Qdrant client: {e}")
        raise

    # Confirm configured collection exists
    collections = list_collections(client)
    if not collections:
        logging.warning("No collections found in Qdrant.")
    else:
        logging.info(f"Available Qdrant collections: {collections}")

    expected_collection = RAG_CONFIG.get("qdrant_collection_name")
    if expected_collection in collections:
        logging.info(f"✅ Confirmed Qdrant collection '{expected_collection}' exists")
    else:
        logging.error(f"❌ Collection '{expected_collection}' not found in Qdrant!")
        raise ValueError(f"Collection '{expected_collection}' does not exist in Qdrant.")

    return client



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
        return collection_names
    except Exception as e:
        logging.warning(f"Error listing collections: {e}")
        return []


def in_qdrant(client: QdrantClient, collection_name: str, pdf_id: str) -> bool:
    """
    Check if a document with a specific pdf_id exists in the Qdrant collection.

    Args:
        client: Qdrant client instance.
        collection name (str).
        pdf_id (str): The PDF ID to check.

    Returns:
        bool: True if the document exists, False otherwise.
    """

    if collection_name is None:
        raise ValueError("Missing QDRANT collection name in RAG_CONFIG")
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


def check_record_exists(client: QdrantClient, collection_name: str, record_id: Union[str, int]) -> bool:

    """
    Check if a record with the given ID exists in the specified Qdrant collection.

    Args:
        record_id (str or int): The ID of the record to check.
        qdrant: Qdrant client instance.
        collection_name: name of Qdrant collection

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



def get_all_pdf_ids_in_qdrant(client: QdrantClient, collection_name: str) -> List[str]:
    """
    Retrieve a list of all unique pdf_ids stored in the Qdrant collection.

    Args:
        client: Qdrant client instance.
        collection_name: name of Qdrant collection

    Returns:
        List of unique pdf_ids found in the Qdrant collection.
    """
    try:
        all_records = client.scroll(
            collection_name=collection_name,
            scroll_filter=None,
            with_payload=True,
            with_vectors=False,
            limit=10000
        )

        records = all_records[0]

        unique_pdf_ids = set()
        for record in records:
            payload = record.payload

            if not isinstance(payload, dict):
                logging.warning(f"🚫 Payload at index {idx} is not a dict: {payload}")
                print(f"⚠️ Malformed payload: {payload}")
                continue

            metadata = payload.get("metadata")
            if not isinstance(metadata, dict):
                logging.warning(f"🚫 metadata missing or not a dict at index {idx}: {payload}")
                continue

            pdf_id = metadata.get("pdf_id")
            if pdf_id:
                unique_pdf_ids.add(str(pdf_id))

        logging.info(f"Retrieving all {len(unique_pdf_ids)} pdf_ids from Qdrant collection.")
        return list(unique_pdf_ids)

    except Exception as e:
        logging.error(f"Error retrieving pdf_ids from Qdrant: {e}")
        return []



def delete_records_by_pdf_id(
    client: QdrantClient,
    pdf_ids: Union[List[str], pd.Series],
    collection_name: str,
    log_event_fn=None  # Optional hook for logging externally (e.g. log_event)
) -> None:
    """
    Deletes all Qdrant vectors whose metadata.pdf_id matches any in the given list.

    Args:
        client (QdrantClient): An initialized Qdrant client.
        pdf_ids (List[str] or pd.Series): Unique PDF IDs to delete.
        collection_name (str): Name of the Qdrant collection.
        log_event_fn (callable, optional): Function to log deletion events externally.
    """
    if collection_name is None:
        raise ValueError("Missing Qdrant collection name.")

    unique_pdf_ids = pd.Series(pdf_ids).dropna().unique()

    if len(unique_pdf_ids) == 0:
        logging.info("🟡 No PDF IDs provided to delete from Qdrant.")
        return

    for pdf_id in unique_pdf_ids:
        try:
            logging.info(f"🗑️ Deleting records for pdf_id: {pdf_id}")
            filter_condition = models.Filter(
                must=[
                    models.FieldCondition(
                        key="metadata.pdf_id",
                        match=models.MatchText(text=pdf_id)
                    )
                ]
            )
            result = client.delete(
                collection_name=collection_name,
                points_selector=filter_condition
            )
            logging.info(f"✅ Deleted points for pdf_id {pdf_id}. Operation ID: {result.operation_id}")
            if log_event_fn:
                log_event_fn("orphan_qdrant_record_deleted", pdf_id, f"Deleted from {collection_name}")
        except Exception as e:
            logging.error(f"❌ Failed to delete records for pdf_id {pdf_id}: {e}")
