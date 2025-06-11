import os
import logging
from typing import List, Dict, Union
import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.http import exceptions as qdrant_exceptions
from qdrant_client import models

from env_config import RAG_CONFIG

from env_config import env_config

config = env_config()

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
    logging.info("Initializing Qdrant client in '%s' mode...", mode)

    try:
        if mode == "cloud":
            client = QdrantClient(
                url=config["QDRANT_URL"],
                api_key=config["QDRANT_API_KEY"]
            )
        elif mode == "local":
            client = QdrantClient(
                path=config["QDRANT_PATH"]
            )
        else:
            raise ValueError(f"Invalid mode '{mode}'. Qdrant client must be 'cloud' or 'local'.")
    except Exception as e:
        logging.error("Failed to initialize Qdrant client: %s", e)
        raise

    # Confirm configured collection exists
    collections = list_collections(client)
    if not collections:
        logging.warning("No collections found in Qdrant.")
    else:
        logging.info("Available Qdrant collections: %s", collections)

    expected_collection = RAG_CONFIG.get("qdrant_collection_name")
    if expected_collection in collections:
        logging.info("✅ Confirmed Qdrant collection '%s' exists", expected_collection)
    else:
        logging.error("❌ Collection '%s' not found in Qdrant!", expected_collection)
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

        logging.info("Qdrant location detected: %s", qdrant_location)

    except qdrant_exceptions.UnexpectedResponse as e:
        if "404" in str(e):
            logging.warning("The server returned a 404 Not Found error — server active but wrong URL or endpoint.")
        else:
            raise
        qdrant_location = "unknown"
    except Exception as e:
        logging.warning("An unexpected error occurred while detecting Qdrant location: %s", e)
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
        logging.warning("Error listing collections: %s", e)
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
        logging.info("PDF ID '%s' existence in collection '%s': %s", pdf_id, collection_name, exists)
        return exists
    except Exception as e:
        logging.warning("Error checking PDF ID in Qdrant: %s", e)
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
        logging.info("Record ID '%s' existence in collection '%s': %s", record_id, collection_name, exists)
        return exists
    except Exception as e:
        logging.warning("Error checking record existence in Qdrant: %s", e)
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
            limit=100000
        )

        records = all_records[0]

        unique_pdf_ids = set()
        for record in records:
            payload = record.payload

            if not isinstance(payload, dict):
                logging.warning("🚫 Payload at index %s is not a dict: %s", idx, payload)
                print(f"⚠️ Malformed payload: {payload}")
                continue

            metadata = payload.get("metadata")
            if not isinstance(metadata, dict):
                logging.warning("🚫 metadata missing or not a dict at index %s: %s", idx, payload)
                continue

            pdf_id = metadata.get("pdf_id")
            if pdf_id:
                unique_pdf_ids.add(str(pdf_id))

        logging.info("Retrieving all %s pdf_ids from Qdrant collection.", len(unique_pdf_ids))
        return list(unique_pdf_ids)

    except Exception as e:
        logging.error("Error retrieving pdf_ids from Qdrant: %s", e)
        return []



def get_summaries_by_pdf_id(client: QdrantClient, collection_name: str, pdf_ids: List[str]) -> pd.DataFrame:
    """
    Retrieve summaries of specific records in a Qdrant collection, grouped by metadata.pdf_id.

    Args:
        client (QdrantClient): Qdrant client instance.
        collection_name (str): Name of the Qdrant collection.
        pdf_ids (List[str]): List of pdf_ids to retrieve summaries for.

    Returns:
        pd.DataFrame: A dataframe with columns:
            - pdf_id (str)
            - pdf_file_name (str, if available)
            - title (str, if available)
            - record_count (int)
            - page_count (int, max page_number + 1)
            - point_ids (List[str])
    """
    if not pdf_ids:
        return pd.DataFrame(columns=["pdf_id", "pdf_file_name", "title", "record_count", "page_count", "point_ids"])

    summary = {}
    scroll_offset = None
    while True:
        results, scroll_offset = client.scroll(
            collection_name=collection_name,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="metadata.pdf_id",
                        match=models.MatchAny(any=pdf_ids)
                    )
                ]
            ),
            with_payload=True,
            with_vectors=False,
            limit=100000,
            offset=scroll_offset
        )

        for record in results:
            payload = record.payload
            point_id = record.id

            if not isinstance(payload, dict):
                logging.warning("🚫 Skipping record with non-dict payload: %s", payload)
                continue

            metadata = payload.get("metadata", {})
            if not isinstance(metadata, dict):
                logging.warning("🚫 Skipping record with malformed metadata: %s", payload)
                continue
            
            pdf_id = metadata.get("pdf_id")
            if not pdf_id:
                logging.warning("⚠️ Skipping record without pdf_id: %s", payload)
                continue
            
            title = metadata.get("title")
            pdf_file_name = metadata.get("pdf_file_name")
            page_count = metadata.get("page_count")

            if pdf_id not in summary:
                summary[pdf_id] = {
                    "pdf_id": pdf_id,
                    "title": title,
                    "pdf_file_name": pdf_file_name,
                    "page_count": page_count,
                    "record_count": 1,
                    "point_ids": [point_id]
                }
            else:
                summary[pdf_id]["record_count"] += 1
                summary[pdf_id]["point_ids"].append(point_id)
                if not summary[pdf_id]["title"] and title:
                    summary[pdf_id]["title"] = title
                if not summary[pdf_id]["pdf_file_name"] and pdf_file_name:
                    summary[pdf_id]["pdf_file_name"] = pdf_file_name
                if not summary[pdf_id]["page_count"] and page_count:
                    summary[pdf_id]["page_count"] = page_count

        if scroll_offset is None:
            break

    return pd.DataFrame(summary.values())


    

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
            logging.info("🗑️ Deleting records for pdf_id: %s", pdf_id)
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
            logging.info("✅ Deleted points for pdf_id %s. Operation ID: %s", pdf_id, result.operation_id)
            if log_event_fn:
                log_event_fn("orphan_qdrant_record_deleted", pdf_id, f"Deleted from {collection_name}")
        except Exception as e:
            logging.error("❌ Failed to delete records for pdf_id %s: %s", pdf_id, e)
