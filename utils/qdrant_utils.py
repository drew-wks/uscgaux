import logging
from typing import List, Union
import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.http import exceptions as qdrant_exceptions
from qdrant_client import models
from env_config import env_config, RAG_CONFIG
from utils.gcp_utils import fetch_sheet_as_df


def _validate_metadata(metadata: dict, require_file_id: bool = False) -> bool:
    """Return True if metadata has required keys."""
    if not isinstance(metadata, dict):
        logging.warning("🚫 metadata missing or not a dict: %s", metadata)
        return False

    pdf_id = metadata.get("pdf_id")
    gcp_file_id = metadata.get("gcp_file_id") or metadata.get("file_id")
    if not pdf_id or (require_file_id and not gcp_file_id):
        logging.warning("🚫 Missing required metadata keys: %s", metadata)
        return False
    return True

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

    except qdrant_exceptions.UnexpectedResponse as e:
        if "404" in str(e):
            logging.warning("The server returned a 404 Not Found — server is reachable, but URL or endpoint may be wrong.")
        else:
            logging.exception("Unexpected response from Qdrant")
        raise

    except (qdrant_exceptions.ResponseHandlingException, TypeError, ValueError):
        logging.exception("An unexpected error occurred during Qdrant client initialization or collection verification.")
        raise

    except Exception:
        logging.exception("General failure during Qdrant client setup.")
        raise



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
    except (qdrant_exceptions.ResponseHandlingException,
                TypeError, ValueError):
        logging.exception("An unexpected error occurred while detecting Qdrant location: %s")
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
    except (qdrant_exceptions.UnexpectedResponse,
                qdrant_exceptions.ResponseHandlingException,
                TypeError, ValueError):
        logging.exception("Error listing collections: %s")
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
    except (qdrant_exceptions.UnexpectedResponse,
                qdrant_exceptions.ResponseHandlingException,
                TypeError, ValueError):
        logging.exception("Error checking PDF ID in Qdrant: %s")
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
    except (qdrant_exceptions.UnexpectedResponse,
            qdrant_exceptions.ResponseHandlingException,
            TypeError, ValueError):
        logging.exception("Error checking record existence in Qdrant: %s")
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
        for idx, record in enumerate(records):
            payload = record.payload

            if not isinstance(payload, dict):
                logging.warning("🚫 Payload at index %s is not a dict: %s", idx, payload)
                logging.warning(f"⚠️ Malformed payload: {payload}")
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

    except (qdrant_exceptions.UnexpectedResponse,
                qdrant_exceptions.ResponseHandlingException,
                TypeError, ValueError):
        logging.exception("Error retrieving pdf_ids from Qdrant: %s")
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
            
    Notes:
    - Records with invalid metadata (e.g., missing pdf_id) are skipped.
    - `gcp_file_id` is not required in metadata for a record to be counted.
    - `title`, `pdf_file_name`, and `page_count` are taken from the first valid record that contains them.
    - All matching point IDs for a given pdf_id are collected into the `point_ids` list.
    - The function performs a full scroll of all matching points in batches (limit=100,000).
    - Returns an empty DataFrame if no matching pdf_ids are found or input is empty.
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
            # Presence checks should not require a gcp_file_id since
            # older records may not have it populated. Validate only
            # that a pdf_id exists so these points are counted.
            if not _validate_metadata(metadata, require_file_id=False):
                continue

            pdf_id = metadata.get("pdf_id")
            
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


def get_gcp_file_ids_by_pdf_id(client: QdrantClient, collection_name: str, pdf_ids: List[str]) -> pd.DataFrame:
    """
    Retrieve all unique gcp_file_id values from Qdrant metadata for each specified pdf_id.

    This function scans the Qdrant collection for records matching the given pdf_ids
    and extracts the associated 'gcp_file_id' (or legacy 'file_id') values from each record's metadata.
    It groups the file IDs by pdf_id and returns a summary including how many unique file IDs were found.

    Args:
        client (QdrantClient): An initialized Qdrant client.
        collection_name (str): The name of the Qdrant collection to query.
        pdf_ids (List[str]): A list of pdf_id values to match against metadata.pdf_id.

    Returns:
        pd.DataFrame: A DataFrame with the following columns:
            - pdf_id (str): The identifier of the PDF document.
            - gcp_file_ids (List[str]): A sorted list of unique file IDs associated with this pdf_id.
            - unique_file_count (int): The number of distinct file IDs found for this pdf_id.

    Notes:
        - Records that lack a gcp_file_id are still included (with an empty list).
        - Records with invalid or missing metadata are skipped.
        - This function does not return duplicate file IDs for the same pdf_id.
    """
    if not pdf_ids:
        return pd.DataFrame(columns=["pdf_id", "gcp_file_ids", "unique_file_count"])

    file_map: dict[str, set[str]] = {}
    filter_condition = models.Filter(
        must=[models.FieldCondition(key="metadata.pdf_id", match=models.MatchAny(any=pdf_ids))]
    )
    scroll_offset = None
    while True:
        results, scroll_offset = client.scroll(
            collection_name=collection_name,
            scroll_filter=filter_condition,
            with_payload=True,
            with_vectors=False,
            limit=100000,
            offset=scroll_offset,
        )
        for rec in results:
            payload = rec.payload
            if not isinstance(payload, dict):
                continue
            meta = payload.get("metadata", {})
            # Include records even when gcp_file_id is missing so that
            # build_status_map can flag them appropriately.
            if not _validate_metadata(meta, require_file_id=False):
                continue
            pid = meta.get("pdf_id")
            fid = meta.get("gcp_file_id") or meta.get("file_id")
            file_map.setdefault(str(pid), set())
            if fid:
                file_map[str(pid)].add(str(fid))
        if scroll_offset is None:
            break

    rows = [
        {"pdf_id": pid, "gcp_file_ids": sorted(list(fids)), "unique_file_count": len(fids)}
        for pid, fids in file_map.items()
    ]
    return pd.DataFrame(rows)


def get_unique_metadata_df(client: QdrantClient, collection_name: str) -> pd.DataFrame:
    """Return dataframe of unique metadata across all records.

    Each row corresponds to a unique set of metadata values with ``page``
    removed. A ``point_ids`` column lists all record IDs that share that
    metadata. Records lacking ``pdf_id`` are included with an empty string.

    Args:
        client: Initialized Qdrant client.
        collection_name: Name of the Qdrant collection to scan.

    Returns:
        DataFrame where columns are metadata keys plus ``point_ids``.
    """

    metadata_map: dict[tuple, dict] = {}
    scroll_offset = None
    while True:
        results, scroll_offset = client.scroll(
            collection_name=collection_name,
            scroll_filter=None,
            with_payload=True,
            with_vectors=False,
            limit=100000,
            offset=scroll_offset,
        )

        for rec in results:
            payload = rec.payload
            if not isinstance(payload, dict):
                continue
            meta = payload.get("metadata")
            if not isinstance(meta, dict):
                continue

            meta = meta.copy()
            meta.pop("page", None)
            if meta.get("pdf_id") is None:
                meta["pdf_id"] = ""

            key = tuple(sorted(meta.items()))
            if key not in metadata_map:
                metadata_map[key] = {"metadata": meta, "point_ids": [rec.id]}
            else:
                metadata_map[key]["point_ids"].append(rec.id)

        if scroll_offset is None:
            break

    if not metadata_map:
        return pd.DataFrame()

    all_keys = sorted({k for entry in metadata_map.values() for k in entry["metadata"].keys()})
    rows: list[dict] = []
    for entry in metadata_map.values():
        row = {k: entry["metadata"].get(k) for k in all_keys}
        row["point_ids"] = entry["point_ids"]
        rows.append(row)

    return pd.DataFrame(rows)


def update_file_id_for_pdf_id(client: QdrantClient, collection_name: str, pdf_id: str, gcp_file_id: str) -> bool:
    """Update metadata.gcp_file_id for all points matching pdf_id."""
    try:
        filter_condition = models.Filter(
            must=[
                models.FieldCondition(
                    key="metadata.pdf_id",
                    match=models.MatchValue(value=pdf_id)
                )
            ]
        )
        client.set_payload(
            collection_name=collection_name,
            payload={"gcp_file_id": gcp_file_id},
            points=filter_condition,
            key="metadata"
        )
        logging.info("Updated gcp_file_id for pdf_id %s to %s", pdf_id, gcp_file_id)
        return True
    except Exception:
        logging.exception("Failed to update gcp_file_id for pdf_id %s", pdf_id)
        return False


def update_qdrant_file_ids_for_live_rows(qdrant_client: QdrantClient, sheets_client, collection_name: str | None = None) -> pd.DataFrame:
    """Sync gcp_file_id into Qdrant for every live row in LIBRARY_UNIFIED."""
    library_df = fetch_sheet_as_df(sheets_client, config["LIBRARY_UNIFIED"])
    if library_df.empty or "status" not in library_df.columns:
        return pd.DataFrame()
    live_df = library_df[library_df["status"] == "live"]
    results = []
    for _, row in live_df.iterrows():
        pdf_id = str(row.get("pdf_id", ""))
        file_id = str(row.get("gcp_file_id", ""))
        if not pdf_id or not file_id:
            continue
        success = update_file_id_for_pdf_id(qdrant_client, collection_name, pdf_id, file_id)
        results.append({"pdf_id": pdf_id, "gcp_file_id": file_id, "updated": success})
    return pd.DataFrame(results)


def delete_records_by_pdf_id(
    client: QdrantClient,
    collection_name: str,
    pdf_ids: Union[List[str], pd.Series],
    log_event_fn=None
) -> None:
    """
    Deletes all Qdrant vectors whose metadata.pdf_id matches any in the given list.

    Args:
        client (QdrantClient): An initialized Qdrant client.
        collection_name (str): Name of the Qdrant collection.
        pdf_ids (List[str] or pd.Series): Unique PDF IDs to delete.
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
                log_event_fn("orphan_qdrant_record_deleted", str(pdf_id), f"Deleted from {collection_name}")

        except (qdrant_exceptions.UnexpectedResponse,
                qdrant_exceptions.ResponseHandlingException,
                TypeError, ValueError):
            logging.exception("❌ Failed to delete records for pdf_id %s", pdf_id)