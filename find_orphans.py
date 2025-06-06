import os
import pandas as pd
import logging
from typing import Tuple
from env_config import RAG_CONFIG
from gcp_utils import list_pdfs_in_folder, fetch_sheet, fetch_sheet_as_df
from qdrant_utils import get_all_pdf_ids_in_qdrant, delete_record_by_pdf_id
from log_writer import log_event



def find_orphan_rows(sheets_client: SheetsClient, df: pd.DataFrame, all_file_ids: set) -> Tuple[pd.DataFrame, list[dict]]: # type: ignore
    """Identify orphan rows and flag them in the sheet."""
    log_entries = []
    updates = []

    orphan_rows = df[~df["google_id"].astype(str).isin(all_file_ids)]
    logging.info(f"⚠️ Found {len(orphan_rows)} orphan rows in LIBRARY_UNIFIED.")

    sheet = fetch_sheet(sheets_client, os.environ["LIBRARY_UNIFIED"]) # type: ignore

    for _, row in orphan_rows.iterrows():
        pdf_id = row["pdf_id"]
        google_id = row.get("google_id", "unknown_id")
        filename = row.get("pdf_file_name", "unknown_filename")
        idx = df.index[df["pdf_id"] == pdf_id][0]
        df.at[idx, "status"] = "orphan_row"

        log_entries.append({
            "action": "orphan_row_flagged in LIBRARY_UNIFIED",
            "pdf_id": google_id,
            "pdf_file_name": filename
        })

        row_idx = idx + 2
        updates.append({
            "range": f"A{row_idx}:{row_idx}",
            "values": [df.loc[idx].tolist()]
        })

    if updates:
        try:
            sheet.batch_update(updates, value_input_option="RAW")
        except Exception as e:
            logging.error(f"Failed batch row update: {e}")

    return orphan_rows, log_entries

def find_orphan_files(df: pd.DataFrame, all_files_df: pd.DataFrame) -> Tuple[pd.DataFrame, list[dict]]:
    """Identify orphan files not referenced in LIBRARY_UNIFIED."""
    google_ids_in_sheet = set(df["google_id"].astype(str).unique())
    orphan_files = all_files_df[~all_files_df["ID"].astype(str).isin(google_ids_in_sheet)]
    logging.info(f"⚠️ Found {len(orphan_files)} orphan files in Google Drive folders.")

    log_entries = []
    for _, row in orphan_files.iterrows():
        folder = row.get("source_folder", "unknown_folder")
        log_entries.append({
            "action": f"orphan_file_detected_in_{folder}",
            "pdf_id": row["ID"],
            "pdf_file_name": row["Name"]
        })

    return orphan_files, log_entries


def find_orphan_qdrant_records(qdrant_client: QdrantClient, collection_name: str, library_df: pd.DataFrame) -> Tuple[pd.DataFrame, list[dict]]:
    """
    Find Qdrant records whose pdf_id is not marked as 'live' in LIBRARY_UNIFIED.

    Args:
        qdrant_client: Qdrant client instance.
        collection_name: Qdrant collection name.
        library_df: DataFrame of LIBRARY_UNIFIED.

    Returns:
        Tuple of:
            - orphan_qdrant_df (DataFrame): DataFrame of orphan pdf_ids.
            - log_entries (list of dict): Corresponding log actions.
    """
    qdrant_pdf_ids = get_all_pdf_ids_in_qdrant(qdrant_client, collection_name)
    live_pdf_ids = set(library_df[library_df["status"] == "live"]["pdf_id"].astype(str))

    orphan_ids = [pdf_id for pdf_id in qdrant_pdf_ids if pdf_id not in live_pdf_ids]
    log_entries = []

    if orphan_ids:
        logging.warning(f"Found {len(orphan_ids)} orphan Qdrant records: {orphan_ids}")
        log_entries = [{
            "action": "orphan_record_detected_in_qdrant",
            "pdf_id": pdf_id
        } for pdf_id in orphan_ids]

    orphan_qdrant_records = pd.DataFrame({"pdf_id": orphan_ids})
    return orphan_qdrant_records, log_entries



def find_orphans(drive_client: DriveClient, sheets_client: SheetsClient, qdrant_client: QdrantClient) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:  # type: ignore
    """
    Identify and flag orphan records and files across LIBRARY_UNIFIED, Google Drive folders, and Qdrant.

    Returns:
        Tuple of:
            - orphan_rows (DataFrame)
            - orphan_files (DataFrame)
            - orphan_qdrant_records (DataFrame)
            - log_entries (DataFrame)
    """
    df = fetch_sheet_as_df(sheets_client, os.environ["LIBRARY_UNIFIED"])  # type: ignore

    if df.empty or "google_id" not in df.columns or "pdf_id" not in df.columns:
        raise ValueError("LIBRARY_UNIFIED missing required columns or is empty")

    df["pdf_id"] = df["pdf_id"].astype(str)

    drive_files = []
    for folder_name in ["PDF_TAGGING", "PDF_LIVE"]:
        folder_id = os.getenv(folder_name)
        logging.info(f"Cataloging {folder_name} for orphan review")
        files_df = list_pdfs_in_folder(drive_client, folder_id)
        files_df["source_folder"] = folder_name
        drive_files.append(files_df)

    all_files_df = pd.concat(drive_files, ignore_index=True)
    all_file_ids = set(all_files_df["ID"].astype(str).unique())

    orphan_rows, row_log_entries = find_orphan_rows(sheets_client, df, all_file_ids)
    orphan_files, file_log_entries = find_orphan_files(df, all_files_df)
    orphan_qdrant_records, qdrant_log_entries = find_orphan_qdrant_records(qdrant_client, RAG_CONFIG["qdrant_collection_name"], df)

    all_log_entries = row_log_entries + file_log_entries + qdrant_log_entries
    log_df = pd.DataFrame(all_log_entries)

    logging.info(f"Returning {len(log_df)} total log entries.")
    return orphan_rows, orphan_files, orphan_qdrant_records, log_df



def delete_orphan_qdrant_records(
    qdrant_client: QdrantClient,
    orphan_qdrant_records: pd.DataFrame 
) -> None:
    """
    Deletes all Qdrant records whose pdf_id appears in the orphan_qdrant_df DataFrame.

    Args:
        client (QdrantClient): Qdrant client instance.
        orphan_qdrant_records (pd.DataFrame): DataFrame with orphaned pdf_ids.
    """
    pdf_ids = orphan_qdrant_records["pdf_id"].dropna().unique()
    collection_name = RAG_CONFIG["qdrant_collection_name"]
    
    for pdf_id in pdf_ids:
        try:
            logging.info(f"🗑️ Deleting orphaned Qdrant records for pdf_id: {pdf_id}")
            delete_record_by_pdf_id(qdrant_client, collection_name, pdf_id)
            log_event("orphan_qdrant_record_deleted", pdf_id, f"Deleted from {collection_name}")
        except Exception as e:
            logging.error(f"❌ Failed to delete records for pdf_id {pdf_id} from {collection_name}: {e}")
            log_event("orphan_qdrant_record_delete_failed", pdf_id, str(e))
