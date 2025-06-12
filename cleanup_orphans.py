import logging
from typing import Tuple
import pandas as pd
from gspread.client import Client as SheetsClient
from googleapiclient.discovery import Resource as DriveClient
from qdrant_client import QdrantClient
from env_config import rag_config, env_config
from gcp_utils import list_pdfs_in_folder, fetch_sheet, fetch_sheet_as_df
from qdrant_utils import get_all_pdf_ids_in_qdrant, get_summaries_by_pdf_id


config = env_config()


def flag_rows_as_orphans(sheet, df: pd.DataFrame, orphan_rows: pd.DataFrame) -> list[dict]:
    """
    Update the 'status' column in LIBRARY_UNIFIED and batch update the Google Sheet
    for the rows identified as orphans. Logs status and prepares log entries.

    Args:
        sheet: The gspread worksheet object for LIBRARY_UNIFIED.
        df (pd.DataFrame): Full DataFrame of the sheet.
        orphan_rows (pd.DataFrame): Subset of rows identified as orphans.

    Returns:
        List of dictionaries representing log entries for each flagged row.
    """
    log_entries = []
    updates = []
     
    for _, row in orphan_rows.iterrows():
        pdf_id = row["pdf_id"]
        google_id = row.get("google_id", "unknown_id")
        filename = row.get("pdf_file_name", "unknown_filename")
        idx = df.index[df["pdf_id"] == pdf_id][0]
        df.at[idx, "status"] = "orphan_row"

        action_msg = f"orphan_row_flagged in LIBRARY_UNIFIED — pdf_id: {pdf_id}, file: {filename}"
        logging.info(action_msg)

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
            logging.info("✅ Updated %s orphan rows in LIBRARY_UNIFIED.", len(updates))
        except Exception as e:
            logging.error("❌ Failed batch row update in LIBRARY_UNIFIED: %s", e)

    return log_entries



def find_rows_missing_google_ids(sheets_client: SheetsClient, df: pd.DataFrame, all_file_ids: set) -> Tuple[pd.DataFrame, list[dict]]: 
    """
    Identify rows in the LIBRARY_UNIFIED sheet whose 'google_id' is not found in the set of file IDs,
    and delegate row flagging to a helper function.

    Args:
        sheets_client (SheetsClient): An authenticated Google Sheets client.
        df (pd.DataFrame): DataFrame representing the LIBRARY_UNIFIED sheet.
        all_file_ids (set): Set of valid Google Drive file IDs currently known to exist.

    Returns:
        Tuple[pd.DataFrame, list[dict]]:
            - A DataFrame containing all orphan rows detected.
            - A list of dictionaries representing log entries for flagged rows.
    """
    orphan_rows = df[~df["google_id"].astype(str).isin(all_file_ids)]

    if orphan_rows.empty:
        logging.info("✅ No orphan rows found in LIBRARY_UNIFIED.")
        return pd.DataFrame(columns=df.columns), []

    sheet = fetch_sheet(sheets_client, config["LIBRARY_UNIFIED"])
    log_entries = flag_rows_as_orphans(sheet, df, orphan_rows)

    return orphan_rows, log_entries



def find_files_missing_rows(library_df: pd.DataFrame, all_files_df: pd.DataFrame) -> Tuple[pd.DataFrame, list[dict]]:
    """Identify orphan files in Google Drive not referenced in LIBRARY_UNIFIED.
        - Compares file IDs from Google Drive with the 'google_id' values in the sheet DataFrame.
        - Flags any unmatched files as orphaned.
        - Generates a list of log entries indicating the folder and file metadata.

    Args:
        df (pd.DataFrame): DataFrame representing rows from the LIBRARY_UNIFIED sheet.
        all_files_df (pd.DataFrame): DataFrame of files found in Google Drive folders, with columns like 'ID', 'Name', and 'source_folder'.

    Returns:
        Tuple[pd.DataFrame, list[dict]]:
            - A DataFrame containing all orphan files not referenced in LIBRARY_UNIFIED.
            - A list of dictionaries representing log entries for each orphan file detected.
    """
    google_ids_in_library_df = set(library_df["google_id"].astype(str).unique())
    orphan_files = all_files_df[~all_files_df["ID"].astype(str).isin(google_ids_in_library_df)]
    logging.info("⚠️ Found %s orphans (files in Google Drive folders but not in LIBRARY_UNIFIED).", len(orphan_files))

    log_entries = []
    for _, row in orphan_files.iterrows():
        folder = row.get("folder", "unknown_folder")
        log_entries.append({
            "action": f"orphan_file_detected_in_{folder}",
            "pdf_id": row["ID"],
            "pdf_file_name": row["Name"]
        })

    return orphan_files, log_entries


def find_records_missing_liverows(qdrant_client: QdrantClient, library_df: pd.DataFrame) -> Tuple[pd.DataFrame, list[dict]]:
    """
    Find Qdrant records whose pdf_id is not marked as 'live' in LIBRARY_UNIFIED.

    Args:
        qdrant_client: Qdrant client instance.
        collection_name: Qdrant collection name.
        library_df: DataFrame of LIBRARY_UNIFIED.

    Returns:
        Tuple of:
            - orphan_qdrant_df (DataFrame): Orphan pdf_id records with summary info.
            - log_entries (list of dict): Corresponding log actions.
    """


    qdrant_pdf_ids = get_all_pdf_ids_in_qdrant(qdrant_client, rag_config("qdrant_collection_name"))
    live_pdf_ids = set(library_df[library_df["status"] == "live"]["pdf_id"].astype(str))

    orphan_qdrant_ids = [pdf_id for pdf_id in qdrant_pdf_ids if pdf_id not in live_pdf_ids]
    log_entries = []

    if orphan_qdrant_ids:
        logging.warning("Found %s orphans (records in Qdrant but not in LIBRARY_UNIFIED):\n %s", len(orphan_qdrant_ids), orphan_qdrant_ids)
        log_entries = [{
            "action": "orphan_record_detected_in_qdrant",
            "pdf_id": pdf_id
        } for pdf_id in orphan_qdrant_ids]

        # Create initial orphan DataFrame
        orphan_df = pd.DataFrame({"pdf_id": orphan_qdrant_ids})

        # Get full summaries from Qdrant
        qdrant_summary_df = get_summaries_by_pdf_id(
            qdrant_client, rag_config("qdrant_collection_name"), orphan_qdrant_ids)

        # Merge to include title, file name, page count, etc.
        orphan_qdrant_df = orphan_df.merge(qdrant_summary_df, on="pdf_id", how="left")

        return orphan_qdrant_df, log_entries




def find_orphans(drive_client: DriveClient, sheets_client: SheetsClient, qdrant_client: QdrantClient) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:  
    """
    Identify and flag orphan records and files across LIBRARY_UNIFIED, Google Drive folders, and Qdrant.

    Returns:
        Tuple of:
            - orphan_rows (DataFrame)
            - orphan_files (DataFrame)
            - orphan_qdrant_records (DataFrame)
            - log_entries (DataFrame)
    """
    library_df = fetch_sheet_as_df(sheets_client, config["LIBRARY_UNIFIED"])
    
    if library_df.empty or "google_id" not in library_df.columns or "pdf_id" not in library_df.columns:
        raise ValueError("LIBRARY_UNIFIED missing required columns (google_id, pdf_id) or is empty")

    library_df["pdf_id"] = library_df["pdf_id"].astype(str)

    drive_files_list = []
    for folder_name in ["PDF_TAGGING", "PDF_LIVE"]:
        folder_id = config[folder_name]
        logging.info("Cataloging %s for orphan review", folder_name)
        files_info_df = list_pdfs_in_folder(drive_client, folder_id)
        files_info_df["folder"] = folder_name
        drive_files_list.append(files_info_df)

    tagginglive_files_list = pd.concat(drive_files_list, ignore_index=True)
    tagginglive_file_ids = set(tagginglive_files_list["ID"].astype(str).unique())

    orphan_rows_missing_tagginglivefiles, row_log_entries = find_rows_missing_google_ids(sheets_client, library_df, tagginglive_file_ids)
    orphan_tagginglivefiles_missing_rows, file_log_entries = find_files_missing_rows(library_df, tagginglive_files_list)
    orphan_records_missing_liverows, qdrant_log_entries = find_records_missing_liverows(qdrant_client, library_df)

    all_log_entries = row_log_entries + file_log_entries + qdrant_log_entries
    log_df = pd.DataFrame(all_log_entries)

    logging.info("Returning %s total log entries.", len(log_df))
    return orphan_rows_missing_tagginglivefiles, orphan_tagginglivefiles_missing_rows, orphan_records_missing_liverows, log_df

