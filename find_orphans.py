"""
Agent: find_orphans.py


"""

import os
import pandas as pd
import logging
from google_utils import list_pdfs_in_folder, fetch_sheet, fetch_sheet_as_df
from log_writer import log_events




def find_orphans(drive_client, sheets_client) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Identify and flag orphan records and files across LIBRARY_UNIFIED and Google Drive folders.
    
    Performs integrity checks:
    - Finds orphan rows: LIBRARY_UNIFIED entries whose pdf_id has no file in TAGGING, LIVE, or ARCHIVE
    - Finds orphan files: PDFs in folders with no matching pdf_id in LIBRARY_UNIFIED
    - Flags orphan rows by setting status to 'orphan_row'
    - Logs findings to ADMIN_EVENT_LOG

    Returns:
        DataFrame: A pandas dataframe of events  that were written to the log. If no issues were found, returns an empty dataframe.
    """
        
    df = fetch_sheet_as_df(sheets_client, os.environ["LIBRARY_UNIFIED"])
    sheet = fetch_sheet(sheets_client, os.environ["LIBRARY_UNIFIED"])
    df["pdf_id"] = df["pdf_id"].astype(str)

    # Gather files only from TAGGING and LIVE
    drive_files = []
    for folder_name in ["PDF_TAGGING", "PDF_LIVE"]:
        folder_id = os.getenv(folder_name)
        logging.info(f"Catalogging {folder_name} for orphan review")
        files_df = list_pdfs_in_folder(drive_client, folder_id)
        files_df["source_folder"] = folder_name
        drive_files.append(files_df)
        

    all_files_df = pd.concat(drive_files, ignore_index=True)
    all_files_df["pdf_id"] = all_files_df["Name"].apply(lambda name: os.path.splitext(name)[0])

    log_entries = []
    updates = []

    # --- Orphan ROWS ---
    file_ids_in_drive = set(all_files_df["ID"].astype(str).unique())
    orphan_rows = df[~df["google_id"].astype(str).isin(file_ids_in_drive)]
    logging.info(f"⚠️ Found {len(orphan_rows)} orphan rows in LIBRARY_UNIFIED.")

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

    # --- Orphan FILES ---
    google_ids_in_sheet = set(df["google_id"].astype(str).unique())
    orphan_files = all_files_df[~all_files_df["ID"].astype(str).isin(google_ids_in_sheet)]
    logging.info(f"⚠️ Found {len(orphan_files)} orphan files in Google Drive folders.")

    for _, row in orphan_files.iterrows():
        folder = row.get("source_folder", "unknown_folder")
        log_entries.append({
            "action": f"orphan_file_detected_in_{folder}",
            "pdf_id": row["ID"],
            "pdf_file_name": row["Name"]
        })

    # --- Log + Return ---
    if log_entries:
        return orphan_rows, orphan_files, pd.DataFrame(log_entries)

