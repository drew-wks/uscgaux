
"""
Agent: cleanup_orphans.py

Performs integrity checks:
- Finds orphan rows: LIBRARY_UNIFIED entries whose pdf_id has no file in TAGGING, LIVE, or ARCHIVE
- Finds orphan files: PDFs in folders with no matching pdf_id in LIBRARY_UNIFIED
- Flags orphan rows by setting status to 'orphan_row'
- Logs findings to ADMIN_EVENT_LOG
"""

import os
import pandas as pd
import logging
from google_utils import get_gcp_clients, list_pdfs_in_drive_folder, get_folder_name
from admin_config import GOOGLE_CONFIG
from log_writer import log_admin_event


def cleanup_orphans():
    drive_client, sheets_client = get_gcp_clients()

    # Load LIBRARY_UNIFIED
    sheet = sheets_client.open_by_key(os.environ["LIBRARY_UNIFIED"])
    df = pd.DataFrame(sheet.worksheet("Sheet1").get_all_records())
    df["pdf_id"] = df["pdf_id"].astype(str)

    # Gather all Drive files across folders
    drive_files = []
    for folder_name, folder_id in GOOGLE_CONFIG.items():
        files_df = list_pdfs_in_drive_folder(drive_client, folder_id)
        files_df["source_folder"] = folder_name
        drive_files.append(files_df)
    all_files_df = pd.concat(drive_files, ignore_index=True)
    all_files_df["pdf_id"] = all_files_df["Name"].apply(lambda name: name.split(".")[0])

    # --- Orphan ROWS ---
    pdf_ids_in_drive = set(all_files_df["pdf_id"].unique())
    orphan_rows = df[~df["pdf_id"].isin(pdf_ids_in_drive)]

    for _, row in orphan_rows.iterrows():
        pdf_id = row["pdf_id"]
        filename = row["filename"]
        idx = df.index[df["pdf_id"] == pdf_id][0]
        df.at[idx, "status"] = "orphan_row"
        log_admin_event("orphan_row_flagged", pdf_id, filename)

    # Write back updated rows
    sheet.values_update(
        "Sheet1" + "!A2",
        params={"valueInputOption": "RAW"},
        body={"values": df.values.tolist()},
    )

    # --- Orphan FILES ---
    pdf_ids_in_sheet = set(df["pdf_id"].unique())
    orphan_files = all_files_df[~all_files_df["pdf_id"].isin(pdf_ids_in_sheet)]

    for _, row in orphan_files.iterrows():
        folder = row.get("source_folder", "unknown_folder")
        log_admin_event(f"orphan_file_detected_in_{folder}", row["pdf_id"], row["Name"])


if __name__ == "__main__":
    cleanup_orphans()
