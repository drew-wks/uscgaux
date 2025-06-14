import pandas as pd
from typing import Iterable, Tuple, List, Dict, Any

from env_config import env_config
from gcp_utils import fetch_sheet

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
        gcp_file_id = row.get("gcp_file_id", "unknown_id")
        filename = row.get("pdf_file_name", "unknown_filename")
        idx = df.index[df["pdf_id"] == pdf_id][0]
        df.at[idx, "status"] = "orphan_row"

        action_msg = f"orphan_row_flagged in LIBRARY_UNIFIED — pdf_id: {pdf_id}, file: {filename}"
        logging.info(action_msg)

        log_entries.append({
            "action": "orphan_row_flagged in LIBRARY_UNIFIED",
            "pdf_id": gcp_file_id,
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

