import logging
import pandas as pd
from typing import List
from env_config import env_config, rag_config
from gcp_utils import fetch_sheet_as_df, list_pdfs_in_folder
from qdrant_utils import get_summaries_by_pdf_id, get_file_ids_by_pdf_id

config = env_config()


def build_status_map(drive_client, sheets_client, qdrant_client) -> pd.DataFrame:
    """Build a consolidated status map across Sheet, Drive and Qdrant."""
    library_df = fetch_sheet_as_df(sheets_client, config["LIBRARY_UNIFIED"])
    if library_df.empty:
        logging.warning("LIBRARY_UNIFIED is empty or unavailable")
        return pd.DataFrame()

    live_df = library_df[library_df.get("status") == "live"].copy()
    if live_df.empty:
        return pd.DataFrame()

    live_df["pdf_id"] = live_df["pdf_id"].astype(str)
    live_df["gcp_file_id"] = live_df["gcp_file_id"].astype(str)
    live_df["in_sheet"] = True

    # Drive presence
    drive_df = list_pdfs_in_folder(drive_client, config["PDF_LIVE"])
    drive_df["gcp_file_id"] = drive_df["ID"].astype(str)
    drive_df["in_drive"] = True
    drive_df.rename(columns={"Name": "file_name"}, inplace=True)

    # Qdrant summaries and file-id mapping
    collection = rag_config("qdrant_collection_name")
    pdf_ids: List[str] = live_df["pdf_id"].dropna().tolist()
    qdrant_summary = get_summaries_by_pdf_id(qdrant_client, collection, pdf_ids)
    qdrant_summary.rename(columns={"pdf_file_name": "file_name"}, inplace=True)
    if not qdrant_summary.empty:
        qdrant_summary["in_qdrant"] = True
    qdrant_files = get_file_ids_by_pdf_id(qdrant_client, collection, pdf_ids)

    # Merge data
    status_df = live_df.merge(
        drive_df[["gcp_file_id", "in_drive"]], on="gcp_file_id", how="left"
    )
    status_df = status_df.merge(
        qdrant_summary[["pdf_id", "file_name", "in_qdrant", "record_count", "page_count"]],
        on="pdf_id", how="left"
    )
    status_df = status_df.merge(qdrant_files, on="pdf_id", how="left")

    status_df["in_drive"] = status_df["in_drive"].fillna(False)
    status_df["in_qdrant"] = status_df["in_qdrant"].fillna(False)

    def file_ids_match(row) -> bool | None:
        if not row["in_qdrant"]:
            return None
        ids = row.get("gcp_file_ids")
        if isinstance(ids, list):
            return row["gcp_file_id"] in ids
        return False

    status_df["file_ids_match"] = status_df.apply(file_ids_match, axis=1)

    def collect_issues(row):
        issues: List[str] = []
        if not row["in_drive"]:
            issues.append("Missing in Drive")
        if not row["in_qdrant"]:
            issues.append("Missing in Qdrant")
        if row["file_ids_match"] is False:
            issues.append("File ID mismatch")
        return issues

    status_df["issues"] = status_df.apply(collect_issues, axis=1)

    return status_df
