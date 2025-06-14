import logging
from typing import List

import pandas as pd

from env_config import env_config, rag_config
from gcp_utils import fetch_sheet_as_df, list_files_in_folder
from qdrant_utils import (
    get_summaries_by_pdf_id,
    get_gcp_file_ids_by_pdf_id,
    get_all_pdf_ids_in_qdrant,
)


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

    # Flag duplicates and empty IDs while values are still raw
    live_df["empty_pdf_id_in_sheet"] = live_df["pdf_id"].isna() | (live_df["pdf_id"].astype(str).str.strip() == "")
    live_df["empty_gcp_file_id_in_sheet"] = live_df["gcp_file_id"].isna() | (
        live_df["gcp_file_id"].astype(str).str.strip() == ""
    )

    live_df["pdf_id"] = live_df["pdf_id"].astype(str)
    live_df["gcp_file_id"] = live_df["gcp_file_id"].astype(str)
    live_df["duplicate_pdf_id_in_sheet"] = live_df["pdf_id"].duplicated(keep=False)
    live_df["in_sheet"] = True

    # Drive presence
    drive_df = list_files_in_folder(drive_client, config["PDF_LIVE"])
    drive_df["gcp_file_id"] = drive_df["ID"].astype(str)
    drive_df["in_drive"] = True
    drive_df.rename(columns={"Name": "file_name"}, inplace=True)
    drive_ids = set(drive_df["gcp_file_id"])

    # Qdrant summaries and file-id mapping
    collection = rag_config("qdrant_collection_name")
    pdf_ids: List[str] = live_df["pdf_id"].dropna().tolist()
    qdrant_summary = get_summaries_by_pdf_id(qdrant_client, collection, pdf_ids)
    qdrant_summary.rename(columns={"pdf_file_name": "file_name"}, inplace=True)
    if not qdrant_summary.empty:
        qdrant_summary["in_qdrant"] = True
    qdrant_files = get_gcp_file_ids_by_pdf_id(qdrant_client, collection, pdf_ids)

    # Orphan pdf_ids present only in Qdrant
    all_pdf_ids = set(get_all_pdf_ids_in_qdrant(qdrant_client, collection))
    orphan_pdf_ids = sorted(all_pdf_ids - set(live_df["pdf_id"]))
    if orphan_pdf_ids:
        orphan_summary = get_summaries_by_pdf_id(qdrant_client, collection, orphan_pdf_ids)
        orphan_summary.rename(columns={"pdf_file_name": "file_name"}, inplace=True)
        if not orphan_summary.empty:
            orphan_summary["in_qdrant"] = True
            qdrant_summary = pd.concat([qdrant_summary, orphan_summary], ignore_index=True)
        orphan_files = get_gcp_file_ids_by_pdf_id(qdrant_client, collection, orphan_pdf_ids)
        if not orphan_files.empty:
            qdrant_files = pd.concat([qdrant_files, orphan_files], ignore_index=True)

    # Merge data
    status_df = live_df.merge(
        drive_df[["gcp_file_id", "in_drive"]], on="gcp_file_id", how="left"
    )
    status_df = status_df.merge(
        qdrant_summary[["pdf_id", "file_name", "in_qdrant", "record_count", "page_count"]],
        on="pdf_id", how="left"
    )
    status_df = status_df.merge(qdrant_files, on="pdf_id", how="left")

    status_df["in_drive"] = status_df["in_drive"].fillna(False).astype("bool")
    status_df["in_qdrant"] = status_df["in_qdrant"].fillna(False).astype("bool")
    status_df["record_count"] = status_df["record_count"].fillna(0).astype(int)

    status_df["zero_record_count"] = status_df["in_qdrant"] & (status_df["record_count"] == 0)

    # Flag rows where Qdrant records exist but have no associated gcp_file_id
    status_df["unique_file_count"] = status_df["unique_file_count"].fillna(0).astype(int)
    status_df["empty_gcp_file_id_in_qdrant"] = status_df["in_qdrant"] & (status_df["unique_file_count"] == 0)
    status_df["missing_gcp_file_id"] = status_df["empty_gcp_file_id_in_sheet"] | status_df["empty_gcp_file_id_in_qdrant"]

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
        if row.get("duplicate_pdf_id_in_sheet"):
            issues.append("Duplicate pdf_id in Sheet")
        if row.get("empty_pdf_id_in_sheet"):
            issues.append("Empty pdf_id in Sheet")
        if row.get("empty_gcp_file_id_in_sheet"):
            issues.append("Empty gcp_file_id in Sheet")
        if row.get("empty_gcp_file_id_in_qdrant"):
            issues.append("Empty gcp_file_id in Qdrant")
        if row.get("zero_record_count"):
            issues.append("No Qdrant records")
        if not row["in_drive"]:
            issues.append("Missing in Drive")
        if not row["in_qdrant"]:
            issues.append("Missing in Qdrant")
        if row["file_ids_match"] is False:
            issues.append("Qdrant record missing expected gcp_file_id")
        return issues

    status_df["issues"] = status_df.apply(collect_issues, axis=1)

    desired_columns = [
        "title",
        "pdf_file_name",
        "pdf_id",
        "gcp_file_id",
        "issues",
        "empty_pdf_id_in_sheet",
        "empty_gcp_file_id_any_source",
        "empty_gcp_file_id_in_sheet",
        "empty_gcp_file_id_in_qdrant",
        "duplicate_pdf_id_in_sheet",
        "in_sheet",
        "in_drive",
        "file_name",
        "in_qdrant",
        "record_count",
        "page_count",
        "gcp_file_ids",
        "unique_file_count",
        "zero_record_count",
        "file_ids_match",
    ]
    status_df = status_df.reindex(columns=[c for c in desired_columns if c in status_df.columns])
    return status_df
