import logging
from typing import List

import pandas as pd

from env_config import env_config, rag_config
from gcp_utils import fetch_sheet_as_df, list_pdfs_in_folder
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

    # Flag duplicates and missing IDs while values are still raw
    live_df["missing_pdf_id"] = live_df["pdf_id"].isna() | (live_df["pdf_id"].astype(str).str.strip() == "")
    live_df["missing_gcp_file_id"] = live_df["gcp_file_id"].isna() | (
        live_df["gcp_file_id"].astype(str).str.strip() == ""
    )

    live_df["pdf_id"] = live_df["pdf_id"].astype(str)
    live_df["gcp_file_id"] = live_df["gcp_file_id"].astype(str)
    live_df["duplicate_pdf_id"] = live_df["pdf_id"].duplicated(keep=False)
    live_df["in_sheet"] = True

    # Drive presence
    drive_df = list_pdfs_in_folder(drive_client, config["PDF_LIVE"])
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

    status_df["in_drive"] = status_df["in_drive"].fillna(False)
    status_df["in_qdrant"] = status_df["in_qdrant"].fillna(False)
    status_df["record_count"] = status_df["record_count"].fillna(0)

    status_df["zero_record_count"] = status_df["in_qdrant"] & (status_df["record_count"] == 0)

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
        if row.get("duplicate_pdf_id"):
            issues.append("Duplicate pdf_id")
        if row.get("missing_pdf_id"):
            issues.append("Missing pdf_id")
        if row.get("missing_gcp_file_id"):
            issues.append("Missing gcp_file_id")
        if row.get("zero_record_count"):
            issues.append("No Qdrant records")
        if not row["in_drive"]:
            issues.append("Missing in Drive")
        if not row["in_qdrant"]:
            issues.append("Missing in Qdrant")
        if row["file_ids_match"] is False:
            issues.append("File ID mismatch")
        return issues

    status_df["issues"] = status_df.apply(collect_issues, axis=1)

    # --- Orphan checks ---
    existing_pairs = set(zip(status_df["pdf_id"], status_df["gcp_file_id"]))

    orphan_rows = []
    for _, row in qdrant_files.iterrows():
        pid = str(row.get("pdf_id"))
        ids = row.get("gcp_file_ids") or []
        for fid in ids:
            pair = (pid, str(fid))
            if pair not in existing_pairs:
                summary_row = qdrant_summary[qdrant_summary["pdf_id"] == pid]
                rec_count = int(summary_row["record_count"].iloc[0]) if not summary_row.empty else 0
                page_count = int(summary_row["page_count"].iloc[0]) if not summary_row.empty else None
                file_name = summary_row["file_name"].iloc[0] if not summary_row.empty else None
                orphan_rows.append(
                    {
                        "pdf_id": pid,
                        "gcp_file_id": str(fid),
                        "file_name": file_name,
                        "in_sheet": False,
                        "in_drive": str(fid) in drive_ids,
                        "in_qdrant": True,
                        "record_count": rec_count,
                        "page_count": page_count,
                        "gcp_file_ids": [str(fid)],
                        "unique_file_count": 1,
                        "file_ids_match": True,
                        "duplicate_pdf_id": False,
                        "missing_pdf_id": False,
                        "missing_gcp_file_id": False,
                        "zero_record_count": rec_count == 0,
                        "issues": ["Orphan Qdrant record"] + ([] if str(fid) in drive_ids else ["Missing in Drive"]),
                    }
                )

    drive_orphans = drive_df[~drive_df["gcp_file_id"].isin(status_df["gcp_file_id"])]
    for _, row in drive_orphans.iterrows():
        orphan_rows.append(
            {
                "pdf_id": None,
                "gcp_file_id": row["gcp_file_id"],
                "file_name": row.get("file_name"),
                "in_sheet": False,
                "in_drive": True,
                "in_qdrant": False,
                "record_count": None,
                "page_count": None,
                "gcp_file_ids": None,
                "unique_file_count": None,
                "file_ids_match": None,
                "duplicate_pdf_id": False,
                "missing_pdf_id": False,
                "missing_gcp_file_id": False,
                "zero_record_count": None,
                "issues": ["Orphan file"],
            }
        )

    if orphan_rows:
        status_df = pd.concat([status_df, pd.DataFrame(orphan_rows)], ignore_index=True)

    return status_df

