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
=======
def get_file_ids_by_pdf_id(client, collection_name, pdf_ids):
    """Return DataFrame of pdf_id to gcp_file_id mapping from Qdrant."""
    if not pdf_ids:
        return pd.DataFrame(columns=["pdf_id", "gcp_file_id"])
    mapping = {}
    scroll_offset = None
    while True:
        results, scroll_offset = client.scroll(
            collection_name=collection_name,
            scroll_filter=models.Filter(
                must=[models.FieldCondition(
                    key="metadata.pdf_id",
                    match=models.MatchAny(any=pdf_ids)
                )]
            ),
            with_payload=True,
            with_vectors=False,
            limit=100000,
            offset=scroll_offset
        )
        for record in results:
            payload = record.payload or {}
            metadata = payload.get("metadata", {})
            pdf_id = metadata.get("pdf_id")
            file_id = metadata.get("gcp_file_id")
            if pdf_id and file_id and pdf_id not in mapping:
                mapping[str(pdf_id)] = str(file_id)
        if scroll_offset is None:
            break
    return pd.DataFrame({"pdf_id": list(mapping.keys()), "gcp_file_id": list(mapping.values())})


def build_status_map(drive_client, sheets_client, qdrant_client):
    """Return a consolidated status DataFrame across Sheet, Drive and Qdrant."""
    library_df = fetch_sheet_as_df(sheets_client, config["LIBRARY_UNIFIED"])
    sheet_df = pd.DataFrame(columns=["pdf_id", "sheet_file_id"])
    if not library_df.empty and "pdf_id" in library_df.columns:
        sheet_df = library_df[["pdf_id"]].copy()
        if "gcp_file_id" in library_df.columns:
            sheet_df["sheet_file_id"] = library_df["gcp_file_id"].astype(str)
        else:
            sheet_df["sheet_file_id"] = ""
        sheet_df["in_sheet"] = True
    else:
        sheet_df["in_sheet"] = False

    live_df = list_pdfs_in_folder(drive_client, config["PDF_LIVE"])
    tag_df = list_pdfs_in_folder(drive_client, config["PDF_TAGGING"])
    drive_df = pd.concat([live_df, tag_df], ignore_index=True)
    drive_df = drive_df.rename(columns={"ID": "gcp_file_id"})
    drive_ids = set(drive_df["gcp_file_id"].astype(str))

    collection = rag_config("qdrant_collection_name")
    pdf_ids = get_file_ids_by_pdf_id(qdrant_client, collection, [])
    qdrant_pdf_ids = pdf_ids["pdf_id"].tolist()
    qdrant_file_df = pdf_ids.rename(columns={"gcp_file_id": "qdrant_file_id"})
    qdrant_file_df["in_qdrant"] = True
    qdrant_summary_df = get_summaries_by_pdf_id(qdrant_client, collection, qdrant_pdf_ids)
    qdrant_df = qdrant_file_df.merge(qdrant_summary_df, on="pdf_id", how="left")

    all_ids = sorted(set(sheet_df["pdf_id"].astype(str)) | set(qdrant_df["pdf_id"].astype(str)))
    result = pd.DataFrame({"pdf_id": all_ids})
    result = result.merge(sheet_df[["pdf_id", "sheet_file_id", "in_sheet"]], on="pdf_id", how="left")
    result = result.merge(qdrant_df, on="pdf_id", how="left")
    result["in_sheet"] = result["in_sheet"].fillna(False)
    result["in_qdrant"] = result["in_qdrant"].fillna(False)

    result["in_drive"] = result.apply(
        lambda r: (str(r.get("sheet_file_id")) in drive_ids) or (str(r.get("qdrant_file_id")) in drive_ids),
        axis=1,
    )
    result["file_ids_match"] = (
        result["sheet_file_id"].notna()
        & result["qdrant_file_id"].notna()
        & (result["sheet_file_id"] == result["qdrant_file_id"])
    )

    def flag(row):
        issues = []
        if not row["in_sheet"]:
            issues.append("missing_sheet")
        if not row["in_qdrant"]:
            issues.append("missing_qdrant")
        if not row["in_drive"]:
            issues.append("missing_drive")
        if row["in_sheet"] and row["in_qdrant"] and not row["file_ids_match"]:
            issues.append("file_id_mismatch")
        return "; ".join(issues)

    result["issues"] = result.apply(flag, axis=1)
    return result
