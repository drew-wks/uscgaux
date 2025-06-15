import pandas as pd
from typing import Tuple, List
import logging
from gspread.client import Client as SheetsClient
from googleapiclient.discovery import Resource as DriveClient
from qdrant_client import QdrantClient

from env_config import env_config, rag_config
from utils.gcp_utils import fetch_sheet_as_df, list_files_in_folder, get_folder_name, file_exists
from utils.library_utils import fetch_rows_by_status, remove_rows, 
from utils.log_writer import log_event
from utils.qdrant_utils import (
    get_summaries_by_pdf_id,
    get_gcp_file_ids_by_pdf_id,
    get_all_pdf_ids_in_qdrant,
    delete_records_by_pdf_id, 
    in_qdrant
)



config = env_config()


def build_status_map(drive_client, sheets_client, qdrant_client) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build a comprehensive status map of PDF records across Google Sheets (LIBRARY_UNIFIED), 
    Google Drive (PDF_LIVE), and Qdrant (vector database), identifying inconsistencies 
    and missing data across systems.

    This function inspects the `live` entries in the LIBRARY_UNIFIED spreadsheet 
    and evaluates the state of each document across three dimensions:
    
    - Presence in the Google Sheet (`in_sheet`)
    - Presence in the Google Drive live folder (`in_drive`)
    - Presence in the Qdrant collection (`in_qdrant`)
    
    It also flags issues such as:
        - Missing or empty `pdf_id` or `gcp_file_id` in Sheet or Qdrant
        - Duplicate `pdf_id` entries in the Sheet
        - Qdrant records with zero documents
        - Mismatches between expected and actual `gcp_file_id` values in Qdrant
        - Qdrant records with no associated file ID at all

    Returns two DataFrames:
        1. `status_df`: A full overview with enriched metadata and computed diagnostic fields.
        2. `issues_only`: A filtered subset of rows with one or more identified issues.

    Columns in `status_df` include:
        - `pdf_id`, `gcp_file_id`, `pdf_file_name`, `title`
        - `in_sheet`, `in_drive`, `in_qdrant`
        - `record_count`, `page_count`
        - `gcp_file_ids`, `unique_file_count`
        - `file_ids_match`: whether `gcp_file_id` in Sheet matches any listed in Qdrant
        - Diagnostic flags:
            - `empty_pdf_id_in_sheet`, `empty_gcp_file_id_in_sheet`
            - `empty_gcp_file_id_in_qdrant`, `empty_gcp_file_id_any_source`
            - `duplicate_pdf_id_in_sheet`, `zero_record_count`
        - `issues`: A list of human-readable issue descriptions per row

    Args:
        drive_client (DriveClient): Authenticated Google Drive API client.
        sheets_client (SheetsClient): Authenticated Google Sheets API client.
        qdrant_client (QdrantClient): Initialized Qdrant client connected to the target collection.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing:
            - status_df: Complete status map for all live documents
            - issues_only: Subset of `status_df` containing rows with one or more issues
    """
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
    qdrant_summary = qdrant_summary.copy()
    qdrant_summary.rename(columns={"pdf_file_name": "file_name"}, inplace=True)
    if not qdrant_summary.empty:
        qdrant_summary["in_qdrant"] = True
    qdrant_files = get_gcp_file_ids_by_pdf_id(qdrant_client, collection, pdf_ids)

    # Identify drive files not referenced in Sheet or Qdrant
    qdrant_file_ids: set[str] = set()
    if not qdrant_files.empty and "gcp_file_ids" in qdrant_files.columns:
        qdrant_file_ids = set(
            qdrant_files["gcp_file_ids"].explode().dropna().astype(str).tolist()
        )
    sheet_file_ids = set(live_df["gcp_file_id"])
    orphan_drive_ids = drive_ids - sheet_file_ids - qdrant_file_ids
    orphan_drive_df = drive_df[drive_df["gcp_file_id"].isin(orphan_drive_ids)].copy()

    # Orphan pdf_ids present only in Qdrant
    all_pdf_ids = set(get_all_pdf_ids_in_qdrant(qdrant_client, collection))
    orphan_pdf_ids = sorted(all_pdf_ids - set(live_df["pdf_id"]))
    if orphan_pdf_ids:
        orphan_summary = get_summaries_by_pdf_id(qdrant_client, collection, orphan_pdf_ids)
        orphan_summary = orphan_summary.copy()
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

    if not orphan_drive_df.empty:
        orphan_drive_df = orphan_drive_df.assign(
            pdf_id=pd.NA,
            in_sheet=False,
            in_qdrant=False,
            pdf_file_name=orphan_drive_df["file_name"],
        )
        for col in status_df.columns:
            if col not in orphan_drive_df.columns:
                orphan_drive_df[col] = pd.NA
        status_df = pd.concat([status_df, orphan_drive_df[status_df.columns]], ignore_index=True)

    bool_cols = [
        "in_sheet",
        "empty_pdf_id_in_sheet",
        "empty_gcp_file_id_in_sheet",
        "duplicate_pdf_id_in_sheet",
    ]
    for col in bool_cols:
        if col in status_df.columns:
            status_df[col] = status_df[col].astype("boolean").fillna(False)

    status_df["in_drive"] = status_df["in_drive"].astype("boolean").fillna(False)
    status_df["in_qdrant"] = status_df["in_qdrant"].astype("boolean").fillna(False)
    status_df["record_count"] = status_df["record_count"].astype("Int64").fillna(0)

    status_df["zero_record_count"] = status_df["in_qdrant"] & (status_df["record_count"] == 0)

    # Flag rows where Qdrant records exist but have no associated gcp_file_id
    status_df["unique_file_count"] = (
        status_df["unique_file_count"].astype("Int64").fillna(0)
    )
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
        if row.get("duplicate_pdf_id_in_sheet") is True:
            issues.append("Duplicate pdf_id in Sheet")
        if row.get("empty_pdf_id_in_sheet") is True:
            issues.append("Empty pdf_id in Sheet")
        if row.get("empty_gcp_file_id_in_sheet") is True:
            issues.append("Empty gcp_file_id in Sheet")
        if row.get("empty_gcp_file_id_in_qdrant") is True:
            issues.append("Empty gcp_file_id in Qdrant")
        if row.get("zero_record_count") is True:
            issues.append("No Qdrant records")
        if not row["in_drive"]:
            issues.append("Missing in Drive")
        if not row["in_qdrant"]:
            issues.append("Missing in Qdrant")
        if row["file_ids_match"] is False:
            issues.append("Qdrant record missing expected gcp_file_id")
        if row["in_drive"] and not row["in_sheet"] and not row["in_qdrant"]:
            issues.append("Orphan in Drive")
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
    
    issues_only = status_df[
        status_df["issues"].apply(lambda x: isinstance(x, list) and len(x) > 0)
    ]
    return status_df, issues_only




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


def delete_tagged(
    drive_client: DriveClient,
    sheets_client: SheetsClient,
    qdrant_client: QdrantClient,
) -> pd.DataFrame:
    """Delete rows and PDFs marked for deletion from all systems."""
    target_statuses: List[str] = ["deletion"]

    library_df = fetch_sheet_as_df(sheets_client, config["LIBRARY_UNIFIED"])
    rows_to_delete = fetch_rows_by_status(library_df, target_statuses)

    if rows_to_delete.empty:
        logging.info("No rows marked for deletion. No further action taken.")
        return pd.DataFrame()

    duplicates_df = library_df[library_df.duplicated(subset="pdf_id", keep=False)]
    if not duplicates_df.empty:
        logging.error(
            "❌ Rows with duplicate pdf_ids found. Handle/remove duplicates before attempting deletion. No further action taken.%s",
            duplicates_df,
        )
        return pd.DataFrame()

    deleted_rows = []

    for _, row in rows_to_delete.iterrows():
        file_id = str(row.get("gcp_file_id", ""))
        pdf_id = str(row.get("pdf_id", ""))
        filename = str(row.get("pdf_file_name", "unknown_file"))
        original_status = str(row.get("status", "unknown_status"))

        folder_name = "unknown_folder"
        if file_exists(drive_client, file_id):
            folder_name = get_folder_name(drive_client, file_id)
            try:
                drive_client.files().delete(fileId=file_id).execute()
                log_event(
                    sheets_client,
                    f"file_deleted from {folder_name}",
                    str(pdf_id),
                    str(filename),
                    extra_columns=[original_status],
                )
            except Exception as e:  # pragma: no cover - log only
                logging.warning("Failed to delete file %s (ID: %s): %s", filename, file_id, e)
        else:
            logging.info("File ID %s not found in Drive. Skipping deletion.", file_id)

        collection_name = rag_config("qdrant_collection_name")
        if not original_status.startswith("new_for_deletion"):
            if in_qdrant(qdrant_client, collection_name, pdf_id):
                try:
                    delete_records_by_pdf_id(qdrant_client, [pdf_id], collection_name)
                except Exception as e:  # pragma: no cover - log only
                    logging.warning("Failed to delete Qdrant record for %s: %s", pdf_id, e)
            else:
                logging.info("PDF ID %s not found in Qdrant. Skipping deletion.", pdf_id)
        else:
            logging.info("Skipping Qdrant deletion for new record: %s", pdf_id)

        try:
            row_indices = library_df[library_df["pdf_id"] == pdf_id].index.tolist()
            if not row_indices:
                logging.warning("No matching row index found for pdf_id %s. Skipping row deletion.", pdf_id)
                continue

            remove_rows(sheets_client, config["LIBRARY_UNIFIED"], row_indices=row_indices)
            log_event(
                sheets_client,
                "deleted",
                str(pdf_id),
                str(filename),
                extra_columns=[original_status, folder_name],
            )
        except Exception as e:  # pragma: no cover - log only
            logging.error("Failed to remove row for %s: %s", pdf_id, e)

        deleted_rows.append(row)

    return pd.DataFrame(deleted_rows)
