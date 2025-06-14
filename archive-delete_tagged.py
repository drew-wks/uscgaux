import logging
from datetime import datetime, timezone
from typing import List

import pandas as pd
from gspread.client import Client as SheetsClient
from googleapiclient.discovery import Resource as DriveClient
from qdrant_client import QdrantClient

from env_config import rag_config, env_config
from gcp_utils import (
    move_pdf,
    fetch_sheet_as_df,
    get_folder_name,
    file_exists,
)
from qdrant_utils import delete_records_by_pdf_id, in_qdrant
from library_utils import fetch_rows_by_status, remove_rows, append_new_rows
from log_writer import log_event


config = env_config()


def archive_tagged(
    drive_client: DriveClient,
    sheets_client: SheetsClient,
    qdrant_client: QdrantClient,
) -> pd.DataFrame:
    """Archive rows and PDFs marked for archiving.

    Moves the corresponding PDF to ``PDF_ARCHIVE``, deletes Qdrant records,
    appends the row to ``LIBRARY_ARCHIVE`` and removes it from ``LIBRARY_UNIFIED``.
    """
    target_statuses: List[str] = ["live_for_archive"]

    library_df = fetch_sheet_as_df(sheets_client, config["LIBRARY_UNIFIED"])
    rows_to_archive = fetch_rows_by_status(library_df, target_statuses)
    if rows_to_archive.empty:
        logging.info("No rows marked for archive. No further action taken.")
        return pd.DataFrame()

    archived_rows = []

    for i, row in rows_to_archive.iterrows():
        pdf_id = row.get("pdf_id", "[unknown]")
        file_id = row.get("gcp_file_id")
        filename = row.get("pdf_file_name", "unknown_file.pdf")
        row_index = library_df[library_df["pdf_id"] == pdf_id].index.tolist()

        move_pdf(drive_client, file_id, config["PDF_ARCHIVE"])
        delete_records_by_pdf_id(
            qdrant_client,
            rag_config("qdrant_collection_name"),
            pdf_id,
        )

        row["timestamp_archived"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        append_new_rows(
            sheets_client,
            spreadsheet_id=config["LIBRARY_ARCHIVE"],
            new_rows_df=pd.DataFrame([row]),
            sheet_name="Sheet1",
        )

        try:
            remove_rows(
                sheets_client,
                spreadsheet_id=config["LIBRARY_UNIFIED"],
                row_indices=row_index,
            )
        except Exception as e:  # pragma: no cover - log only
            logging.error("Failed to remove row %s for %s: %s", i, pdf_id, e)

        archived_rows.append(row)
        log_event(sheets_client, "archived", str(pdf_id), str(filename))

    return pd.DataFrame(archived_rows)


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
