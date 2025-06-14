import pandas as pd
from typing import Iterable, Tuple, List, Dict, Any

from env_config import env_config
from gcp_utils import fetch_sheet

config = env_config()


def flag_rows_as_orphans(sheet, full_df: pd.DataFrame, orphan_rows: pd.DataFrame) -> List[Dict[str, Any]]:
    """Placeholder that flags orphan rows in a sheet.

    In production this would update the Google Sheet and return log entries of the
    updates performed. The implementation here is minimal so that unit tests can
    patch it.
    """
    return []


def find_rows_missing_gcp_file_ids(
    sheets_client,
    library_df: pd.DataFrame,
    known_file_ids: Iterable[str],
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """Return rows whose GCP file IDs are missing from ``known_file_ids``.

    Parameters
    ----------
    sheets_client:
        Authenticated gspread client used to fetch the sheet.
    library_df:
        DataFrame of rows from ``LIBRARY_UNIFIED``.
    known_file_ids:
        Set of file IDs known to exist in Drive.
    """
    missing_mask = ~library_df["gcp_file_id"].isin(list(known_file_ids))
    orphans: pd.DataFrame = library_df.loc[missing_mask].copy()
    if orphans.empty:
        return orphans, []

    sheet = fetch_sheet(sheets_client, config["LIBRARY_UNIFIED"])
    logs = flag_rows_as_orphans(sheet, library_df, orphans)
    return orphans, logs


def find_files_missing_rows(
    library_df: pd.DataFrame, files_df: pd.DataFrame
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """Return Drive files that do not have matching rows in ``library_df``."""
    orphan_mask = ~files_df["ID"].isin(library_df["gcp_file_id"])
    orphans: pd.DataFrame = files_df.loc[orphan_mask].copy()
    logs: List[Dict[str, Any]] = []
    for _, row in orphans.iterrows():
        logs.append(
            {
                "action": f"orphan_file_detected_in_{row.get('folder')}",
                "pdf_id": row["ID"],
                "pdf_file_name": row["Name"],
            }
        )
    return orphans, logs
