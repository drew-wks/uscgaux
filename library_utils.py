import logging
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, List, Union
import pandas as pd
from pypdf import PdfReader
from gcp_utils import fetch_sheet_as_df  


def compute_pdf_id(pdf_bytes_io):
    """
    Compute a unique identifier for a PDF based on its full text content.

    Args:
        pdf_bytes_io (BytesIO): In-memory bytes buffer containing the PDF data.

    Returns:
        str: A UUID string representing the PDF content.
    """
    try:
        logging.getLogger("pypdf").setLevel(logging.ERROR)
        pdf_bytes_io.seek(0)
        reader = PdfReader(pdf_bytes_io)
        full_text = ""
        for page in reader.pages:
            full_text += page.extract_text() or ""
        pdf_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, full_text))
        return pdf_uuid
    except Exception as e:
        logging.warning(f"Error computing PDF ID: {e}")
        return None


def validate_core_metadata_format(df):
    """
    Validates that the catalog DataFrame contains required columns.

    Args:
        df (pd.DataFrame): The catalog DataFrame to validate.

    Raises:
        ValueError: If any required columns are missing.
    """
    required_columns = {"pdf_id", "pdf_file_name", "google_id", "link"}
    existing_columns = set(df.columns.str.strip().str.lower())
    missing_columns = required_columns - existing_columns

    return missing_columns


def validate_all_rows_format(df):
    """
    Validates all rows in the dataframe regardless of status.

    - Required fields (except ignored) must be non-empty
    - All fields must be strings, except for aux_specific and public_release
    - aux_specific and public_release must be valid booleans
    - Date fields must be ISO 8601 UTC strings if present
    - If row is valid, logs the validation event only (no data is modified)

    Returns:
        valid_df (DataFrame): rows that passed all validation checks
        invalid_df (DataFrame): rows that failed one or more validation checks, with an 'issues' column
        log_df (DataFrame): DataFrame of log entries for valid and invalid cases
    """
    

    headers = df.columns.tolist()
    optional_fields = {"publication_number", "organization", "unit", "upsert_date", "status", "status_timestamp"}
    required_fields = [col for col in headers if col not in optional_fields]
    date_fields = {"issue_date", "upsert_date", "expiration_date", "status_timestamp"}
    bool_fields = {"aux_specific", "public_release"}

    # Fill NaNs in optional fields
    for field in optional_fields:
        if field in df.columns:
            df[field] = df[field].fillna("")
            
            
    valid_rows = []
    invalid_rows = []
    log_entries = []

    for idx, row in df.iterrows():
        pdf_id = row.get("pdf_id")
        row_valid = True
        issues = []

        if not pdf_id:
            issues.append("missing_pdf_id")
            row_valid = False

        missing_fields = [f for f in required_fields if not str(row.get(f, "")).strip()]
        if missing_fields:
            issues.append(f"missing_required_fields: {missing_fields}")
            row_valid = False

        scope = str(row.get("scope", "")).strip()
        unit = str(row.get("unit", "")).strip()
        if scope and scope.lower() != "national" and not unit:
            issues.append(f"missing_unit_for_scope: scope='{scope}' requires a unit")
            row_valid = False

        non_string_fields = [
            f for f in headers
            if f not in bool_fields and not isinstance(row.get(f), str)
        ]
        if non_string_fields:
            issues.append(f"non_string_fields: {non_string_fields}")
            row_valid = False

        bad_bools = [
            f for f in bool_fields
            if str(row.get(f)).strip().lower() not in {"true", "false", "1", "0"}
        ]
        if bad_bools:
            issues.append(f"invalid_boolean_fields: {bad_bools}")
            row_valid = False

        bad_dates = []
        for f in date_fields:
            val = str(row.get(f, "")).strip()
            if val:
                try:
                    datetime.strptime(val, "%Y-%m-%dT%H:%M:%SZ")
                except ValueError:
                    bad_dates.append(f"{f}='{val}'")
        if bad_dates:
            issues.append(f"invalid_date_format: {bad_dates}")
            row_valid = False

        if row_valid:
            valid_rows.append(row)
        else:
            row_with_issues = row.copy()
            row_with_issues["issues"] = "; ".join(issues)
            invalid_rows.append(row_with_issues)

            log_entries.append({
                "action": "validation_failed",
                "pdf_id": pdf_id,
                "pdf_file_name": row.get("pdf_file_name"),
                "row_index": idx + 2,
                "issues": row_with_issues["issues"]
            })

    valid_df = pd.DataFrame(valid_rows)
    invalid_df = pd.DataFrame(invalid_rows)
    log_df = pd.DataFrame(log_entries)

    logging.info(f"✅ Found {len(valid_df)} valid rows in LIBRARY_UNIFIED.")
    logging.info(f"⚠️ Found {len(invalid_df)} invalid rows in LIBRARY_UNIFIED.")

    if not invalid_df.empty and "issues" in invalid_df.columns:
        cols = ["issues"] + [col for col in invalid_df.columns if col != "issues"]
        invalid_df = invalid_df[cols]

    return valid_df, invalid_df, log_df



def find_duplicates_against_reference(
    df_to_check: pd.DataFrame,
    reference_df: Optional[pd.DataFrame] = None,
    fields_to_check: Optional[Union[Dict[str, str], List[Dict[str, str]]]] = None
) -> pd.DataFrame:
    """
    Finds rows in df_to_check that are duplicates against reference_df or specific field-value criteria.

    Args:
        df_to_check (pd.DataFrame): DataFrame containing rows to evaluate.
        reference_df (pd.DataFrame, optional): The DataFrame to compare against. If None, defaults to df_to_check.
        fields_to_check (dict or list of dicts, optional): One or more {column: value} dictionaries to look for in the reference_df.

    Returns:
        pd.DataFrame: A DataFrame of duplicate rows matched against the reference or criteria. Empty if no duplicates.

    Behavior:
        - If `fields_to_check` is provided, it filters `reference_df` for matching field-value pairs.
        - If `fields_to_check` is not provided, it performs a row-level duplicate check between df_to_check and reference_df.
        - Skips any blank or None values in the key-value criteria.
        - Logs warnings for any matches found.

    Usage Examples:
    ----------------
    1. Match by a single key:
        >>> find_duplicates_against_reference(
        ...     df_to_check=new_df,
        ...     reference_df=library_df,
        ...     fields_to_check={"pdf_id": "abc123"}
        ... )

    2. Match by multiple keys:
        >>> find_duplicates_against_reference(
        ...     df_to_check=new_df,
        ...     reference_df=library_df,
        ...     fields_to_check=[{"pdf_id": "abc123"}, {"google_id": "1A2B3C4D"}]
        ... )

    3. Full row-level comparison:
        >>> find_duplicates_against_reference(
        ...     df_to_check=new_df,
        ...     reference_df=library_df
        ... )

    4. Self-duplicate check:
        >>> find_duplicates_against_reference(
        ...     df_to_check=library_df
        ... )

    Notes:
        - The return value is suitable for inspection, logging, or filtering.
        - No exceptions are raised; all findings are returned as a DataFrame and optionally logged.

    """
    if reference_df is None:
        reference_df = df_to_check

    if fields_to_check:
        # Normalize to list of dicts
        if isinstance(fields_to_check, dict):
            fields_to_check = [fields_to_check]

        matches = []
        for criteria in fields_to_check:
            filtered_ref = reference_df.copy()
            for field, value in criteria.items():
                if not value or str(value).strip() == "":
                    continue
                if field not in reference_df.columns:
                    logging.warning(f"Field '{field}' not found in reference_df. Skipping.")
                    continue
                filtered_ref = filtered_ref[filtered_ref[field].astype(str) == str(value).strip()]
            if not filtered_ref.empty:
                matches.append(filtered_ref)

        if matches:
            result = pd.concat(matches).drop_duplicates()
            logging.warning(f"⚠️ {len(result)} duplicates found based on provided field(s).")
            return result
        logging.info("✅ No duplicates found based on provided field(s).")
        return pd.DataFrame()

    # Default behavior: detect exact row duplicates between the two DataFrames
    common_columns = list(set(df_to_check.columns) & set(reference_df.columns))
    merged = df_to_check.merge(reference_df, how="inner", on=common_columns)
    if not merged.empty:
        logging.warning(f"⚠️ {len(merged)} duplicate row(s) found in reference.")
    else:
        logging.info("✅ No duplicate rows found in reference.")
    return merged



def get_planned_metadata_for_single_record(metadata_df, pdf_id):
    """
    Given a DataFrame of metadata and a pdf_id,
    returns a dictionary of strings representing the document's metadata.
    """
    try:
        # Find the metadata row that corresponds to this pdf_id
        pdf_metadata = metadata_df[metadata_df['pdf_id'].str.strip().astype(str).str.lower() == pdf_id.lower()]

        if pdf_metadata.empty:
            raise ValueError(f"No metadata found for pdf_id: {pdf_id}")

        # Confirm no duplicate pdf_ids
        if len(pdf_metadata) > 1:
            raise ValueError(f"Found duplicates for pdf_id '{pdf_id}' in metadata sheet.")

        pdf_metadata = pdf_metadata.iloc[0].copy()
        logging.info(f"Successfully accessed metadata for pdf_id: {pdf_id}")


        # Confirm no existing upsert date
        if pd.notna(pdf_metadata.get('upsert_date')):
            raise ValueError(f"Existing upsert_date found for pdf_id: {pdf_id}")

        # Set the upsert date
        pdf_metadata['upsert_date'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        logging.info(f"Set upsert_date to {pdf_metadata['upsert_date']} for pdf_id: {pdf_id}")


        # Cast everything to string (except booleans)
        document_metadata = {
            key: value if isinstance(value, bool) else str(value) if value is not None else ''
            for key, value in pdf_metadata.to_dict().items()
        }

        return document_metadata

    except Exception as e:
        logging.error(f"Error retrieving metadata for pdf_id {pdf_id}: {e}")
        return None



def fetch_rows_by_status(catalog_df, keywords):
    """
    Fetch rows from a catalog DataFrame where the 'status' column contains 
    any of the given keywords (case-insensitive partial match).

    Args:
        catalog_df (pd.DataFrame): The catalog DataFrame.
        keywords (str or list of str): Keyword(s) to match in the 'status' column.

    Returns:
        pd.DataFrame: Filtered rows matching any of the keywords.
    """
    if "status" not in catalog_df.columns:
        logging.warning("'status' column not found in catalog.")
        return pd.DataFrame()

    # Normalize to lowercase string
    status_series = catalog_df["status"].astype(str).str.lower()

    # Ensure keywords is a list
    if isinstance(keywords, str):
        keywords = [keywords]

    # Build combined match mask
    match_mask = status_series.apply(lambda x: any(kw.lower() in x for kw in keywords))
    matched_rows = catalog_df[match_mask]

    logging.info(f"Found {len(matched_rows)} rows matching keywords: {keywords}")
    return matched_rows



def remove_rows(sheets_client, spreadsheet_id, row_indices, sheet_name="Sheet1"):
    """
    Delete multiple rows from a specific sheet tab.

    Args:
        sheets_client: gspread client.
        spreadsheet_id: ID of the source spreadsheet.
        row_indices: List of 0-based row indices (relative to a DataFrame).
        sheet_name: Name of the worksheet/tab (default = "Sheet1").
    """
    try:
        sheet = sheets_client.open_by_key(spreadsheet_id).worksheet(sheet_name)

        # Sort in reverse to prevent index shifting
        for row_index in sorted(row_indices, reverse=True):
            row = row_index + 2  # account for 1-indexing + header row
            sheet.delete_rows(row)
            logging.info(f"Deleted row {row_index} (sheet row {row}) from {sheet_name}")

    except Exception as e:
        logging.error(f"Failed to delete rows from {sheet_name}: {e}")




def append_new_rows(sheets_client, spreadsheet_id, new_rows_df, sheet_name = "Sheet1"):
    """
    Appends new rows to a Google Sheet tab in column order, assuming no duplicates.

    Args:
        sheets_client (SheetsClient): Authenticated Google Sheets client.
        spreadsheet_id (str): ID of the Google Spreadsheet.
        new_rows_df (pd.DataFrame): Rows to append.
        sheet_name (str): Tab name in the spreadsheet (default = "Sheet1").

    Returns:
        List[Dict]: Rows that were appended, ordered to match the sheet.
    """
    try:
        if new_rows_df.empty:
            logging.warning("No rows to append — DataFrame is empty.")
            return []

        existing_df = fetch_sheet_as_df(sheets_client, spreadsheet_id)
        headers = list(existing_df.columns)
        sheet = sheets_client.open_by_key(spreadsheet_id).worksheet(sheet_name)

        rows_to_append = []
        appended_dicts = []

        for _, row in new_rows_df.iterrows():
            row_dict = {col: row.get(col, "") for col in headers}
            row_list = [row_dict[col] for col in headers]
            rows_to_append.append(row_list)
            appended_dicts.append(row_dict)

        sheet.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        logging.info(f"✅ Appended {len(rows_to_append)} new row(s) to {sheet_name}")

        return appended_dicts

    except Exception as e:
        logging.error(f"❌ Error in append_new_rows: {e}")
        return []
