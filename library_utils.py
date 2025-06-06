import os
import logging
import uuid
from datetime import datetime, timezone
import pandas as pd
from pypdf import PdfReader
from gcp_utils import fetch_sheet, get_as_dataframe


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


def pdf_to_Docs_via_pypdf(pdf_bytes_io, pdf_id=None):
    """
    Extract text documents from an in-memory PDF using pypdf.

    Args:
        pdf_bytes_io (BytesIO): In-memory bytes buffer containing the PDF data.
        pdf_id (str, optional): An identifier for the PDF.

    Returns:
        list of dict: List of documents extracted from the PDF, each with text and metadata.
    """
    try:
        logging.getLogger("pypdf").setLevel(logging.ERROR)
        pdf_bytes_io.seek(0)
        reader = PdfReader(pdf_bytes_io)
        docs = []
        for i, page in enumerate(reader.pages):
            text = page.extract_text() or ""
            doc = {
                "page_number": i + 1,
                "text": text,
                "pdf_id": pdf_id
            }
            docs.append(doc)
        return docs
    except Exception as e:
        logging.warning(f"Error extracting documents from PDF: {e}")
        return []



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

        # Confirm no duplicate publication_numbers
        publication_number = pdf_metadata.get('publication_number', '')
        if publication_number and metadata_df[metadata_df['publication_number'] == publication_number].shape[0] > 1:
            raise ValueError(f"Found duplicates for publication_number: '{publication_number}'")

        # Confirm no existing upsert date
        if pd.notna(pdf_metadata.get('upsert_date')):
            raise ValueError(f"Existing upsert_date found for pdf_id: {pdf_id}")

        # Set the upsert date
        pdf_metadata['upsert_date'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        logging.info(f"Set upsert_date to {pdf_metadata['upsert_date']} for pdf_id: {pdf_id}")

        # Format datetime fields correctly
        for col in ['upsert_date', 'effective_date', 'expiration_date']:
            if col in pdf_metadata.index:
                try:
                    parsed = pd.to_datetime(pdf_metadata[col], errors='raise', utc=True)
                    pdf_metadata[col] = parsed.strftime('%Y-%m-%dT%H:%M:%SZ')
                except Exception as e:
                    raise ValueError(f"Invalid date format in '{col}' for pdf_id '{pdf_id}': {e}")

        # Replace NaN with empty strings
        pdf_metadata = pdf_metadata.fillna('')

        # Cast everything to string (except booleans)
        document_metadata = {
            key: value if isinstance(value, bool) else str(value) if value is not None else ''
            for key, value in pdf_metadata.to_dict().items()
        }

        # Confirm required fields
        required_fields = ['title', 'scope', 'aux_specific', 'public_release', 'pdf_file_name', 'embedding']
        for field in required_fields:
            if field not in pdf_metadata or pd.isna(pdf_metadata[field]) or pdf_metadata[field] == '':
                raise ValueError(f"Required field '{field}' is empty or missing for pdf_id '{pdf_id}'")

        return document_metadata

    except Exception as e:
        logging.error(f"Error retrieving metadata for pdf_id {pdf_id}: {e}")
        return None
    

def safe_append_rows_to_sheet(sheet_client, spreadsheet_id, new_rows_df, sheet_name="Sheet1"):
    """
    Safely append new rows to a Google Sheets tab,
    skipping duplicates based on pdf_id, and return inserted entries.

    Args:
        sheet_client: Authenticated Google Sheets API client.
        spreadsheet_id: ID of the target spreadsheet.
        new_rows_df: Pandas DataFrame containing new rows to append.
        sheet_name: Name of the tab (default = "Sheet1").

    Returns:
        list of dicts: Each dict has 'pdf_id' and 'pdf_file_name' of appended rows.
    """
    try:
        if new_rows_df.empty:
            logging.warning("No rows to append — DataFrame is empty.")
            return []
        
        worksheet = sheet_client.open_by_key(spreadsheet_id).worksheet(sheet_name)

        # Fetch existing rows
        existing_rows = worksheet.get_all_values()
        headers = existing_rows[0]
        existing_pdf_ids = {
            row[headers.index("pdf_id")].strip().lower()
            for row in existing_rows[1:]
            if len(row) > headers.index("pdf_id")
        }

        # Step 2: Filter new rows to exclude duplicates
        safe_rows = []
        safe_entries = []
        for _, row in new_rows_df.iterrows():
            pdf_id = str(row.get("pdf_id", "")).strip().lower()
            pdf_file_name = row.get("pdf_file_name", "")
            if pdf_id not in existing_pdf_ids:
                row_ordered = {col: row.get(col, "") for col in headers}
                safe_rows.append([row_ordered[col] for col in headers])
                safe_entries.append(row_ordered)
            else:
                logging.warning(f"Duplicate detected, skipping pdf_id: {pdf_id}")

        if safe_rows:
            worksheet.append_rows(safe_rows, value_input_option="USER_ENTERED")
            logging.info(f"Appended {len(safe_rows)} new unique rows to {sheet_name}")
        else:
            logging.info("No unique rows to append after duplicate check.")

        return safe_entries

    except Exception as e:
        logging.error(f"Error appending rows to Google Sheet: {e}")
        return []



def validate_core_metadata(df):
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

    if missing_columns:
        raise ValueError(
            f"The catalog is missing required columns: {missing_columns}. Please fix the sheet structure before continuing."
        )


    logging.info("Catalog structure validated successfully.")



def validate_rows(sheets_client):
    """
    Validates all rows in the spreadsheet regardless of status.

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
    spreadsheet_id = os.environ["LIBRARY_UNIFIED"]

    try:
        sheet = fetch_sheet(sheets_client, spreadsheet_id)
        if sheet is None:
            logging.error(f"[validate_rows] Sheet not found for ID: {spreadsheet_id}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        df = get_as_dataframe(sheet, evaluate_formulas=True, dtype=str)
        df = df.fillna('')
        logging.debug(f"[get_as_dataframe] Column dtypes: {df.dtypes.to_dict()}")
    except Exception as e:
        logging.error(f"[get_as_dataframe] Failed to convert worksheet to DataFrame: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    headers = df.columns.tolist()
    ignored_fields = {"publication_number", "organization", "unit", "upsert_date", "status", "status_timestamp"}
    required_fields = [col for col in headers if col not in ignored_fields]
    date_fields = {"issue_date", "upsert_date", "expiration_date", "status_timestamp"}
    bool_fields = {"aux_specific", "public_release"}

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
        ws = sheets_client.open_by_key(spreadsheet_id).worksheet(sheet_name)

        # Sort in reverse to prevent index shifting
        for row_index in sorted(row_indices, reverse=True):
            gsheet_row = row_index + 2  # account for 1-indexing + header row
            ws.delete_rows(gsheet_row)
            logging.info(f"Deleted row {row_index} (sheet row {gsheet_row}) from {sheet_name}")

    except Exception as e:
        logging.error(f"Failed to delete rows from {sheet_name}: {e}")

