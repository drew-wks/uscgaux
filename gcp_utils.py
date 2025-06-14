#  Utilities for Google Drive file management

import logging
import json
from typing import Optional
from io import BytesIO
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from gspread.client import Client as SheetsClient
from gspread.worksheet import Worksheet
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build, Resource as DriveClient
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from env_config import env_config

config = env_config()


def get_gcp_credentials() -> Credentials:
    """
    Returns a Google `Credentials` object from a flattened JSON string in the environment.
    """
    creds_json = config["GCP_CREDENTIALS_FOR_STREAMLIT_USCGAUX_APP"]
    if not creds_json:
        raise EnvironmentError("Missing GCP_CREDENTIALS_FOR_STREAMLIT_USCGAUX_APP in environment.")

    try:
        creds_dict = json.loads(creds_json)
        return Credentials.from_service_account_info(creds_dict)
    except Exception as e:
        raise ValueError(f"Failed to load GCP credentials from environment: {e}") from e


def init_drive_client(creds: Credentials) -> DriveClient:
    """
    Initializes a Google Drive client using the provided credentials.

    Applies the required Drive API scopes to the provided credentials and returns
    an authorized `googleapiclient.discovery.Resource` object for interacting with
    the Drive v3 API.

    Args:
        creds (Credentials): Google service account or user credentials.

    Returns:
        DriveClient: An authorized client for the Google Drive API.
    """
    scoped_creds = creds.with_scopes(["https://www.googleapis.com/auth/drive"])
    client = build("drive", "v3", credentials=scoped_creds)
    logging.info("✅ Google Drive client initialized successfully with scoped credentials.")
    return client


def init_sheets_client(creds: Credentials) -> SheetsClient:
    """
    Initializes a Google Sheets client using the provided credentials.

    Applies the necessary scopes for accessing both Sheets and Drive APIs,
    and returns a `gspread` client authorized with those credentials.

    Args:
        creds (Credentials): Google service account or user credentials.

    Returns:
        SheetsClient: An authorized `gspread` client for accessing Google Sheets.
    """
    scoped_creds = creds.with_scopes([
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ])
    client = gspread.authorize(scoped_creds)
    logging.info("✅ Google Sheets client initialized successfully with scoped credentials.")
    return client


def get_folder_name(drive_client: DriveClient, file_id: str) -> str:
    """
    Returns the name of the folder containing the file with the given file_id.

    Args:
        drive_client: An authenticated Google Drive API client.
        file_id (str): The ID of the file.

    Returns:
        str: Name of the parent folder, or "Unknown" if not found.
    """
    try:
        file_metadata = drive_client.files().get(fileId=file_id, fields='parents').execute()
        parent_ids = file_metadata.get("parents", [])
        if not parent_ids:
            return "Unknown"

        folder_metadata = drive_client.files().get(fileId=parent_ids[0], fields='name').execute()
        return folder_metadata.get("name", "Unknown")
    except Exception as e:
        logging.warning("Failed to fetch folder name for file ID %s: %s", file_id, e)
        return "Unknown"


def is_pdf_file(file_stream: Optional[BytesIO]) -> bool:
    """
    Check if the given file stream is a valid PDF based on its header.

    Args:
        file_stream (BytesIO): A file-like object containing the file's binary content.

    Returns:
        bool: True if the file is a PDF (starts with %PDF), False otherwise.
    """
    if not file_stream:
        return False

    try:
        file_stream.seek(0)
        header = file_stream.read(5)
        file_stream.seek(0)
        return header == b'%PDF-'
    except Exception as e:
        logging.warning("Could not validate PDF header: %s", e)
        return False
    

def list_pdfs_in_folder(drive_client, folder_id: str, require_pdf: bool = True) -> pd.DataFrame:
    """
    List files in a Google Drive folder as a DataFrame with columns: ['Name', 'ID', 'URL'].

    If require_pdf=True (default), filters by MIME type. If False, checks file header for valid PDF.

    Args:
        drive_client: Authenticated Google Drive API client.
        folder_id (str): Folder ID to search.
        require_pdf (bool): If True, restrict to PDFs via MIME type. If False, verify by content.

    Returns:
        pd.DataFrame
    """
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        if require_pdf:
            query += " and mimeType='application/pdf'"

        files = []
        page_token = None
        while True:
            resp = drive_client.files().list(
                q=query,
                fields="nextPageToken, files(id, name)",
                pageSize=100,
                pageToken=page_token,
            ).execute()
            files.extend(resp.get("files", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        df = pd.DataFrame(files).rename(columns={"id": "ID", "name": "Name"})
        df["URL"] = df["ID"].apply(lambda x: f"https://drive.google.com/file/d/{x}/view")

        if not require_pdf and not df.empty:
            from io import BytesIO
            from googleapiclient.http import MediaIoBaseDownload

            def is_pdf(file_id: str) -> bool:
                try:
                    fh = BytesIO()
                    request = drive_client.files().get_media(fileId=file_id)
                    MediaIoBaseDownload(fh, request).next_chunk()
                    fh.seek(0)
                    return fh.read(5) == b"%PDF-"
                except Exception:
                    return False

            df = df[df["ID"].apply(is_pdf)]

        return df[["Name", "ID", "URL"]]

    except Exception as e:
        logging.error("Failed to list files from folder %s: %s", folder_id, e)
        return pd.DataFrame(columns=["Name", "ID", "URL"])

    

def fetch_file(drive_client, file_id: str, require_pdf: bool = True) -> Optional[BytesIO]:
    """
    Download a file from Google Drive into memory as BytesIO.

    Args:
        drive_client: An authenticated Google Drive API client.
        file_id (str): The ID of the file to fetch.
        require_pdf (bool): If True, the file must be a valid PDF or None is returned.

    Returns:
        BytesIO: File content if successfully downloaded and (optionally) validated as a PDF; otherwise None.
    """
    try:
        request = drive_client.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)

        if require_pdf and not is_pdf_file(fh):
            logging.warning("File ID %s is not a valid PDF", file_id)
            return None

        return fh
    except HttpError as e:
        logging.error("Failed to download file with ID %s: %s", file_id, e)
        return None


def fetch_sheet(sheets_client: SheetsClient, spreadsheet_id: str) -> Worksheet | None:
    """
    Fetches the first worksheet from a Google Sheet by ID.

    Args:
        sheets_client (GSpreadClient): An authenticated gspread client.
        spreadsheet_id (str): The ID of the Google Spreadsheet.

    Returns:
        Worksheet: The first worksheet of the spreadsheet.
        None: If there is an error.
    """
    try:
        sheet = sheets_client.open_by_key(spreadsheet_id).sheet1
        try:
            if not sheet.get_all_values():
                logging.error("Worksheet %s is empty.", spreadsheet_id)
                return None
        except Exception as inner:
            logging.error("[fetch_sheet] Could not read worksheet %s values: %s", spreadsheet_id, inner)
            return None
        logging.info(sheet.title)
        try:
            if not sheet.get_all_values():
                logging.error("Worksheet %s is empty.", spreadsheet_id)
                return None
        except Exception as inner:
            logging.error("[fetch_sheet] Could not read worksheet %s values: %s", spreadsheet_id, inner)
            return None
        return sheet
    except Exception as e:
        logging.error("[fetch_sheet] Failed to fetch worksheet: %s", e)
        return None
    


def fetch_sheet_as_df(sheets_client: SheetsClient, spreadsheet_id: str) -> pd.DataFrame:
    """
    Fetches the first worksheet from a Google Sheet by ID and returns its contents as a DataFrame
    with all fields coerced to strings. Handles blank columns, empty rows, and sheet edge cases
    more reliably than `get_as_dataframe`. Don't use gspread; it's problematic.

    Args:
        sheets_client (SheetsClient): An authenticated gspread client.
        spreadsheet_id (str): The ID of the Google Spreadsheet.

    Returns:
        pd.DataFrame: DataFrame containing the worksheet data.
                      Returns empty DataFrame on failure or if no data rows exist.
    """
    try:
        sheet = fetch_sheet(sheets_client, spreadsheet_id)
        if sheet is None:
            logging.error("❌ Worksheet %s could not be fetched.", spreadsheet_id)
            return pd.DataFrame()

        raw_data = sheet.get_all_values()
        if not raw_data or len(raw_data) < 2:
            logging.warning("⚠️ Worksheet %s has no data rows.", spreadsheet_id)
            return pd.DataFrame(columns=raw_data[0] if raw_data else [])

        headers = raw_data[0]
        rows = raw_data[1:]

        df = pd.DataFrame(rows, columns=headers)
        df = df.loc[:, [col.strip() != "" for col in df.columns]]
        df = df.fillna("").astype(str)


        logging.info("✅ Fetched and converted worksheet %s with %d rows.", spreadsheet_id, len(df))
        return df

    except Exception as e:
        logging.error("🚨 [fetch_sheet_as_df] Failed to convert worksheet to DataFrame: %s", e)
        return pd.DataFrame()



def upload_pdf(drive_client: DriveClient, file_obj, file_name: str, folder_id: str) -> None:
    """
    Upload a PDF file to a specified Google Drive folder.

    Args:
        drive_client: Authenticated Google Drive API client.
        file_obj: File-like object (e.g., Streamlit UploadedFile).
        file_name (str): Desired file name in Drive.
        folder_id (str): Target Google Drive folder ID.

    Returns:
        str: File ID of the uploaded file.
    """
    try:
        file_obj.seek(0)  # Ensure start of file
        media = MediaIoBaseUpload(file_obj, mimetype="application/pdf")
        file_metadata = {
            "name": file_name,
            "parents": [folder_id]
        }

        uploaded_file = drive_client.files().create(
            body=file_metadata,
            media_body=media,
            fields="id"
        ).execute()

        return uploaded_file.get("id")
    except Exception as e:
        logging.error("Failed to upload file '%s' to Drive: %s", file_name, e)
        return None


def move_file(drive_client: DriveClient, file_id, target_folder_id):
    """
    Move a file to a new folder in Google Drive.
    
    Args:
        drive_client: Authenticated Google Drive API client.
        file_id (str): ID of the file to move.
        target_folder_id (str): ID of the destination folder.
    
    Returns:
        bool: True if move successful, False otherwise.
    """
    try:
        # Retrieve the existing parents to remove
        file = drive_client.files().get(fileId=file_id, fields='parents').execute()
        previous_parents = ",".join(file.get('parents'))

        # Move the file to the new folder
        drive_client.files().update(
            fileId=file_id,
            addParents=target_folder_id,
            removeParents=previous_parents,
            fields='id, parents'
        ).execute()
        return True
    except Exception as e:
        logging.error("Failed to move file %s to folder %s: %s", file_id, target_folder_id, e)
        return False


def file_exists(drive_client, file_id: str, require_pdf: bool = True) -> bool:
    """
    Check if a file exists in Google Drive, and optionally verify it's a valid PDF.

    Args:
        drive_client: An authenticated Google Drive API client.
        file_id (str): The ID of the file to check.
        require_pdf (bool): If True, confirms the file is a valid PDF.

    Returns:
        bool: True if the file exists (and is a PDF if required), False otherwise.
    """
    try:
        metadata = drive_client.files().get(fileId=file_id, fields="id").execute()
        if not require_pdf:
            return True

        # Fetch and validate as PDF
        request = drive_client.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)

        if not is_pdf_file(fh):
            logging.warning("File ID %s exists but is not a valid PDF", file_id)
            return False

        return True

    except HttpError as e:
        if getattr(e, "resp", None) and getattr(e.resp, "status", None) == 404:
            logging.info("File not found in Drive: %s", file_id)
        else:
            logging.warning("Error checking file existence for %s: %s", file_id, e)
        return False
    except Exception as e:
        logging.warning("Error checking file existence for %s: %s", file_id, e)
        return False
    
    

