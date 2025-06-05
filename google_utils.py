#  Utilities for Google Drive file management

import os
import logging
import json
import streamlit as st
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

from env_config import GOOGLE_CONFIG


def get_gcp_credentials() -> Credentials:
    """
    Returns a Google `Credentials` object from a flattened JSON string in the environment.
    """
    creds_json = os.getenv("GCP_CREDENTIALS_FOR_STREAMLIT_USCGAUX_APP")
    if not creds_json:
        raise EnvironmentError("Missing GCP_CREDENTIALS_FOR_STREAMLIT_USCGAUX_APP in environment.")

    try:
        creds_dict = json.loads(creds_json)
        return Credentials.from_service_account_info(creds_dict)
    except Exception as e:
        raise ValueError(f"Failed to load GCP credentials from environment: {e}")


def get_drive_client(creds: Credentials) -> DriveClient:
    scoped_creds = creds.with_scopes(["https://www.googleapis.com/auth/drive"])
    return build("drive", "v3", credentials=scoped_creds)


def get_sheets_client(creds: Credentials) -> SheetsClient:
    scoped_creds = creds.with_scopes([
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ])
    return gspread.authorize(scoped_creds)


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
        logging.warning(f"Failed to fetch folder name for file ID {file_id}: {e}")
        return "Unknown"


def list_pdfs_in_folder(drive_client: DriveClient, folder_id: str) -> pd.DataFrame:
    """
    Fetch all PDF files from a Google Drive folder into a DataFrame.
    Returns columns: ['Name', 'ID', 'URL'].
    """
    try:
        all_files = []
        page_token = None
        query = (
            f"'{folder_id}' in parents and trashed=false and mimeType='application/pdf'"
        )
        while True:
            resp = drive_client.files().list(
                q=query,
                fields="nextPageToken, files(id, name)",
                pageSize=100,
                pageToken=page_token,
            ).execute()
            files = resp.get("files", [])
            all_files.extend(files)
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        df = pd.DataFrame(all_files).rename(columns={"name": "Name", "id": "ID"})
        df["URL"] = df["ID"].apply(lambda x: f"https://drive.google.com/file/d/{x}/view")
        return df
    except Exception as e:
        logging.error(f"Could not fetch PDFs from Drive: {e}")
        # Return an empty DataFrame with the expected columns
        return pd.DataFrame(columns=["Name", "ID", "URL"])


def fetch_pdf(drive_client: DriveClient, file_id):
    """
    Download a PDF file from Google Drive into memory as BytesIO.
    Returns BytesIO object if successful, else None.
    """
    try:
        request = drive_client.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        return fh
    except HttpError as e:
        logging.error(f"Failed to download PDF file with ID {file_id}: {e}")
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
        return sheets_client.open_by_key(spreadsheet_id).sheet1
    except Exception as e:
        logging.error(f"[fetch_sheet] Failed to fetch worksheet: {e}")
        return None
    

def fetch_sheet_as_df(sheets_client: SheetsClient, spreadsheet_id: str) -> pd.DataFrame:
    """
    Fetches the first worksheet from a Google Sheet by ID and returns its contents as a DataFrame.

    Args:
        sheets_client (GSpreadClient): An authenticated gspread client.
        spreadsheet_id (str): The ID of the Google Spreadsheet.

    Returns:
        pd.DataFrame: DataFrame containing the worksheet data.
        Returns empty DataFrame on failure.
    """
    try:
        sheet = fetch_sheet(sheets_client, spreadsheet_id)
        if sheet is None:
            return pd.DataFrame()
        data = sheet.get_all_records()
        return pd.DataFrame(data)
    except Exception as e:
        logging.error(f"[fetch_sheet_as_df] Failed to convert worksheet to DataFrame: {e}")
        return pd.DataFrame()
    

def upload_pdf(drive_client: DriveClient, file_obj, file_name: str, folder_id: str) -> str:
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
        logging.error(f"Failed to upload file '{file_name}' to Drive: {e}")
        return None


def move_pdf(drive_client: DriveClient, file_id, target_folder_id):
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
        logging.error(f"Failed to move file {file_id} to folder {target_folder_id}: {e}")
        return False
    
    

