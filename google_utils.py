#  Utilities for Google Drive authentication and file management


import streamlit as st
import pandas as pd
import gspread
from gspread.client import Client as SheetsClient
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build, Resource as DriveClient
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from streamlit_authenticator import Authenticate
from io import BytesIO



def init_auth():
    """Gate the app behind Google OAuth2 via streamlit-authenticator."""
    #    (authenticator needs to be able to mutate this, so we can't give it st.secrets directly)
    credentials_conf = {
        "usernames": {},  
        "preauthorized": st.secrets.get("credentials", {}).get("preauthorized", {})
    }

    # 2) Pull cookie settings & OAuth2 config from st.secrets
    cookie_conf = st.secrets.get("cookie", {})
    oauth2_conf = st.secrets["oauth2"]

    auth = Authenticate(
        credentials=credentials_conf,
        cookie_name=cookie_conf.get("name"),
        key=cookie_conf.get("key"),
        expiry_days=cookie_conf.get("expiry_days"),
        preauthorized=credentials_conf["preauthorized"],
    )
    if not st.session_state.get("name"):
        auth.experimental_guest_login(
            button_name="🔒 Login with Google",
            provider="google",
            oauth2=oauth2_conf,
            location="main",
        )
        st.stop()
    st.sidebar.write(f"👤 Hello, {st.session_state['name']}")


def get_gcp_clients() -> tuple[DriveClient, SheetsClient]:
    """Return (sheets_client, drive_client) using your service-account in secrets."""
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    creds_info = st.secrets.get("gcp_service_account")
    if not creds_info:
        st.error("Missing `[gcp_service_account]` in your secrets—cannot authenticate to Google APIs.")
        st.stop()

    try:
        creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    except Exception as e:
        st.error(f"Error loading service-account credentials: {e}")
        st.stop()

    sheets_client = gspread.authorize(creds)
    drive_client = build("drive", "v3", credentials=creds)
    return drive_client, sheets_client


def fetch_pdfs(drive_client, folder_id: str) -> pd.DataFrame:
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
        st.error(f"Could not fetch PDFs from Drive: {e}")
        # Return an empty DataFrame with the expected columns
        return pd.DataFrame(columns=["Name", "ID", "URL"])


def download_pdf_from_drive(drive_client, file_id):
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
        st.error(f"Failed to download PDF file with ID {file_id}: {e}")
        return None


def fetch_sheet_as_dataframe(sheets_client, spreadsheet_id) -> pd.DataFrame:
    """
    Load the first worksheet (index 0) from the spreadsheet and return as a Pandas DataFrame.
    """
    try:
        spreadsheet = sheets_client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.get_worksheet(0)
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        st.error(f"Failed to fetch sheet as DataFrame: {e}")
        return pd.DataFrame()


def move_file_between_folders(drive_client, file_id, target_folder_id):
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