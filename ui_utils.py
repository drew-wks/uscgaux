# Contains path-specific commands. Keep in project root

import os
from pathlib import Path
from fnmatch import fnmatch
import re
import datetime
import requests
import pandas as pd
import streamlit as st 
from streamlit_authenticator import Authenticate
from utils.gcp_utils import init_sheets_client, init_drive_client
from utils.qdrant_utils import init_qdrant_client

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

from env_config import env_config

config = env_config()

def init_auth():
    """Gate the app behind Google OAuth2 via streamlit-authenticator."""
    #    (authenticator needs to be able to mutate this, so we can't give it st.secrets directly)
    
    #   Google policy is OAuth clients that have remained inactive for six months will be automatically deleted. Inactivity is 
    #   determined based on the absence of token exchanges or client updates.
    
    if config["FORCE_USER_AUTH"] is False:
        return  # Skip auth when testing locally. Run & Debug launch.json is set to look for this switch
    
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


# Hide Streamlit's default UI elements: Main menu, footer, and header
COLLAPSED_CONTROL = """
    <style> 
        [data-testid="collapsedControl"] { display: none } html, body, [class*="st-"] {font-family: "Source Sans Pro", "Arial", "Helvetica", sans-serif !important;}
    </style>
    """

HIDE_STREAMLIT_UI = """
    <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
    </style>
    """

BLOCK_CONTAINER = """
        <style>
                .block-container {
                    padding-top: 1rem;
                    padding-bottom: 1rem;
                    padding-left: 3rem;
                    padding-right: 3rem;
                }
        </style>
        """

BLOCK_CONTAINER_2 = """
        <style>
                .block-container {
                    padding-top: 0rem;
                    padding-bottom: 1rem;
                    padding-left: 2rem;
                    padding-right: 2rem;
                }
        </style>
        """

FOOTER = """
    <style>
        .app-footer {
            bottom: 0;
            position: sticky;
            font-size: smaller;
            width: 700px;
            margin: auto;
            background: white;
            padding: 10px;
            border-top: 1px solid #ccc;
            text-align: left;
        }
    </style>
    <br><br>
    <div class="app-footer">
        <a href="Terms_of_service" target="_self">Terms of service</a>
    </div>
    """

LOGO = "https://raw.githubusercontent.com/drew-wks/ASK/main/images/ASK_logotype_color.png?raw=true"


def apply_styles():
    st.markdown(COLLAPSED_CONTROL, unsafe_allow_html=True)
    st.markdown(HIDE_STREAMLIT_UI, unsafe_allow_html=True)
    st.markdown(BLOCK_CONTAINER_2, unsafe_allow_html=True)
    st.image(LOGO, use_container_width=True)

# THIS DOES NOT WORK RIGHT NOW
@st.cache_resource
def init_cached_clients():
    """
    Initializes and returns cached instances of Google Drive, Google Sheets, and Qdrant clients.
    Requires the following secrets to be set in `.streamlit/secrets.toml` or Streamlit Cloud:
    - [gcp_service_account]
    - qdrant_location

    Returns:
        Tuple: (drive_client, sheets_client, qdrant_client)
    """
    # --- Google clients ---
    creds_info = st.secrets.get("gcp_service_account")
    if not creds_info:
        st.error("❌ Missing `[gcp_service_account]` in your secrets.")
        st.stop()

    try:
        drive_client = init_drive_client(creds_info)
    except Exception as e:
        st.error(f"❌ Could not connect to Google Drive: {e}")
        st.stop()

    try:
        sheets_client = init_sheets_client(creds_info)
    except Exception as e:
        st.error(f"❌ Could not connect to Google Sheets: {e}")
        st.stop()

    # --- Qdrant client ---
    location = st.secrets.get("qdrant_location")
    if not location:
        st.error("❌ Missing `qdrant_location` in your secrets.")
        st.stop()

    try:
        qdrant_client = init_qdrant_client(location)
    except Exception as e:
        st.error(f"❌ Could not connect to Qdrant: {e}")
        st.stop()

    return drive_client, sheets_client, qdrant_client

    
@st.cache_data
def get_openai_api_status():
    '''Notify user if OpenAI is down so they don't blame the app'''

    components_url = 'https://status.openai.com/api/v2/components.json'
    status_message = ''

    try:
        response = requests.get(components_url, timeout=10)
        # Raises an HTTPError if the HTTP request returned an unsuccessful status code
        response.raise_for_status()
        components_info = response.json()
        components = components_info.get('components', [])

        # Find the component that represents the API
        chat_component = next(
            (component for component in components if component.get('name', '').lower() == 'chat'), 
            None
        )
            
        if chat_component:
            status_message = chat_component.get('status', 'unknown')
            return f"ChatGPT API status: {status_message}" if status_message != 'operational' else "ChatGPT API is operational"
        else:
            return "ChatGPT API component not found"

    except requests.exceptions.HTTPError as http_err:
        return f"API check failed (HTTP error): {repr(http_err)}"
    except requests.exceptions.Timeout:
        return "API check timed out"
    except requests.exceptions.RequestException as req_err:
        return f"API check failed (Request error): {repr(req_err)}"
    except Exception as err:
        return f"API check failed (Unknown error): {repr(err)}"


def get_markdown(markdown_file):
    return Path(markdown_file).read_text()


def find_catalog_directory() -> str | None:
    """
    Attempts to locate the 'docs/library_catalog' directory either
    adjacent to this script or one level up. Returns the absolute
    path if found, otherwise None.
    """
    # level-0: next to this file
    dir1 = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "docs", "library_catalog"
    )
    # level-1: one up from parent_dir
    dir2 = os.path.join(parent_dir, "docs", "library_catalog")

    if os.path.isdir(dir1):
        return dir1
    if os.path.isdir(dir2):
        return dir2
    return None


@st.cache_data
def get_library_catalog_excel_and_date():
    """
    Retrieves the most recent Excel file matching the pattern 'docs_report_qdrant_cloud*.xlsx'
    from the 'docs/library_catalog/' directory. The function returns the file as a Pandas 
    DataFrame along with the timestamp from the filename in ISO 8601 format.

    Returns:
        tuple: (DataFrame, str) - The DataFrame containing the Excel file data and
        the timestamp from the filename as a string in the format 'YYYY-MM-DDTHH:MM:SSZ'.
        Returns (None, None) if no matching file is found or if an error occurs.
    """
    directory_path = find_catalog_directory()
    if not directory_path:
        os.write(1, b"Directory 'docs/library_catalog' not found at either level.\n")
        return None, None
    
    try:
        files_in_directory = os.listdir(directory_path)
    except FileNotFoundError:
        os.write(1, b"Directory not found.\n")
        return None, None

    # Use fnmatch for filename matching
    excel_files = [
        f for f in files_in_directory
        if fnmatch(f, "docs_report_qdrant_cloud*.xlsx")
    ]
    if not excel_files:
        os.write(1, b"There is no matching Excel file in the directory.\n")
        return None, None

    # Function to extract the timestamp from a filename.
    def extract_timestamp(filename):
        match = re.search(
            r'docs_report_qdrant_cloud_(\d{4}-\d{2}-\d{2}T\d{6}Z)\.xlsx',
            filename
        )
        if match:
            date_str = match.group(1)
            try:
                return datetime.datetime.strptime(date_str, '%Y-%m-%dT%H%M%SZ')
            except Exception:
                return None
        return None

    # Build a list of tuples (filename, extracted_timestamp)
    excel_files_with_time = []
    for file in excel_files:
        ts = extract_timestamp(file)
        if ts:
            excel_files_with_time.append((file, ts))
    
    if not excel_files_with_time:
        os.write(1, b"None of the Excel files have a valid timestamp in their filename.\n")
        return None, None

    # Sort files by the extracted timestamp, most recent first
    excel_files_with_time.sort(key=lambda x: x[1], reverse=True)
    most_recent_file, file_timestamp = excel_files_with_time[0]

    try:
        df = pd.read_excel(os.path.join(directory_path, most_recent_file))
    except Exception as e:
        os.write(1, f"Failed to read the Excel file: {e}\n".encode())
        return None, None

    # Format the timestamp from the filename to ISO 8601
    last_update_date = file_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
    return df, last_update_date
