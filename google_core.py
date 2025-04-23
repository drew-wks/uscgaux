# core.py

import streamlit as st
import copy
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from streamlit_authenticator import Authenticate

def init_auth():
    """Gate the app behind Google OAuth2 via streamlit-authenticator."""
    #    (authenticator needs to be able to mutate this, so we can't give it st.secrets directly)
    credentials_conf = {
        "usernames": {},  
        "preauthorized": st.secrets.get("credentials", {}).get("preauthorized", {})
    }

    # 2) Pull cookie settings & OAuth2 config from st.secrets
    cookie_conf = st.secrets["cookie"]
    oauth2_conf = st.secrets["oauth2"]

    auth = Authenticate(
        credentials=credentials_conf,
        cookie_name=st.secrets["cookie"]["name"],
        key=st.secrets["cookie"]["key"],
        expiry_days=st.secrets["cookie"]["expiry_days"],
        preauthorized=st.secrets["credentials"].get("preauthorized", {}),
    )
    if not st.session_state.get("name"):
        auth.experimental_guest_login(
            button_name="🔒 Login with Google",
            provider="google",
            oauth2=st.secrets["oauth2"],
            location="main",
        )
        st.stop()
    st.sidebar.write(f"👤 Hello, {st.session_state['name']}")

def get_gcp_clients():
    """Return (sheets_client, drive_client) using your service-account in secrets."""
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=scopes
    )
    sheets_client = gspread.authorize(creds)
    drive_client = build("drive", "v3", credentials=creds)
    return sheets_client, drive_client
