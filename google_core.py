# core.py

import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from streamlit_authenticator import Authenticate

def init_auth():
    """Gate the app behind Google OAuth2 via streamlit-authenticator."""
    auth = Authenticate(
        credentials=st.secrets["credentials"],
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
