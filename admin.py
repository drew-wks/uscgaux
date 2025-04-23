import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from streamlit_authenticator import Authenticate

# ——————————————————————————————
# 1) Build the authenticator from Streamlit Secrets
authenticator = Authenticate(
    credentials=st.secrets["credentials"],
    cookie_name=st.secrets["cookie"]["name"],
    key=st.secrets["cookie"]["key"],
    expiry_days=st.secrets["cookie"]["expiry_days"],
    preauthorized=st.secrets["credentials"].get("preauthorized", {}),
)

# ——————————————————————————————
# 2) Require Google login before rendering anything else
if not st.session_state.get("name"):
    try:
        authenticator.experimental_guest_login(
            button_name="🔒 Login with Google",
            provider="google",
            oauth2=st.secrets["oauth2"],
            location="main",
        )
    except Exception as e:
        st.error(f"Authentication error: {e}")
    st.stop()

st.sidebar.write(f"👤 Hello, {st.session_state['name']}")

# ——————————————————————————————
# 3) Google Sheets authorization via google-auth (service account in secrets)
scopes = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=scopes
)
gc = gspread.authorize(creds)

# ——————————————————————————————
# 4) Load the catalog sheet into a DataFrame
sheet = gc.open("ASK Library Catalog").sheet1
records = sheet.get_all_records()
df = pd.DataFrame(records)

# ——————————————————————————————
# 5) Build the admin UI with tabs
st.title("ASK Admin Console")
tabs = st.tabs(["Add Docs", "Delete Docs", "Reports", "Catalog"])

with tabs[3]:
    st.header("Library Catalog")
    edited = st.experimental_data_editor(df, num_rows="dynamic")
    if st.button("Save catalog"):
        sheet.clear()
        sheet.update([edited.columns.tolist()] + edited.values.tolist())
        st.success("Catalog updated!")
