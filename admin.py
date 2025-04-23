import streamlit as st
import pandas as pd
import yaml
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from streamlit_authenticator import Authenticate

# ——————————————————————————————
# 1) Load your OAuth & cookie config for streamlit-authenticator
with open("config.yaml") as f:
    config = yaml.safe_load(f)

authenticator = Authenticate(
    credentials=config["credentials"],
    cookie_name=config["cookie"]["name"],
    key=config["cookie"]["key"],
    expiry_days=config["cookie"]["expiry_days"],
    preauthorized=config.get("preauthorized", {}),
)

# ——————————————————————————————
# 2) Require Google login before anything else
if not st.session_state.get("name"):
    try:
        authenticator.experimental_guest_login(
            button_name="🔒 Login with Google",
            provider="google",
            oauth2=config["oauth2"],
            location="main",
        )
    except Exception as e:
        st.error(f"Authentication error: {e}")
    st.stop()

st.sidebar.write(f"👤 Hello, {st.session_state['name']}")

# ——————————————————————————————
# 3) Google Sheets authorization (store service-account JSON in Streamlit Secrets)
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_dict(
    st.secrets["gcp_service_account"],
    scope
)
gc = gspread.authorize(creds)

# ——————————————————————————————
# 4) Load the catalog sheet into a DataFrame
sheet = gc.open("ASK Library Catalog").sheet1
records = sheet.get_all_records()
df = pd.DataFrame.from_records(records)

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
