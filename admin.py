import streamlit as st
import pandas as pd
from google_core import init_auth, get_gcp_clients

# ——————————————————————————————
# 1) Enforce login & greeting
init_auth()

# ——————————————————————————————
# 2) Instantiate Google Sheets & Drive clients
sheets, drive = get_gcp_clients()

# ——————————————————————————————
# 3) Load the catalog DataFrame once
LIBRARY_CATALOG_ID = "16F5tRIvuHncofRuXCsQ20A7utZWRuEgA2bvj4nQQjek"
sheet = sheets.open_by_key(LIBRARY_CATALOG_ID).sheet1
records = sheet.get_all_records()
catalog_df = pd.DataFrame(records)

# ——————————————————————————————
# 4) Build the Admin UI
st.title("ASK Admin Console")

tabs = st.tabs([
    "Browse Drive",
    "Add Docs",
    "Delete Docs",
    "Reports",
    "Catalog",
])

# — Browse Drive Tab —
with tabs[0]:
    st.header("Browse Raw PDFs in Drive")
    folder_id = st.text_input(
        "Drive Folder ID",
        value="",
        help="Paste the Google Drive folder ID where raw PDFs are stored",
    )
    if st.button("List files"):
        try:
            resp = drive.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                fields="files(id, name)"
            ).execute()
            files = resp.get("files", [])
            if not files:
                st.info("No files found.")
            else:
                for f in files:
                    st.write(f"{f['name']}  →  {f['id']}")
        except Exception as e:
            st.error(f"Error listing files: {e}")

# — Add Docs Tab —
with tabs[1]:
    st.header("Add Documents")
    st.info("Upload your PDFs and enter metadata below")
    uploaded = st.file_uploader("Choose PDF file", type="pdf", accept_multiple_files=False)
    if uploaded:
        st.write("Filename:", uploaded.name)
        # TODO: extract basic info (e.g. page count) and show form fields
    metadata = {
        "title": st.text_input("Title"),
        "organization": st.text_input("Organization"),
        "scope": st.text_input("Scope"),
        "unit": st.text_input("Unit"),
        "issue_date": st.date_input("Issue Date"),
        "expiration_date": st.date_input("Expiration Date"),
        "public_release": st.checkbox("Public Release"),
    }
    if st.button("Generate UUID & Add to Qdrant"):
        # TODO: call your add-doc logic here, passing `uploaded` and `metadata`
        st.success("Document added (placeholder)")

# — Delete Docs Tab —
with tabs[2]:
    st.header("Delete Documents")
    delete_id = st.text_input("UUID to delete", help="Enter the document's UUID")
    if st.button("Delete from Qdrant"):
        # TODO: call your delete-doc logic here using delete_id
        st.success(f"Deleted document {delete_id} (placeholder)")

# — Reports Tab —
with tabs[3]:
    st.header("Generate Report")
    st.info("Select filters and click to refresh the report CSV")
    filter_scope = st.text_input("Scope filter (leave blank for all)")
    filter_unit = st.text_input("Unit filter (leave blank for all)")
    if st.button("Create & Download Report"):
        # TODO: invoke your report-generation function with the filters
        st.success("Report created at `docs_report_qdrant_cloud{date}.csv` (placeholder)")

# — Catalog Tab —
with tabs[4]:
    st.header("Library Catalog")
    edited = st.data_editor(catalog_df, num_rows="dynamic")
    if st.button("Save catalog"):
        st.write("Saving…")
        try:
            sheet.clear()
            sheet.update([edited.columns.tolist()] + edited.values.tolist())
            st.success("Catalog updated!")
        except Exception as e:
            st.error(f"Save failed: {e}")



