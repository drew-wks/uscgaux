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

PDF_LIVE_FOLDER_ID = "1-vyQQp30mKzudkTOk7YJLmmVDirBOIpg"
PDF_BACKLOG_FOLDER_ID = "1993TlUkd9_4XqWCutyY5oNTpmBdnxefc"
PDF_DELETED_FOLDER_ID = "1FYUFxenYC6nWomzgv6j1O4394Zv6Bs5F"

# ——————————————————————————————
# 4) Build the Admin UI
st.title("ASK Admin Console")

tabs = st.tabs([
    "Browse PDFs",
    "Add PDFs",
    "Delete PDFs",
    "Reports",
    "Catalog",
])

# — Browse Drive Tab —
with tabs[0]:
    st.header("Browse Raw PDFs in Drive")

    with st.spinner("Loading PDF list…"):
        try:
            all_pdfs = []
            page_token = None
            while True:
                resp = drive.files().list(
                    q=(
                        f"'{PDF_LIVE_FOLDER_ID}' in parents "
                        "and trashed=false "
                        "and mimeType='application/pdf'"
                    ),
                    fields="nextPageToken, files(id, name)",
                    pageSize=100,
                    pageToken=page_token,
                ).execute()
                files = resp.get("files", [])
                all_pdfs.extend(files)
                page_token = resp.get("nextPageToken")
                if not page_token:
                    break

            if not all_pdfs:
                st.info("No PDFs found in the specified folder.")
            else:
                st.success(f"Found {len(all_pdfs)} PDF(s):")
                for f in all_pdfs:
                    url = f"https://drive.google.com/file/d/{f['id']}/view"
                    st.markdown(f"- [{f['name']}]({url})")

        except Exception as e:
            st.error(f"Error fetching PDFs: {e}")



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



