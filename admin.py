import streamlit as st
st.set_page_config(page_title="ASK Auxiliary Source of Knowledge", initial_sidebar_state="collapsed")
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from google_utils import init_auth, get_gcp_clients, fetch_pdfs
import ui_utils

# ——————————————————————————————
# 1) Enforce login & greeting
init_auth()

# ——————————————————————————————
# Instantiates Google Sheets & Drive clients
sheets, drive = get_gcp_clients()

# ——————————————————————————————
# Loads the catalog DataFrame once
LIBRARY_CATALOG_ID = "16F5tRIvuHncofRuXCsQ20A7utZWRuEgA2bvj4nQQjek"
sheet = sheets.open_by_key(LIBRARY_CATALOG_ID).sheet1
records = sheet.get_all_records()
catalog_df = pd.DataFrame(records)

PDF_LIVE_FOLDER_ID = "1-vyQQp30mKzudkTOk7YJLmmVDirBOIpg"
PDF_BACKLOG_FOLDER_ID = "1993TlUkd9_4XqWCutyY5oNTpmBdnxefc"
PDF_DELETED_FOLDER_ID = "1FYUFxenYC6nWomzgv6j1O4394Zv6Bs5F"


# Helper for Tab 0
def display_pdf_table(df: pd.DataFrame):
    """Render a sortable DataFrame with clickable URL links."""
    st.dataframe(
        df,
        column_config={
            "URL": st.column_config.LinkColumn(
                "Link",
                help="Click to open the PDF in Drive"
            )
        },
        use_container_width=True,
    )

# ——————————————————————————————
# 4) Build the Admin UI

ui_utils.apply_styles()

tabs = st.tabs([
    "Inspect PDFs",
    "Add PDFs",
    "Delete PDFs",
    "DB Report",
    "Catalog",
])

# — Browse Drive Tab —
with tabs[0]:
    pdf_container = st.empty()

    with pdf_container.spinner("Loading PDF list…"):
        df_pdfs = fetch_pdfs(drive, PDF_LIVE_FOLDER_ID)

        with pdf_container:
            if df_pdfs is None:
                st.error("Failed to fetch the PDF list.")
            elif df_pdfs.empty:
                st.info("No PDFs found in the specified folder.")
            else:
                st.success(f"Found {len(df_pdfs)} PDF(s):")
                display_pdf_table(df_pdfs[["Name", "URL"]])



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
    if st.button("Generate pdf ID & Add to Qdrant"):
        # TODO: call your add-doc logic here, passing `uploaded` and `metadata`
        st.success("Document added (placeholder)")

# — Delete Docs Tab —
with tabs[2]:
    st.header("Delete Documents")
    delete_id = st.text_input("pdf ID to delete", help="Enter the document's UUID")
    if st.button("Delete from Qdrant"):
        # TODO: call your delete-doc logic here using delete_id
        st.success(f"Deleted document {delete_id} (placeholder)")

# — Reports Tab —
with tabs[3]:
    df, last_update_date = ui_utils.get_library_catalog_excel_and_date()
    try:
        num_items = len(df)
        st.markdown(f"{num_items} items. Last update: {last_update_date}")  

        # Display the DataFrame
        display_df = df[['title', 'publication_number', 'organization', 'issue_date', 'expiration_date', 'scope', 'unit']]
        edited_df = st.data_editor(display_df, use_container_width=True, hide_index=False, disabled=True)
        isim = f'ASK_catalog_export{last_update_date}.csv'
        indir = edited_df.to_csv(index=False)
        b64 = base64.b64encode(indir.encode(encoding='utf-8')).decode(encoding='utf-8')  
        linko_final = f'<a href="data:file/csv;base64,{b64}" download="{isim}">Click to download the catalog</a>'
        st.markdown(linko_final, unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Error accessing report: {e}")

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



