import streamlit as st
st.set_page_config(page_title="ASK Auxiliary Source of Knowledge", initial_sidebar_state="collapsed")
from datetime import datetime, timedelta
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
import google_utils as goo_utils
import ui_utils
import base64
from admin_config import *

# ——————————————————————————————
# 1) Enforce login & greeting
goo_utils.init_auth()

# ——————————————————————————————
# Instantiates Google Sheets & Drive clients
drive_client, sheets_client = goo_utils.get_gcp_clients()

# ——————————————————————————————
# Loads the catalog DataFrame once
sheet = sheets_client.open_by_key(LIBRARY_CATALOG_ID).sheet1
records = sheet.get_all_records()
catalog_df = pd.DataFrame(records)


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
    "Add PDFs",
    "Delete PDFs",
    "DB Report",
    "Catalog",
])


# — Add Docs Tab —
with tabs[0]:
    st.header("Add PDF Documents")
    st.info("Upload your PDFs and enter metadata below")
    uploaded = st.file_uploader("Choose PDF file", type="pdf", accept_multiple_files=False)
    
    if uploaded:
        st.write("Filename:", uploaded.name)
        # TODO: extract basic info (e.g. page count) and show form fields

    doc_type_options = {
        0: "ALAUX / ALCOAST",
        1: "Coast Guard Directive (e.g., CI, CIM, CCN, etc)",
        2: "L2 Message",
        9: "Other",
    }

    doc_type_selection = st.radio(
        "This is a:",
        options=list(doc_type_options.keys()),
        format_func=lambda x: doc_type_options[x],
        index=0
    )

    # === Scope
    scope = st.selectbox(
        "Scope",
        options=["national", "area", "district", "region", "division", "sector", "flotilla", "station", "other"],
        index=0
    )

    # === Other fields
    title = st.text_input("Title")
    organization = st.text_input("Organization")
    unit = st.text_input("Unit")


    # === Dates
    today_iso = datetime.now().strftime("%Y-%m-%dT00:00:00Z")
    issue_date = st.text_input("Issue Date", value=today_iso)

    # Calculate default expiration date based on selection
    try:
        parsed_issue = datetime.strptime(issue_date, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        parsed_issue = datetime.now()

    if doc_type_selection in (0, 2):  # ALAUX / ALCOAST or L2 Message
        expiration_date_default = (parsed_issue + timedelta(days=365)).strftime("%Y-%m-%dT00:00:00Z")
    elif doc_type_selection == 1:  # Coast Guard Directive
        expiration_date_default = (parsed_issue + timedelta(days=3650)).strftime("%Y-%m-%dT00:00:00Z")
    elif doc_type_selection == 9:  # Other
        expiration_date_default = "2099-12-31T00:00:00Z"
    else:
        expiration_date_default = "2099-12-31T00:00:00Z"  # fallback safety

    # === Auxiliary-specific and Public Release
    aux_specific = st.radio(
        "Auxiliary Specific?",
        options=[True, False],
        index=0
    )

    public_release = st.radio(
        "Public Release?",
        options=[True, False],
        index=0
    )

    # --- Metadata dictionary
    metadata = {
        "title": title,
        "organization": organization,
        "scope": scope,
        "unit": unit,
        "issue_date": issue_date,
        "expiration_date": expiration_date,
        "aux_specific": aux_specific,
        "public_release": public_release,
    }

    if st.button("Generate pdf ID & Add to Qdrant"):
        # TODO: call your add-doc logic here, passing `uploaded` and `metadata`
        st.success("Document added (placeholder)")

# — Delete Docs Tab —
with tabs[1]:
    st.header("Delete Documents")
    delete_id = st.text_input("pdf ID to delete", help="Enter the document's UUID")
    if st.button("Delete from Qdrant"):
        # TODO: call your delete-doc logic here using delete_id
        st.success(f"Deleted document {delete_id} (placeholder)")

# — Reports Tab —
with tabs[2]:
    st.write("Source: docs/docs_report_qdrant_cloud*.xlsx'")
    df, last_update_date = ui_utils.get_library_catalog_excel_and_date()
    try:
        num_items = len(df)
        st.markdown(f"{num_items} items. Last update: {last_update_date}")  

        # Display the DataFrame
        st.data_editor(df, use_container_width=True, hide_index=False, disabled=True)
        isim = f'ASK_catalog_export{last_update_date}.csv'
        indir = df.to_csv(index=False)
        b64 = base64.b64encode(indir.encode(encoding='utf-8')).decode(encoding='utf-8')  
        linko_final = f'<a href="data:file/csv;base64,{b64}" download="{isim}">Click to download the catalog</a>'
        st.markdown(linko_final, unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Error accessing report: {e}")

# — Catalog Tab —
with tabs[3]:
    st.header("Library Catalog")
    st.write("Source: LIBRARY_CATALOG Google Sheet on Google Drive")
    edited = st.data_editor(
        catalog_df, 
        column_config={
            "link": st.column_config.LinkColumn(
                "link",
                help="Click to open the PDF in Drive"
            )
        },
        num_rows="dynamic")
    if st.button("Save catalog"):
        st.write("Saving…")
        try:
            sheet.clear()
            sheet.update([edited.columns.tolist()] + edited.values.tolist())
            st.success("Catalog updated!")
        except Exception as e:
            st.error(f"Save failed: {e}")



