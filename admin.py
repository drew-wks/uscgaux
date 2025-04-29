import streamlit as st
st.set_page_config(page_title="ASK Auxiliary Source of Knowledge", initial_sidebar_state="collapsed")
from datetime import datetime, timedelta
import base64
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from admin_config import *
import google_utils as goo_utils
import ui_utils
import input_form


goo_utils.init_auth()

drive_client, sheets_client = goo_utils.get_gcp_clients()

# ——————————————————————————————
# Loads the catalog DataFrame once
sheet = sheets_client.open_by_key(LIBRARY_CATALOG_ID).sheet1
records = sheet.get_all_records()
catalog_df = pd.DataFrame(records)



ui_utils.apply_styles()

tabs = st.tabs([
    "Add PDFs",
    "Delete PDFs",
    "DB Report",
    "Catalog",
])


# — Add Docs Tab —
with tabs[0]:
    st.markdown("#### Add Documents")
    st.markdown("##### 1. Select a file")
    uploaded = st.file_uploader("Choose PDF file", type="pdf", accept_multiple_files=False)

    if uploaded:
        st.markdown("##### 2. Complete the form")
        metadata = input_form.show_metadata_form()
        if metadata:
            st.markdown("##### 2. Complete the form")

        if st.button("Generate pdf ID & Add to Qdrant"):
            # TODO: your save logic here
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



