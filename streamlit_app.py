import os
import streamlit as st
st.set_page_config(page_title="ASK Auxiliary Source of Knowledge", initial_sidebar_state="collapsed")
import base64
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from admin_config import *
from google_utils import init_auth, get_gcp_clients, fetch_sheet
from propose_new_files import propose_new_files
from promote_files import promote_files
import ui_utils

init_auth()
drive_client, sheets_client = get_gcp_clients()

ui_utils.apply_styles()


tabs = st.tabs([
    "Propose PDFs",
    "Admin Tools",
    "Library",
    "DB Report",
])


with tabs[0]:
    st.write("")
    uploaded_files = st.file_uploader("Choose PDF files to add", type="pdf", accept_multiple_files=True)
    if uploaded_files:
        with st.spinner("Uploading & scanning PDFs for duplicates..."):
            new_rows_df, failed_files, duplicate_files = propose_new_files(uploaded_files)
        if not new_rows_df.empty:
            st.success("Added new PDF(s)...")
            st.dataframe(new_rows_df)
        if duplicate_files:
            st.warning(f"{len(duplicate_files)} duplicate PDF(s) skipped:")
            st.markdown(", ".join(duplicate_files))
        if failed_files:
            st.error(f"{len(failed_files)} file(s) failed to process:")
            st.markdown(", ".join(failed_files))


with tabs[1]:
    st.markdown("#### Admin Tools")
    st.write("")
    lib_url = "https://docs.google.com/spreadsheets/d/1glNjmKYloO0u6tCd5qy3z5ee8I-MkqwUxWsWEvLMBkM/edit?gid=0#gid=0"
    st.markdown(f"1. Populate metadata in [LIBRARY_UNIFIED]({lib_url})")
    pdf_url = "https://drive.google.com/drive/folders/1CZcBJFFhuzrbzIArDOwc07xPELqDnikf"
    st.markdown(f"OPTIONAL: Go to [all PDF folders]({pdf_url})")
    st.write("2. Once you have updated LIBRARY_UNIFIED, then click the button below. ")
    os.environ["DRY_RUN"] = str(st.checkbox("Optional: just test functionlity without uploading anything"))
    if st.button("Promote PDFs", type="secondary"):
        with st.spinner("Uploading PDFs..."):
            promote_files()
        st.write("promoting files...")
    
    
with tabs[2]:
    st.markdown("#### Unified Library")
    
    library_unified_df = fetch_sheet(os.environ["LIBRARY_UNIFIED"])
    if library_unified_df is None or library_unified_df.empty:
        st.warning("LIBRARY_UNIFIED sheet is empty or not accessible.")
        library_unified_df = None
        num_items_library = 0
    else: 
        num_items_library = len(library_unified_df)
    
    st.markdown(f"{num_items_library} items.")
    st.dataframe(
        library_unified_df,
        height=510,
        column_config={
            "link": st.column_config.LinkColumn(
                "link",
                help="Click to open the PDF in Drive"
            )
        },
    )


with tabs[3]:
    qdrant_df, last_update_date = ui_utils.get_library_catalog_excel_and_date()
    try:
        if qdrant_df is not None:
            num_items_qdrant = len(qdrant_df)
            st.markdown(f"{num_items_qdrant} items. Last update: {last_update_date}")  

            # Display the DataFrame
            st.data_editor(qdrant_df, use_container_width=True, hide_index=False, disabled=True)
            isim = f'ASK_catalog_export{last_update_date}.csv'
            indir = qdrant_df.to_csv(index=False)
            b64 = base64.b64encode(indir.encode(encoding='utf-8')).decode(encoding='utf-8')  
            linko_final = f'<a href="data:file/csv;base64,{b64}" download="{isim}">Click to download the catalog</a>'
            st.markdown(linko_final, unsafe_allow_html=True)
        else:
            st.warning("No data to display or export.")
    except Exception as e:
        st.error(f"Error accessing report: {e}")
