import os
import base64
import streamlit as st
st.set_page_config(page_title="ASK Auxiliary Source of Knowledge",
                   initial_sidebar_state="collapsed")
from env_config import env_config
from typing import Tuple
from gcp_utils import get_gcp_credentials, init_sheets_client, init_drive_client
from qdrant_utils import init_qdrant_client
from gcp_utils import fetch_sheet_as_df
from library_utils import validate_all_rows_format
from propose_new_files import propose_new_files
from cleanup_orphans import find_orphans
from promote_files import promote_files
from delete_tagged import delete_tagged
from ui_utils import init_auth, apply_styles, init_cached_clients, get_library_catalog_excel_and_date

config = env_config()
init_auth()
apply_styles()

creds = get_gcp_credentials()
sheets_client = init_sheets_client(creds)
drive_client = init_drive_client(creds)
qdrant_client = init_qdrant_client("cloud")

st.write("")
st.write("")
st.info(f"Run context: {config['RUN_CONTEXT']}")

tabs = st.tabs([
    "Propose PDFs",
    "Admin",
    "Library",
    "DB Report",
])


with tabs[0]:
    st.write("")
    uploaded_files = st.file_uploader(
        "Choose PDF files to propose", type="pdf", accept_multiple_files=True)
    if uploaded_files:
        with st.spinner("Uploading & scanning PDFs for duplicates..."):
            new_rows_df, failed_files, duplicate_files = propose_new_files(
                drive_client, sheets_client, uploaded_files)
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
    st.write("")
    # Step 1
    with st.container():
        st.markdown("**Step 1. Review proposed PDFs in `PDF_TAGGING`**")
        indent_col, content_col = st.columns([0.05, 0.95])
        link = f"https://drive.google.com/drive/folders/{config['PDF_TAGGING']}"
        with content_col:
            st.link_button("Open PDF_TAGGING", link)

    # Step 2
    with st.container():
        st.markdown("**Step 2. Fill out metadata in `LIBRARY_UNIFIED`**")
        indent_col, content_col = st.columns([0.05, 0.95])
        link = f"https://docs.google.com/spreadsheets/d/{config['LIBRARY_UNIFIED']}"
        with content_col:
            st.link_button("Open LIBRARY_UNIFIED", link)

    # Step 3
    with st.container():
        st.markdown("**Step 3. Remove items tagged in `LIBRARY_UNIFIED`**")
        indent_col, content_col = st.columns([0.05, 0.95])
        with content_col:
            if st.button("Remove flagged rows", key="remove_rows", type="secondary"):
                with st.spinner("Searching rows, PDFs, and records..."):
                    rows_to_delete = delete_tagged(drive_client, sheets_client, qdrant_client)
                if rows_to_delete is None or rows_to_delete.empty:
                    st.info("No flagged rows found.")
                else:
                    st.success(
                        "✅ Rows flagged for deletion were removed from `LIBRARY_UNIFIED`, `PDF_TAGGING`/`PDF_LIVE` and Qdrant.")
                    st.dataframe(rows_to_delete.reset_index(
                        drop=True).rename(lambda x: x + 1, axis="index"))

    # Step 4
    with st.container():
        st.markdown("**Step 4. Validate rows in LIBRARY_UNIFIED**")
        indent_col, content_col = st.columns([0.05, 0.95])
        with content_col:
            if st.button("Validate rows format", key="validate_rows_format", type="secondary"):
                with st.spinner("Searching rows, PDFs, and records..."):
                    library_df = fetch_sheet_as_df(sheets_client, config["LIBRARY_UNIFIED"])
                    valid_df, invalid_df, log_df = validate_all_rows_format(library_df)
                if invalid_df.empty:
                    st.success("✅ No invalid rows found.")
                else:
                    if not log_df.empty:
                        st.write("⚠️ Invalid rows found:")
                        st.dataframe(log_df)
                    if not invalid_df.empty:
                        st.write("⚠️ Invalid rows detail:")
                        st.dataframe(invalid_df)

    # Step 5
    with st.container():
        st.markdown("**Step 5. Check for orphans: rows, PDFs and Qdrant records**")
        indent_col, content_col = st.columns([0.05, 0.95])
        with content_col:
            if st.button("Find orphans", key="find_orphans", type="secondary"):
                with st.spinner("Searching rows, PDFs, and records..."):
                    orphan_rows_df, orphan_files_df, orphan_qdrant_records_df, log_df = find_orphans(
                        drive_client, sheets_client, qdrant_client)  # type: ignore
                if orphan_rows_df.empty and orphan_files_df.empty and orphan_qdrant_records_df.empty:
                    st.success("✅ No orphans found.")
                else:
                    if not orphan_rows_df.empty:
                        st.write("⚠️ Orphan rows found in LIBRARY_UNIFIED")
                        st.dataframe(orphan_rows_df)
                    if not orphan_files_df.empty:
                        st.write("⚠️ Orphan files found in PDF folders")
                        st.dataframe(orphan_files_df)
                    if not orphan_qdrant_records_df.empty:
                        st.write("⚠️ Orphan records found in Qdrant")
                        st.dataframe(orphan_qdrant_records_df)
    # Step 6
    with st.container():
        st.markdown("**Step 6. Promote new files to production**")
        indent_col, content_col = st.columns([0.05, 0.95])
        with content_col:
            dry_run = st.checkbox(
                "Optional: test functionality without uploading")
            config["DRY_RUN"] = str(dry_run)
            if st.button("Promote PDFs", key="promote_pdfs", type="secondary"):
                with st.spinner("Promoting PDFs..."):
                    promote_files(drive_client, sheets_client, qdrant_client)
                st.success("✅ Files promoted")


with tabs[2]:
    st.write("")
    TARGET_STATUSES = ["live"]
    library_unified_df = fetch_sheet_as_df(
        sheets_client, config["LIBRARY_UNIFIED"])
    if library_unified_df is None or library_unified_df.empty:
        st.warning("⚠️ LIBRARY_UNIFIED sheet is empty or not accessible.")
        library_unified_df = None
        num_items_library = 0
        num_live_items_library = 0
    else:
        num_items_library = len(library_unified_df)
        library_unified_df["status"] = library_unified_df["status"].astype(
            str).str.strip().str.lower()
        target_statuses = [s.lower() for s in TARGET_STATUSES]
        num_live_items_library = library_unified_df["status"].isin(
            target_statuses).sum()

    st.markdown(
        f"**Unified Library:** {num_live_items_library} live items out of {num_items_library} total.")

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
    st.write("")
    qdrant_df, last_update_date = get_library_catalog_excel_and_date()
    try:
        if qdrant_df is not None:
            num_items_qdrant = len(qdrant_df)
            st.markdown(
                f"**Qdrant DB:** {num_items_qdrant} items. Last update: {last_update_date}")

            # Display the DataFrame
            st.data_editor(qdrant_df, use_container_width=True,
                           hide_index=False, disabled=True)
            isim = f'ASK_catalog_export{last_update_date}.csv'
            indir = qdrant_df.to_csv(index=False)
            b64 = base64.b64encode(indir.encode(
                encoding='utf-8')).decode(encoding='utf-8')
            linko_final = f'<a href="data:file/csv;base64,{b64}" download="{isim}">Click to download the catalog</a>'
            st.markdown(linko_final, unsafe_allow_html=True)
        else:
            st.warning("⚠️ No data to display or export.")
    except Exception as e:
        st.error(f"Error accessing report: {e}")
