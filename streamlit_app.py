import base64
import logging
from typing import cast, List

import pandas as pd
import streamlit as st
from qdrant_client import QdrantClient

st.set_page_config(
    page_title="ASK Auxiliary Source of Knowledge",
    initial_sidebar_state="collapsed",
)

from env_config import env_config, RAG_CONFIG
from gcp_utils import (
    get_gcp_credentials,
    init_sheets_client,
    init_drive_client,
)
from qdrant_utils import (
    init_qdrant_client,
    get_all_pdf_ids_in_qdrant,
    get_summaries_by_pdf_id,
    get_gcp_file_ids_by_pdf_id,
    delete_records_by_pdf_id,
)
from gcp_utils import fetch_sheet_as_df
from library_utils import validate_all_rows_format
from propose_new_files import propose_new_files, FileLike
from promote_files import promote_files
from archive_delete_tagged import delete_tagged
from status_map import build_status_map
from ui_utils import init_auth, apply_styles


config = env_config()
init_auth()
apply_styles()

creds = get_gcp_credentials()
sheets_client = init_sheets_client(creds)
drive_client = init_drive_client(creds)
qdrant_client = init_qdrant_client("cloud")


@st.cache_data(show_spinner=False)
def load_qdrant_report_df(client: QdrantClient, collection: str) -> pd.DataFrame:
    """Return aggregated metadata and file info for Qdrant points."""
    pdf_ids = get_all_pdf_ids_in_qdrant(client, collection)
    if not pdf_ids:
        return pd.DataFrame()

    summary_df = get_summaries_by_pdf_id(client, collection, pdf_ids)
    files_df = get_gcp_file_ids_by_pdf_id(client, collection, pdf_ids)
    if summary_df.empty:
        df = files_df
    else:
        df = summary_df.merge(files_df, on="pdf_id", how="left")

    if "gcp_file_ids" in df.columns:
        df["gcp_file_id"] = df["gcp_file_ids"].apply(
            lambda ids: ids[0] if isinstance(ids, list) and ids else ""
        )

    if "point_ids" in df.columns:
        df["num_points"] = df["point_ids"].apply(
            lambda x: len(x) if isinstance(x, list) else 0
        )
    elif "record_count" in df.columns:
        df["num_points"] = df["record_count"]
    else:
        df["num_points"] = 0
    return df

st.write("")
st.write("")
st.info(f"Run context: {config['RUN_CONTEXT']}")

tabs = st.tabs([
    "1.Propose PDFs",
    "2.Update Metadata",
    "3.Launch",
    "Admin Tools",
    "Library",
    "Qdrant Report",
])


with tabs[0]:
    st.write("")
    uploaded_files = st.file_uploader(
        "Step 1: Choose PDF files to propose", type="pdf", accept_multiple_files=True)
    if uploaded_files:
        with st.spinner("Uploading & scanning PDFs for duplicates..."):
            new_rows_df, failed_files, duplicate_files = propose_new_files(
                drive_client, sheets_client, cast(List[FileLike], uploaded_files))
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
    with st.container():
        st.markdown("**Step 2. Fill out metadata in `LIBRARY_UNIFIED` and set status to `new_tagged`**")
        indent_col, content_col = st.columns([0.05, 0.95])
        link = f"https://docs.google.com/spreadsheets/d/{config['LIBRARY_UNIFIED']}"
        with content_col:
            st.link_button("Open LIBRARY_UNIFIED", link)

with tabs[2]:
    st.write("")
    with st.container():
        st.markdown("**Step 3. Promote new, tagged files to production**")
        indent_col, content_col = st.columns([0.05, 0.95])
        with content_col:
            dry_run = st.checkbox(
                "Optional: test functionality without uploading")
            config["DRY_RUN"] = str(dry_run)
            if st.button("Promote PDFs", key="promote_pdfs", type="secondary"):
                with st.spinner("Promoting PDFs..."):
                    promote_files(drive_client, sheets_client, qdrant_client)
                st.success("✅ Files promoted")

with tabs[3]:
    st.write("")
    with st.container():
        st.markdown("**Review PDFs in `PDF_TAGGING`, `PDF_LIVE`, `PDF_ARCHIVE`**")
        st.write("Do not upload new PDFs this way unless you are prepared to deal with the new file_id they generate.")
        indent_col, content_col1, content_col2, content_col3 = st.columns([0.05, 0.32, 0.32, 0.33])
        link1 = f"https://drive.google.com/drive/folders/{config['PDF_TAGGING']}"
        link2 = f"https://drive.google.com/drive/folders/{config['PDF_LIVE']}"
        link3 = f"https://drive.google.com/drive/folders/{config['PDF_ARCHIVE']}"
        with content_col1:
            st.link_button("Open PDF_TAGGING", link1)
        with content_col2:
            st.link_button("Open LIVE", link2)
        with content_col3:
            st.link_button("Open PDF_ARCHIVE", link3)
    
    st.write("") 
    with st.container():
        st.markdown("**Validate rows in LIBRARY_UNIFIED**")
        indent_col, content_col = st.columns([0.05, 0.95])
        with content_col:
            if st.button("Validate rows format", key="validate_rows_format", type="secondary"):
                with st.spinner("Searching rows, PDFs, and records..."):
                    valid_df, invalid_df, log_df = validate_all_rows_format(sheets_client)
                if invalid_df.empty:
                    st.success("✅ No invalid rows found.")
                else:
                    if not invalid_df.empty:
                        st.write("⚠️ Invalid rows found in LIBRARY_UNIFIED")
                        st.dataframe(invalid_df)    

    st.write("")        
    with st.container():
        st.markdown("**Remove items tagged in `LIBRARY_UNIFIED`**")
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
    st.write("")
    with st.container():
        st.markdown("**Display a status map across systems showing orphan rows, PDFs and Qdrant records**")
        indent_col, content_col = st.columns([0.05, 0.95])
        with content_col:
            if st.button("Create status map", key="status_map", type="secondary"):
                with st.spinner("Searching rows, PDFs, and records...Building status map..."):
                    status_df, issues_df = build_status_map(
                        drive_client, sheets_client, qdrant_client
                    )
                if issues_df.empty:
                    st.info("No issues found — all rows are consistent across systems.")
                else:
                    st.info("🚨 Issues Detected")
                    st.dataframe(issues_df)

                    with st.expander("🔍 Show Full Status Map"):
                        st.dataframe(status_df)

with tabs[4]:
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

with tabs[5]:
    st.write("")

    with st.spinner("Loading metadata from Qdrant..."):
        collection = RAG_CONFIG["qdrant_collection_name"]
        metadata_df = load_qdrant_report_df(qdrant_client, collection)

    if metadata_df.empty:
        st.warning("⚠️ No data to display.")
    else:
        df = metadata_df.copy()
        gcp_col = "gcp_file_id" if "gcp_file_id" in df.columns else "gcp_id" if "gcp_id" in df.columns else None
        order_cols = ["pdf_id"]
        if gcp_col:
            order_cols.append(gcp_col)
        order_cols.append("num_points")
        df = df[order_cols + [c for c in df.columns if c not in order_cols]]
        df.insert(0, "selected", False)

        edited = st.data_editor(df, use_container_width=True, hide_index=False)

        selected = edited[edited["selected"]]
        if st.button(
            "Delete selected points",
            disabled=selected.empty,  # pyright: ignore[reportAttributeAccessIssue]
        ):
            delete_records_by_pdf_id(
                qdrant_client, collection, list(selected["pdf_id"])
            )
            load_qdrant_report_df.clear()  # pyright: ignore[reportFunctionMemberAccess]
            st.success("Selected points deleted from Qdrant")

        try:
            csv_string = metadata_df.to_csv(index=False)
            b64 = base64.b64encode(csv_string.encode("utf-8")).decode("utf-8")
            file_name = "qdrant_metadata.csv"
            link = f'<a href="data:file/csv;base64,{b64}" download="{file_name}">Download CSV</a>'
            st.markdown(link, unsafe_allow_html=True)
        except Exception:
            logging.exception("Error exporting metadata to CSV")
            st.error("Failed to prepare download link.")

