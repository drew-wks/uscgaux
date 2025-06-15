import sys
import types
import pandas as pd
import streamlit.testing.v1 as st
from unittest.mock import MagicMock


def test_streamlit_app_runs(monkeypatch):
    monkeypatch.setenv("RUN_CONTEXT", "cli")
    monkeypatch.setenv("FORCE_USER_AUTH", "false")

    fake_env_config = types.SimpleNamespace(env_config=lambda: {
        "RUN_CONTEXT": "cli",
        "FORCE_USER_AUTH": False,
        "LIBRARY_UNIFIED": "lib",
        "PDF_TAGGING": "tag",
        "PDF_LIVE": "live",
        "PDF_ARCHIVE": "arc",
    }, RAG_CONFIG={"qdrant_collection_name": "col"})
    fake_gcp_utils = types.SimpleNamespace(
        get_gcp_credentials=lambda: None,
        init_sheets_client=lambda c: MagicMock(),
        init_drive_client=lambda c: MagicMock(),
        fetch_sheet_as_df=lambda *a, **k: pd.DataFrame(),
    )
    fake_qdrant_utils = types.SimpleNamespace(
        init_qdrant_client=lambda mode="cloud": MagicMock(),
        get_all_pdf_ids_in_qdrant=lambda *a, **k: [],
        get_summaries_by_pdf_id=lambda *a, **k: pd.DataFrame(),
        get_gcp_file_ids_by_pdf_id=lambda *a, **k: pd.DataFrame(),
        delete_records_by_pdf_id=lambda *a, **k: None,
        get_unique_metadata_df=lambda *a, **k: pd.DataFrame(),
    )
    fake_library_utils = types.SimpleNamespace(
        validate_all_rows_format=lambda *a, **k: (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    )
    fake_propose = types.SimpleNamespace(
        propose_new=lambda *a, **k: (pd.DataFrame(), [], []),
        FileLike=object,
    )
    fake_promote = types.SimpleNamespace(promote_files=lambda *a, **k: None)
    fake_cleanup = types.SimpleNamespace(
        delete_tagged=lambda *a, **k: pd.DataFrame(),
        build_status_map=lambda *a, **k: (pd.DataFrame(), pd.DataFrame()),
    )
    fake_ui = types.SimpleNamespace(init_auth=lambda: None, apply_styles=lambda: None)

    monkeypatch.setitem(sys.modules, "env_config", fake_env_config)
    monkeypatch.setitem(sys.modules, "utils.gcp_utils", fake_gcp_utils)
    monkeypatch.setitem(sys.modules, "utils.qdrant_utils", fake_qdrant_utils)
    monkeypatch.setitem(sys.modules, "utils.library_utils", fake_library_utils)
    monkeypatch.setitem(sys.modules, "propose_new", fake_propose)
    monkeypatch.setitem(sys.modules, "promote", fake_promote)
    monkeypatch.setitem(sys.modules, "cleanup", fake_cleanup)
    monkeypatch.setitem(sys.modules, "ui_utils", fake_ui)

    at = st.AppTest.from_file("streamlit_app.py")
    at.run()

    assert len(at.tabs) == 6
    assert "Run context: cli" in at.info[0].value
