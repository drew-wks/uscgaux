import pandas as pd
from unittest.mock import MagicMock
import status_map


def test_build_status_map(monkeypatch):
    lib_df = pd.DataFrame({
        "pdf_id": ["p1", "p2"],
        "gcp_file_id": ["f1", "f2"],
        "pdf_file_name": ["one.pdf", "two.pdf"],
        "status": ["live", "live"],
    })
    drive_df = pd.DataFrame({"ID": ["f1"], "Name": ["one.pdf"], "URL": ["url"]})
    qsum_df = pd.DataFrame({
        "pdf_id": ["p1"],
        "file_name": ["one.pdf"],
        "record_count": [1],
        "page_count": [1],
    })
    qfile_df = pd.DataFrame({
        "pdf_id": ["p1"],
        "gcp_file_ids": [["f1"]],
        "unique_file_count": [1],
    })

    monkeypatch.setattr(status_map, "config", {"LIBRARY_UNIFIED": "lib", "PDF_LIVE": "live"})
    monkeypatch.setattr(status_map, "rag_config", lambda k: "col")
    monkeypatch.setattr(status_map, "fetch_sheet_as_df", lambda sc, sid: lib_df)
    monkeypatch.setattr(status_map, "list_pdfs_in_folder", lambda dc, fid: drive_df)
    monkeypatch.setattr(status_map, "get_summaries_by_pdf_id", lambda qc, col, ids: qsum_df)
    monkeypatch.setattr(status_map, "get_file_ids_by_pdf_id", lambda qc, col, ids: qfile_df)

    df = status_map.build_status_map(MagicMock(), MagicMock(), MagicMock())

    df = df.sort_values("pdf_id").reset_index(drop=True)
    assert list(df["in_drive"]) == [True, False]
    assert list(df["in_qdrant"]) == [True, False]
    assert df.loc[0, "issues"] == []
    assert df.loc[1, "issues"] == ["Missing in Drive", "Missing in Qdrant"]
