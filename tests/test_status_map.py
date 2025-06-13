import pandas as pd
from unittest.mock import MagicMock
import status_map


def test_build_status_map(monkeypatch):
    lib_df = pd.DataFrame({
        "pdf_id": ["p1", "p1", "p2", "p3"],
        "gcp_file_id": ["f1", "f1", "", "f3"],
        "pdf_file_name": ["one.pdf", "one_dup.pdf", "two.pdf", "three.pdf"],
        "status": ["live", "live", "live", "live"],
    })
    drive_df = pd.DataFrame({
        "ID": ["f1", "f3", "f_orphan"],
        "Name": ["one.pdf", "three.pdf", "orphan.pdf"],
        "URL": ["u1", "u3", "u4"],
    })
    qsum_df = pd.DataFrame({
        "pdf_id": ["p1", "p3", "p_orphan"],
        "file_name": ["one.pdf", "three.pdf", "orphan_q.pdf"],
        "record_count": [2, 0, 1],
        "page_count": [1, 1, 1],
    })
    qfile_df = pd.DataFrame({
        "pdf_id": ["p1", "p3", "p_orphan"],
        "gcp_file_ids": [["f1"], ["f_mismatch"], ["f_orphan_q"]],
        "unique_file_count": [1, 1, 1],
    })

    monkeypatch.setattr(status_map, "config", {"LIBRARY_UNIFIED": "lib", "PDF_LIVE": "live"})
    monkeypatch.setattr(status_map, "rag_config", lambda k: "col")
    monkeypatch.setattr(status_map, "fetch_sheet_as_df", lambda sc, sid: lib_df)
    monkeypatch.setattr(status_map, "list_pdfs_in_folder", lambda dc, fid: drive_df)
    monkeypatch.setattr(status_map, "get_summaries_by_pdf_id", lambda qc, col, ids: qsum_df[qsum_df.pdf_id.isin(ids)])
    monkeypatch.setattr(status_map, "get_gcp_file_ids_by_pdf_id", lambda qc, col, ids: qfile_df[qfile_df.pdf_id.isin(ids)])
    monkeypatch.setattr(status_map, "get_all_pdf_ids_in_qdrant", lambda qc, col: ["p1", "p3", "p_orphan"])

    df = status_map.build_status_map(MagicMock(), MagicMock(), MagicMock())

    # sort for deterministic order
    df = df.sort_values(["pdf_id", "gcp_file_id"], na_position="last").reset_index(drop=True)

    # Verify duplicate and missing flags
    assert df.loc[df["pdf_id"] == "p1", "duplicate_pdf_id"].all()
    assert df.loc[df["pdf_id"] == "p2", "missing_gcp_file_id"].iloc[0]

    # Orphan rows should be present
    assert any(df["issues"].apply(lambda iss: "Orphan Qdrant record" in iss))
    assert any(df["issues"].apply(lambda iss: "Orphan file" in iss))

    # Specific issue list for p3
    row_p3 = df[df["pdf_id"] == "p3"].iloc[0]
    assert "No Qdrant records" in row_p3["issues"]
    assert "File ID mismatch" in row_p3["issues"]
