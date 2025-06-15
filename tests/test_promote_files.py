import pandas as pd
from unittest.mock import MagicMock
import promote


def test_upsert_single_file_rejected(monkeypatch):
    monkeypatch.setattr(promote, 'config', {'PDF_LIVE': 'live', 'LIBRARY_UNIFIED': 'lib'})

    row = pd.Series({'pdf_id': '1', 'pdf_file_name': 'f.pdf', 'gcp_file_id': 'gid', 'status': 'new_tagged'})

    monkeypatch.setattr(promote, 'in_qdrant', lambda client, col, pid: True)
    monkeypatch.setattr(promote, 'file_exists', lambda *a, **k: True)

    result, pid = promote.upsert_single_file(MagicMock(), MagicMock(), MagicMock(), row, 0)

    assert result == 'rejected'
    assert pid == '1'


def test_upsert_single_file_success(monkeypatch):
    monkeypatch.setattr(promote, 'config', {'PDF_LIVE': 'live', 'LIBRARY_UNIFIED': 'lib'})
    row = pd.Series({'pdf_id': '2', 'pdf_file_name': 'g.pdf', 'gcp_file_id': 'gid', 'status': 'new_tagged'})

    monkeypatch.setattr(promote, 'in_qdrant', lambda *a, **k: False)
    monkeypatch.setattr(promote, 'file_exists', lambda *a, **k: True)
    monkeypatch.setattr(promote, 'pdf_to_Docs_via_Drive', lambda *a, **k: ['doc'])
    monkeypatch.setattr(promote, 'chunk_Docs', lambda docs, conf: ['chunk'])
    vec = MagicMock()
    monkeypatch.setattr(promote, 'init_vectorstore', lambda client: vec)
    monkeypatch.setattr(promote, 'move_file', lambda *a, **k: None)

    sheet_mock = MagicMock()
    monkeypatch.setattr(promote, 'fetch_sheet', lambda sc, sid: sheet_mock)
    monkeypatch.setattr(promote, 'log_event', lambda *a, **k: None)

    result, pid = promote.upsert_single_file(MagicMock(), MagicMock(), MagicMock(), row, 1)

    assert result == 'uploaded'
    assert row['status'] == 'live'
    assert sheet_mock.update.called
    vec.add_documents.assert_called()
