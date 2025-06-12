import pandas as pd
from unittest.mock import MagicMock
import promote_files


def test_upsert_single_file_rejected(monkeypatch):
    monkeypatch.setattr(promote_files, 'config', {'PDF_LIVE': 'live', 'LIBRARY_UNIFIED': 'lib'})

    row = pd.Series({'pdf_id': '1', 'pdf_file_name': 'f.pdf', 'google_id': 'gid', 'status': 'new_tagged'})

    monkeypatch.setattr(promote_files, 'in_qdrant', lambda client, col, pid: True)

    result, pid = promote_files.upsert_single_file(MagicMock(), MagicMock(), MagicMock(), row, 0)

    assert result == 'rejected'
    assert pid == '1'


def test_upsert_single_file_success(monkeypatch):
    monkeypatch.setattr(promote_files, 'config', {'PDF_LIVE': 'live', 'LIBRARY_UNIFIED': 'lib'})
    row = pd.Series({'pdf_id': '2', 'pdf_file_name': 'g.pdf', 'google_id': 'gid', 'status': 'new_tagged'})

    monkeypatch.setattr(promote_files, 'in_qdrant', lambda *a, **k: False)
    monkeypatch.setattr(promote_files, 'pdf_to_Docs_via_Drive', lambda *a, **k: ['doc'])
    monkeypatch.setattr(promote_files, 'chunk_Docs', lambda docs, conf: ['chunk'])
    vec = MagicMock()
    monkeypatch.setattr(promote_files, 'init_vectorstore', lambda client: vec)
    monkeypatch.setattr(promote_files, 'move_pdf', lambda *a, **k: None)

    sheet_mock = MagicMock()
    monkeypatch.setattr(promote_files, 'fetch_sheet', lambda sc, sid: sheet_mock)
    monkeypatch.setattr(promote_files, 'log_event', lambda *a, **k: None)

    result, pid = promote_files.upsert_single_file(MagicMock(), MagicMock(), MagicMock(), row, 1)

    assert result == 'uploaded'
    assert row['status'] == 'live'
    assert sheet_mock.update.called
    vec.add_documents.assert_called()
