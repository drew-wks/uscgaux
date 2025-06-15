import io
import json
from pathlib import Path
from unittest.mock import MagicMock
import pandas as pd
import propose_new
import promote


def load_pdf(path: str) -> io.BytesIO:
    data = Path(path).read_bytes()
    buf = io.BytesIO(data)
    buf.name = Path(path).name
    return buf


def test_propose_and_promote(monkeypatch):
    ids = json.load(open('tests/lorem_ipsum_pdf_and_gcp_ids.json'))
    tags = json.load(open('tests/lorem_ipsum_user_input_tags.json'))
    pdf_file = load_pdf('tests/lorem_ipsum.pdf')

    monkeypatch.setattr(propose_new, 'config', {'LIBRARY_UNIFIED': 'lib', 'PDF_TAGGING': 'tag'})
    monkeypatch.setattr(propose_new, 'fetch_sheet_as_df', lambda sc, sid: pd.DataFrame({'pdf_id': []}))
    monkeypatch.setattr(propose_new, 'compute_pdf_id', lambda f: ids['pdf_id'])
    monkeypatch.setattr(propose_new, 'upload_pdf', lambda *a, **k: ids['gcp_file_id'])
    appended = []
    monkeypatch.setattr(propose_new, 'append_new_rows', lambda sc, sid, df: appended.append(df))
    monkeypatch.setattr(propose_new, 'log_event', lambda *a, **k: None)

    new_rows, failed, dup = propose_new.propose_new(MagicMock(), MagicMock(), [pdf_file])

    assert not failed
    assert not dup
    assert len(new_rows) == 1
    assert new_rows.iloc[0]['pdf_id'] == ids['pdf_id']
    assert appended and appended[0].equals(new_rows)

    row_dict = new_rows.iloc[0].to_dict()
    row_dict.update(tags)
    row_dict['status'] = 'new_tagged'
    row = pd.Series(row_dict)

    monkeypatch.setattr(promote, 'config', {'PDF_LIVE': 'live', 'LIBRARY_UNIFIED': 'lib'})
    monkeypatch.setattr(promote, 'in_qdrant', lambda *a, **k: False)
    monkeypatch.setattr(promote, 'file_exists', lambda *a, **k: True)
    monkeypatch.setattr(promote, 'pdf_to_Docs_via_Drive', lambda *a, **k: ['doc'])
    monkeypatch.setattr(promote, 'chunk_Docs', lambda *a, **k: ['chunk'])
    vec = MagicMock()
    monkeypatch.setattr(promote, 'init_vectorstore', lambda client: vec)
    monkeypatch.setattr(promote, 'move_file', lambda *a, **k: None)
    sheet = MagicMock()
    monkeypatch.setattr(promote, 'fetch_sheet', lambda sc, sid: sheet)
    monkeypatch.setattr(promote, 'fetch_sheet_as_df', lambda sc, sid: pd.DataFrame([row_dict]))
    monkeypatch.setattr(promote, 'remove_rows', lambda *a, **k: None)
    monkeypatch.setattr(promote, 'log_event', lambda *a, **k: None)

    result, pid = promote.upsert_single_file(MagicMock(), MagicMock(), MagicMock(), row, 0)

    assert result == 'uploaded'
    assert pid == ids['pdf_id']
    assert row['status'] == 'live'
    assert sheet.update.called
    vec.add_documents.assert_called()
