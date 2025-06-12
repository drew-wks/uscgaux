import io
import pandas as pd
from unittest.mock import MagicMock
import propose_new_files


def make_file(name: str) -> io.BytesIO:
    buf = io.BytesIO(b'data')
    buf.name = name
    return buf


def test_propose_new_files_handles_duplicates(monkeypatch):
    # fake config
    monkeypatch.setattr(propose_new_files, 'config', {'LIBRARY_UNIFIED': 'lib', 'PDF_TAGGING': 'tag'})

    def fake_fetch_sheet_as_df(client, sheet_id):
        return pd.DataFrame({'pdf_id': ['1']})

    monkeypatch.setattr(propose_new_files, 'fetch_sheet_as_df', fake_fetch_sheet_as_df)
    monkeypatch.setattr(propose_new_files, 'compute_pdf_id', lambda f: '1' if f.name == 'dup.pdf' else '2')
    monkeypatch.setattr(propose_new_files, 'upload_pdf', lambda *args, **kwargs: 'gfile')

    appended = []
    monkeypatch.setattr(propose_new_files, 'append_new_rows', lambda sc, sid, df: appended.append(df))

    events = []
    monkeypatch.setattr(propose_new_files, 'log_event', lambda *args, **kwargs: events.append(args[1]))

    dup = make_file('dup.pdf')
    new = make_file('new.pdf')
    new_rows, failed, duplicates = propose_new_files.propose_new_files(MagicMock(), MagicMock(), [dup, new])

    assert duplicates == ['dup.pdf']
    assert failed == []
    assert list(new_rows['pdf_file_name']) == ['new.pdf']
    assert 'duplicate_skipped' in events and 'new_pdf_to_PDF_TAGGING' in events


def test_propose_new_files_handles_failures(monkeypatch):
    monkeypatch.setattr(propose_new_files, 'config', {'LIBRARY_UNIFIED': 'lib', 'PDF_TAGGING': 'tag'})
    monkeypatch.setattr(propose_new_files, 'fetch_sheet_as_df', lambda *a, **kw: pd.DataFrame({'pdf_id': []}))
    monkeypatch.setattr(propose_new_files, 'compute_pdf_id', lambda f: None)

    file1 = make_file('bad.pdf')
    new_rows, failed, duplicates = propose_new_files.propose_new_files(MagicMock(), MagicMock(), [file1])

    assert new_rows.empty
    assert failed == ['bad.pdf']
    assert duplicates == []
