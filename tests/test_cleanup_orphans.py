import pandas as pd
from unittest.mock import MagicMock
import cleanup_orphans


def test_find_rows_missing_google_ids(monkeypatch):
    df = pd.DataFrame({'pdf_id': ['1', '2'], 'google_id': ['a', 'b']})

    monkeypatch.setattr(cleanup_orphans, 'config', {'LIBRARY_UNIFIED': 'lib'})
    monkeypatch.setattr(cleanup_orphans, 'fetch_sheet', lambda sc, sid: MagicMock())

    def fake_flag(sheet, full_df, orphan_rows):
        assert list(orphan_rows['pdf_id']) == ['2']
        return [{'action': 'flag', 'pdf_id': 'b'}]

    monkeypatch.setattr(cleanup_orphans, 'flag_rows_as_orphans', fake_flag)

    orphans, logs = cleanup_orphans.find_rows_missing_google_ids(MagicMock(), df, {'a'})

    assert list(orphans['pdf_id']) == ['2']
    assert logs == [{'action': 'flag', 'pdf_id': 'b'}]


def test_find_files_missing_rows(monkeypatch):
    lib_df = pd.DataFrame({'google_id': ['a']})
    files_df = pd.DataFrame({'ID': ['a', 'b'], 'Name': ['one.pdf', 'two.pdf'], 'folder': ['PDF_TAGGING', 'PDF_TAGGING']})

    orphans, logs = cleanup_orphans.find_files_missing_rows(lib_df, files_df)

    assert list(orphans['ID']) == ['b']
    assert logs == [{'action': 'orphan_file_detected_in_PDF_TAGGING', 'pdf_id': 'b', 'pdf_file_name': 'two.pdf'}]
