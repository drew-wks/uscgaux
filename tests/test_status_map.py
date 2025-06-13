import pandas as pd
from unittest.mock import MagicMock
import status_map


def test_build_status_map(monkeypatch, mock_drive_client, mock_sheets_client, mock_qdrant_client):
    monkeypatch.setattr(status_map, 'config', {
        'LIBRARY_UNIFIED': 'lib',
        'PDF_LIVE': 'live',
        'PDF_TAGGING': 'tag'
    })
    monkeypatch.setattr(status_map, 'rag_config', lambda k: 'col')

    sheet_df = pd.DataFrame({
        'pdf_id': ['1', '2'],
        'pdf_file_name': ['one.pdf', 'two.pdf'],
        'gcp_file_id': ['A', 'B']
    })
    monkeypatch.setattr(status_map, 'fetch_sheet_as_df', lambda sc, sid: sheet_df)

    live_df = pd.DataFrame({'Name': ['one.pdf'], 'ID': ['A'], 'URL': ['urlA']})
    tag_df = pd.DataFrame({'Name': ['two.pdf'], 'ID': ['B'], 'URL': ['urlB']})
    monkeypatch.setattr(status_map, 'list_pdfs_in_folder', lambda dc, fid: live_df if fid == 'live' else tag_df)

    qdrant_file_df = pd.DataFrame({'pdf_id': ['1', '3'], 'gcp_file_id': ['A', 'C']})
    monkeypatch.setattr(status_map, 'get_file_ids_by_pdf_id', lambda qc, col, ids: qdrant_file_df)

    qdrant_summary_df = pd.DataFrame({
        'pdf_id': ['1', '3'],
        'title': ['T1', 'T3'],
        'pdf_file_name': ['one.pdf', 'three.pdf'],
        'record_count': [1, 2],
        'page_count': [5, 6],
        'point_ids': [['p1'], ['p3']]
    })
    monkeypatch.setattr(status_map, 'get_summaries_by_pdf_id', lambda qc, col, ids: qdrant_summary_df)

    result = status_map.build_status_map(mock_drive_client, mock_sheets_client, mock_qdrant_client)
    result = result.sort_values('pdf_id').reset_index(drop=True)

    assert list(result['pdf_id']) == ['1', '2', '3']
    assert result.loc[0, 'in_sheet'] and result.loc[0, 'in_qdrant'] and result.loc[0, 'in_drive']
    assert result.loc[0, 'file_ids_match']
    assert result.loc[0, 'issues'] == ''

    assert result.loc[1, 'issues'] == 'missing_qdrant'
    assert not result.loc[1, 'file_ids_match']

    assert result.loc[2, 'issues'] == 'missing_sheet; missing_drive'
