import io
import pandas as pd
from unittest.mock import MagicMock
from googleapiclient.errors import HttpError
import pytest

import utils.gcp_utils as gcp_utils


def test_get_folder_name(mock_drive_client):
    mock_drive_client.files.return_value.get.side_effect = [
        MagicMock(execute=MagicMock(return_value={'parents': ['parent1']})),
        MagicMock(execute=MagicMock(return_value={'name': 'Folder'}))
    ]
    name = gcp_utils.get_folder_name(mock_drive_client, 'file')
    assert name == 'Folder'


def test_list_files_in_folder(mock_drive_client):
    first_call = MagicMock()
    first_call.execute.return_value = {
        'files': [{'id': '1', 'name': 'one.pdf'}],
        'nextPageToken': 'next'
    }
    second_call = MagicMock()
    second_call.execute.return_value = {
        'files': [{'id': '2', 'name': 'two.pdf'}],
        'nextPageToken': None
    }
    mock_drive_client.files.return_value.list.side_effect = [first_call, second_call]
    df = gcp_utils.list_files_in_folder(mock_drive_client, 'folder')
    assert list(df['ID']) == ['1', '2']
    assert df['URL'].iloc[0].startswith('https://drive.google.com/file/d/')


def test_file_exists_true(mock_drive_client):
    mock_drive_client.files.return_value.get.return_value.execute.return_value = {'id': 'f'}
    assert gcp_utils.file_exists(mock_drive_client, 'f') is True


def test_file_exists_false(monkeypatch, mock_drive_client):
    class FakeResp:
        status = 404
        reason = 'Not Found'
    http_err = HttpError(FakeResp(), b'')
    mock_drive_client.files.return_value.get.side_effect = http_err
    assert gcp_utils.file_exists(mock_drive_client, 'x') is False


def test_fetch_sheet_as_df(monkeypatch, mock_sheets_client):
    sheet = MagicMock()
    sheet.get_all_values.return_value = [
        ['a', 'b'],
        ['1', '2'],
        ['3', '4'],
    ]
    monkeypatch.setattr(gcp_utils, 'fetch_sheet', lambda sc, sid: sheet)
    df = gcp_utils.fetch_sheet_as_df(mock_sheets_client, 'sheet')
    assert df.equals(pd.DataFrame({'a': ['1', '3'], 'b': ['2', '4']}))



def test_fetch_sheet_empty_duplicate(mock_sheets_client):
    empty_sheet = MagicMock()
    empty_sheet.title = 'Empty'
    empty_sheet.get_all_values.return_value = []
    mock_sheets_client.open_by_key.return_value.sheet1 = empty_sheet

    result = gcp_utils.fetch_sheet(mock_sheets_client, 'sheet_id')
    assert result is None



def test_fetch_sheet_as_df_none(monkeypatch, mock_sheets_client):
    sheet = MagicMock()
    sheet.get_all_values.return_value = []
    monkeypatch.setattr(gcp_utils, 'fetch_sheet', lambda sc, sid: sheet)
    df = gcp_utils.fetch_sheet_as_df(mock_sheets_client, 'sheet')
    assert df.empty


def test_fetch_sheet_empty(mock_sheets_client):
    empty_sheet = MagicMock()
    empty_sheet.title = 'Empty'
    empty_sheet.get_all_values.return_value = []
    mock_sheets_client.open_by_key.return_value.sheet1 = empty_sheet

    result = gcp_utils.fetch_sheet(mock_sheets_client, 'sheet_id')
    assert result is None

def test_get_gcp_credentials_missing(monkeypatch):
    monkeypatch.setitem(gcp_utils.config, "GCP_CREDENTIALS_FOR_STREAMLIT_USCGAUX_APP", "")
    with pytest.raises(EnvironmentError):
        gcp_utils.get_gcp_credentials()


def test_get_gcp_credentials_bad_json(monkeypatch):
    monkeypatch.setitem(gcp_utils.config, "GCP_CREDENTIALS_FOR_STREAMLIT_USCGAUX_APP", "{bad}")
    with pytest.raises(ValueError):
        gcp_utils.get_gcp_credentials()
