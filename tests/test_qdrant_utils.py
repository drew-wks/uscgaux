import pandas as pd
from unittest.mock import MagicMock

import qdrant_utils


def test_which_qdrant_local():
    class qdrant_local:  # noqa: N801
        pass
    client = MagicMock()
    client._client = qdrant_local()
    assert qdrant_utils.which_qdrant(client) == 'local'


def test_list_collections():
    client = MagicMock()
    col = MagicMock()
    col.name = 'test'
    client.get_collections.return_value.collections = [col]
    assert qdrant_utils.list_collections(client) == ['test']


def test_in_qdrant_true(mock_qdrant_client):
    mock_qdrant_client.search.return_value = ['result']
    assert qdrant_utils.in_qdrant(mock_qdrant_client, 'col', 'id')


def test_check_record_exists(mock_qdrant_client):
    mock_qdrant_client.get_point.return_value = {'id': '1'}
    assert qdrant_utils.check_record_exists(mock_qdrant_client, 'col', '1')


def test_get_all_pdf_ids_in_qdrant(mock_qdrant_client):
    record = MagicMock()
    record.payload = {'metadata': {'pdf_id': 'a'}}
    mock_qdrant_client.scroll.return_value = ([record], None)
    ids = qdrant_utils.get_all_pdf_ids_in_qdrant(mock_qdrant_client, 'col')
    assert ids == ['a']


def test_delete_records_by_pdf_id(mock_qdrant_client):
    result = MagicMock()
    result.operation_id = 'op'
    mock_qdrant_client.delete.return_value = result
    qdrant_utils.delete_records_by_pdf_id(mock_qdrant_client, ['a'], 'col')
    assert mock_qdrant_client.delete.called


def test_update_file_id_for_pdf_id(monkeypatch, mock_qdrant_client):
    calls = {}

    def fake_set_payload(collection_name, payload, points, key="metadata"):
        calls["collection"] = collection_name
        calls["payload"] = payload
        calls["key"] = key
        calls["points"] = points
        return MagicMock(status="completed")

    monkeypatch.setattr(mock_qdrant_client, "set_payload", fake_set_payload)

    qdrant_utils.update_file_id_for_pdf_id(mock_qdrant_client, "col", "pid", "fid")

    assert calls["collection"] == "col"
    assert calls["payload"] == {"gcp_file_id": "fid"}
    assert calls["key"] == "metadata"
    assert isinstance(calls["points"], qdrant_utils.models.Filter)


def test_update_qdrant_file_ids_for_live_rows(monkeypatch, mock_qdrant_client, mock_sheets_client):
    df = pd.DataFrame({
        "pdf_id": ["1", "2"],
        "gcp_file_id": ["a", "b"],
        "status": ["live", "archived"],
    })

    monkeypatch.setattr(qdrant_utils, "config", {"LIBRARY_UNIFIED": "lib"})
    monkeypatch.setattr(qdrant_utils, "fetch_sheet_as_df", lambda sc, sid: df)

    called = []

    def fake_update(client, collection, pdf_id, file_id):
        called.append((pdf_id, file_id))
        return True

    monkeypatch.setattr(qdrant_utils, "update_file_id_for_pdf_id", fake_update)

    result = qdrant_utils.update_qdrant_file_ids_for_live_rows(mock_qdrant_client, mock_sheets_client, "col")

    assert called == [("1", "a")]
    assert list(result["pdf_id"]) == ["1"]
