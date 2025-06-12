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
