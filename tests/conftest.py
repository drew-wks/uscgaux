import pytest
from unittest.mock import MagicMock

@pytest.fixture
def mock_drive_client():
    return MagicMock()

@pytest.fixture
def mock_sheets_client():
    return MagicMock()

@pytest.fixture
def mock_qdrant_client():
    return MagicMock()
