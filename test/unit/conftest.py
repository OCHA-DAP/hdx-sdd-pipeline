import pytest
from unittest.mock import MagicMock, patch
import requests
import os
from pathlib import Path

from utils.ckan import CKANClient


# ----------------------------------------------------------------------
# Create mock Response factory
# ----------------------------------------------------------------------
@pytest.fixture
def create_mock_response():
    """
    Factory for generating mocked requests.Response objects.
    """

    def factory(status_code, json_data=None, content=None, raise_for_status_exc=None):
        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.status_code = status_code
        mock_resp.content = content or b''
        mock_resp.url = 'http://mock.url/file.csv'

        if json_data is not None:
            mock_resp.json.return_value = json_data
        else:
            mock_resp.json.side_effect = ValueError('No JSON data in mock response')

        if raise_for_status_exc:
            mock_resp.raise_for_status.side_effect = raise_for_status_exc
        else:
            mock_resp.raise_for_status.return_value = None

        return mock_resp

    return factory


# ----------------------------------------------------------------------
# CKANClient fixture
# ----------------------------------------------------------------------
@pytest.fixture
def mock_client():
    """
    Returns a CKANClient configured with temporary env variables
    and an isolated project root to avoid actual file I/O.
    """

    with patch.dict(
        os.environ,
        {'HDX_URL': 'http://mock-ckan.org', 'HDX_KEY': 'mock-token'},
        clear=True,
    ):
        client = CKANClient()
        client.project_root = Path('/tmp/mock-project-root')
        return client
