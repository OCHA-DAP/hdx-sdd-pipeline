import pytest
from unittest.mock import MagicMock, patch
import requests
import os
from pathlib import Path

# NOTE: This import assumes your project structure looks like:
# project_root/
# ├── utils/
# │   └── ckan.py (your provided file)
# └── tests/
#     ├── conftest.py
#     └── test_ckan.py
from utils.ckan import CKANClient


# Helper function to create a mock response object
@pytest.fixture
def create_mock_response():
    """Fixture factory for creating mock requests.Response objects."""

    def _creator(status_code, json_data=None, content=None, raise_for_status_exc=None):
        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.status_code = status_code
        mock_resp.content = content if content is not None else b''
        mock_resp.url = 'http://mock.url/file.csv'  # Mock URL for download tests

        if json_data is not None:
            # Set the value returned when .json() is called
            mock_resp.json.return_value = json_data
        else:
            # If no JSON data, simulate a failure if .json() is called unexpectedly
            mock_resp.json.side_effect = ValueError("No JSON data in mock response")

        if raise_for_status_exc:
            # Configure to raise an exception when raise_for_status() is called
            mock_resp.raise_for_status.side_effect = raise_for_status_exc
        else:
            # Default success case for raise_for_status()
            mock_resp.raise_for_status.return_value = None

        return mock_resp

    return _creator


# Fixture for a mock CKANClient instance
@pytest.fixture
def mock_client():
    """
    Provides a CKANClient instance initialized with mock credentials and headers.
    Patches environment variables only during initialization.
    """
    # Use patch.dict to temporarily set environment variables used by the constructor
    with patch.dict(os.environ, {'HDX_URL': 'http://mock-ckan.org', 'HDX_KEY': 'mock-token'}, clear=True):
        client = CKANClient()
        # Set a mockable project root for file operations
        client.project_root = Path('/tmp/mock-project-root')
        yield client
