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


# --- New Mock for AzureOpenAIStrategy ---


class MockAzureOpenAIStrategy:
    """
    Mock to replace AzureOpenAIStrategy during CI/unit testing.
    It prevents the use of environment variables and mocks the generation methods.
    """

    def __init__(self, model_name: str):
        # Initialize without requiring any environment variables
        self.model = model_name
        self.model_name = model_name
        self.client = MagicMock()  # Ensure a client exists if downstream code checks it

    def generate(self, prompt: str, temperature: float = 0.3, max_new_tokens: int = 200) -> tuple[str, int, int]:
        """Mock method for standard text generation."""
        # Return a standard mock response: (content, completion_tokens, prompt_tokens)
        return 'mock_generated_text', 1, 1

    def generate_json(self, prompt: str, temperature: float = 0.3, max_new_tokens: int = 200) -> tuple[dict, int, int]:
        """Mock method for JSON generation."""
        # Return a standard mock JSON response: (content_dict, completion_tokens, prompt_tokens)
        return {'mock_key': 'mock_value'}, 1, 1

    def get_azure_config(self) -> dict[str, str]:
        """Mock config getter."""
        return {'endpoint': 'mock_endpoint', 'model': self.model}


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
            mock_resp.json.side_effect = ValueError('No JSON data in mock response')

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
