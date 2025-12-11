import pytest
from unittest.mock import MagicMock, patch
import requests
import os
from pathlib import Path
from typing import Any
from utils.ckan import CKANClient
from utils.prompt_manager import PromptManager
import pandas as pd
from utils.utils import table_markdown
from datetime import datetime


class MockBaseClassifier:
    def __init__(self, model: Any):
        self.model = model
        self.prompt_manager = PromptManager()


class MockAzureOpenAIStrategy:
    """Mock to replace AzureOpenAIStrategy during CI/unit testing."""

    def __init__(self, model_name: str, azure_endpoint: str, api_key: str):
        self.model = model_name
        self.model_name = model_name
        self.azure_endpoint = azure_endpoint
        self.api_key = api_key
        self.client = MagicMock()

    def generate(self, _prompt: str, _temperature: float = 0.3, max_new_tokens: int = 200):
        return 'mock_generated_text', 1, 1

    def generate_json(self, _prompt: str, _temperature: float = 0.3, max_new_tokens: int = 200):
        return {'mock_key': 'mock_value'}, 1, 1

    def get_azure_config(self):
        return {'endpoint': 'mock_endpoint', 'model': self.model}


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
def mock_ckan_client():
    """
    Returns a CKANClient configured with temporary env variables
    and an isolated project root to avoid actual file I/O.
    """

    with patch.dict(
        os.environ,
        {'HDX_URL': 'http://mock-ckan.org', 'HDX_KEY': 'mock-token'},
        clear=True,
    ):
        client = CKANClient(base_url='http://mock-ckan.org', api_token='mock-token')
        client.project_root = Path('/tmp/mock-project-root')
        return client


@pytest.fixture
def mock_azure_client():
    # Mock the chat.completions.create method
    mock_chat = MagicMock()
    mock_chat.completions.create.return_value = MagicMock(
        choices=[MagicMock(message=MagicMock(content='mock text'))],
        usage=MagicMock(completion_tokens=5, prompt_tokens=10),
    )

    # Mock AzureOpenAI instance
    mock_azure = MagicMock()
    mock_azure.chat = mock_chat
    return mock_azure


@pytest.fixture
def mock_azure_strategy():
    return MockAzureOpenAIStrategy(
        model_name='mock-model',
        azure_endpoint='mock-endpoint',
        api_key='mock-key',
    )


@pytest.fixture
def mock_base_classifier(mock_azure_strategy):
    return MockBaseClassifier(model=mock_azure_strategy)


@pytest.fixture
def mock_non_pii_report():
    return {
        'model_name': 'mock-model',
        'isp_used': 'mock-isp',
        'sensitivity': 'mock-sensitivity',
        'explanation': 'mock-explanation',
        'sensitive_columns': ['name', 'age', 'country'],
        'cited_isp_rules': ['ISP-1', 'ISP-2', 'ISP-3'],
    }


@pytest.fixture
def mock_sdd_report():
    return {
        'resource_id': '1',
        'file_name': 'file.csv',
        'file_url': 'http://example.com',
        'processing_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'processing_success': True,
        'n_records': 10,
        'n_columns': 3,
        'completion_tokens': 0,
        'prompt_tokens': 0,
        'columns': [
            {'column_name': 'name', 'sample_values': ['Alice', 'Bob', 'Charlie']},
            {'column_name': 'age', 'sample_values': ['25', '30', '35']},
            {'column_name': 'country', 'sample_values': ['US', 'UK', 'DE']},
        ],
    }


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'country': ['US', 'UK', 'DE'],
        }
    )


@pytest.fixture
def sample_report(mock_sdd_report):
    report = {
        'resource_id': '1',
        'file_name': 'file.csv',
        'file_url': 'http://example.com',
        'processing_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'processing_success': True,
        'n_records': 10,
        'n_columns': 3,
        'completion_tokens': 0,
        'prompt_tokens': 0,
        'columns': [
            {
                'column_name': 'name',
                'sample_values': ['Alice', 'Bob', 'Charlie'],
                'pii': {'entity_type': 'PERSON_NAME'},
            },
            {'column_name': 'age', 'sample_values': ['25', '30', '35'], 'pii': {'entity_type': 'AGE'}},
            {'column_name': 'country', 'sample_values': ['US', 'UK', 'DE'], 'pii': {'entity_type': 'STREET_ADDRESS'}},
        ],
    }
    return report


@pytest.fixture
def sample_table_markdown(sample_report):
    return table_markdown(sample_report)


@pytest.fixture
def mock_isp():
    isp = {
        'sensitivity_rules': {
            'HIGH_SENSITIVE': 'data and information type',
            'MODERATE_SENSITIVE': 'data and information type',
            'LOW/NON_SENSITIVE': 'data and information type',
            'SEVERE_SENSITIVE': 'data and information type',
        }
    }

    return isp
