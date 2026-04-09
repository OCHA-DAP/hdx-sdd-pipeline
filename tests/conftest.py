"""Pytest configuration and fixtures."""

import pytest
import sys
from pathlib import Path

# Add src to path for imports
src_path = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(src_path))

# Mock Google Sheets client initialization to avoid FileNotFoundError: service_account.json
from unittest.mock import MagicMock, patch

# We mock at the module levels where initialization happens
mock_creds = MagicMock()
mock_gspread_client = MagicMock()

patcher_creds = patch('google.oauth2.service_account.Credentials.from_service_account_file', return_value=mock_creds)
patcher_gspread = patch('gspread.authorize', return_value=mock_gspread_client)

patcher_creds.start()
patcher_gspread.start()


@pytest.fixture
def sample_column_data():
    """Sample column data for testing."""
    return {
        'column_name': 'email',
        'sample_values': ['test@example.com', 'user@test.com', 'admin@company.org'],
        'pii': {'entity_type': 'EMAIL_ADDRESS', 'sensitive': True},
    }


@pytest.fixture
def sample_sheet_report_data():
    """Sample sheet report data for testing."""
    return {
        'resource_id': 'test-resource-123',
        'file_name': 'test_data.csv',
        'file_url': 'https://example.com/test_data.csv',
        'sheet_name': 'Sheet1',
        'processing_timestamp': '2024-01-15 10:30:00',
        'processing_success': True,
        'n_records': 100,
        'n_columns': 5,
        'completion_tokens': 150,
        'prompt_tokens': 500,
        'personal_data_sensitive': True,
        'non_personal_data_sensitive': False,
        'columns': [
            {
                'column_name': 'email',
                'sample_values': ['test@example.com'],
                'pii': {'entity_type': 'EMAIL_ADDRESS', 'sensitive': True},
            },
            {
                'column_name': 'name',
                'sample_values': ['John Doe'],
                'pii': {'entity_type': 'PERSON_NAME', 'sensitive': True},
            },
        ],
        'non_pii': {'sensitivity': 'NON_SENSITIVE', 'explanation': 'No sensitive non-PII data detected'},
    }


@pytest.fixture
def azure_config():
    """Azure OpenAI configuration for integration tests."""
    import os
    from dotenv import load_dotenv

    load_dotenv()

    return {
        'model_name': 'gpt-4.1-nano',
        'azure_endpoint': os.getenv('AZURE_OPENAI_ENDPOINT', ''),
        'api_key': os.getenv('AZURE_OPENAI_API_KEY', ''),
        'api_version': '2024-02-15-preview',
    }
