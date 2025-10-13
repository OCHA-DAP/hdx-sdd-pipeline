"""test/unit/test_utils.py: Unit tests for utils/result_formatter.py."""

import sys
import os
import pytest

# Go up two directories: from test/unit → test → project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from utils.result_formatter import (
    format_results_for_redis,
    format_error_response,
    extract_key_metrics,
)
from utils.prompt_manager import PromptManager


def test_format_results_for_redis():
    """Test format_results_for_redis. Placeholder for now."""
    assert format_results_for_redis(None, None) is not None


def test_format_error_response():
    """Test format_error_response. Placeholder for now."""
    assert format_error_response(None, None) is not None


def test_extract_key_metrics():
    """Test extract_key_metrics. Placeholder for now."""
    assert extract_key_metrics(None) is not None


def test_prompt_manager():
    """Test prompt_manager. Placeholder for now."""
    assert PromptManager() is not None


def test_prompt_manager_list_versions():
    """Test prompt_manager.list_versions."""
    print(PromptManager().list_versions('non_pii_detection'))
    assert PromptManager().list_versions('non_pii_detection') is not None
    assert isinstance(PromptManager().list_versions('non_pii_detection'), list)
    assert len(PromptManager().list_versions('non_pii_detection')) > 0


MOCK_CONTEXT = {
    'table_context': 'This is a table context',
    'sample_values': ['This is a sample value'],
    'isp': {
        'sensitivity_rules': {
            'LOW/NON_SENSITIVE': {'data and information type': 'This is a non-sensitive data and information type'},
            'MODERATE_SENSITIVE': {
                'data and information type': 'This is a moderate-sensitive data and information type'
            },
            'HIGH_SENSITIVE': {'data and information type': 'This is a high-sensitive data and information type'},
            'SEVERE_SENSITIVE': {'data and information type': 'This is a severe-sensitive data and information type'},
        }
    },
}


@pytest.mark.parametrize(
    'prompt_name, version, context',
    [
        ('non_pii_detection', 'v0', MOCK_CONTEXT),
        ('pii_detection', 'v0', MOCK_CONTEXT),
        ('pii_reflection', 'v0', MOCK_CONTEXT),
    ],
)
def test_prompt_manager_get_prompt_parametrized(prompt_name, version, context):
    """Test prompt_manager.get_prompt."""
    pm = PromptManager()
    prompt = pm.get_prompt(prompt_name, version, context)

    assert prompt is not None
    assert isinstance(prompt, str)
    assert len(prompt) > 0
