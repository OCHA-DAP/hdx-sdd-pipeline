import pytest
from unittest.mock import patch
from classifiers.base_classifier import BaseClassifier
from test.unit.conftest import MockAzureOpenAIStrategy
from utils.prompt_manager import PromptManager

# --- Fixture for BaseClassifier ---


@pytest.fixture
def base_classifier_instance(mock_azure_strategy):
    with (
        patch('classifiers.base_classifier.AzureOpenAIStrategy', MockAzureOpenAIStrategy),
    ):
        classifier = BaseClassifier(model=mock_azure_strategy)
        return classifier


# =====================================================================
# 1. __init__ Tests
# =====================================================================


def test_base_classifier_init(base_classifier_instance):
    """Test successful initialization and attribute assignment."""
    classifier = base_classifier_instance
    assert classifier.model_name == 'mock-model'
    assert isinstance(classifier.prompt_manager, PromptManager)
    assert isinstance(classifier.model, MockAzureOpenAIStrategy)


# =====================================================================
# 2. _standardize_output Tests
# =====================================================================


def test_standardize_output_string_raw(base_classifier_instance):
    """Test output standardization with a string raw model output."""
    raw_output = '   Sensitive Result \n '
    standardized = base_classifier_instance._standardize_output(
        classification_type='Pii', value='Found', raw_model_output=raw_output
    )
    assert standardized == {
        'classification_type': 'Pii',
        'value': 'Found',
        'raw_model_output': 'Sensitive Result',  # Should be stripped
    }


def test_standardize_output_non_string_raw(base_classifier_instance):
    """Test output standardization with a non-string raw model output (e.g., dict)."""
    raw_output = {'json': 'data'}
    standardized = base_classifier_instance._standardize_output(
        classification_type='Topic', value='Finance', raw_model_output=raw_output
    )
    assert standardized == {
        'classification_type': 'Topic',
        'value': 'Finance',
        'raw_model_output': {'json': 'data'},  # Should not be stripped
    }


# =====================================================================
# 3. _run_prompt Tests
# =====================================================================


def test_run_prompt_non_json_success(base_classifier_instance):
    """Test successful non-JSON generation (calls model.generate)."""

    prediction, comp_tokens, prompt_tokens = base_classifier_instance._run_prompt(
        prompt_name='pii_detection', context={'data': 123}, json_response_format=False
    )

    assert prediction == 'mock_generated_text'
    assert comp_tokens == 1
    assert prompt_tokens == 1


def test_run_prompt_json_success(base_classifier_instance, mock_isp):
    """Test successful JSON generation (calls model.generate_json)."""

    prediction, comp_tokens, prompt_tokens = base_classifier_instance._run_prompt(
        prompt_name='non_pii_detection', context={'data': 123, 'isp': mock_isp}, json_response_format=True
    )

    assert prediction == {'mock_key': 'mock_value'}
    assert comp_tokens == 1
    assert prompt_tokens == 1


def test_map_sensitivity(base_classifier_instance):
    """Test sensitivity mapping."""
    sensitivity = base_classifier_instance._map_sensitivity('HIGH_SENSITIVE')
    assert sensitivity == 'HIGH_SENSITIVE'

    sensitivity = base_classifier_instance._map_sensitivity('mOderate_SENSITIVE')
    assert sensitivity == 'MODERATE_SENSITIVE'

    sensitivity = base_classifier_instance._map_sensitivity('LOW/NON_SENSITIVE')
    assert sensitivity == 'NON_SENSITIVE'

    sensitivity = base_classifier_instance._map_sensitivity('severe_sensitive')
    assert sensitivity == 'SEVERE_SENSITIVE'

    sensitivity = base_classifier_instance._map_sensitivity('unknown')
    assert sensitivity == 'UNDETERMINED'
