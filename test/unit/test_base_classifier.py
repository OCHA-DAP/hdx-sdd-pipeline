import pytest
from unittest.mock import patch
from typing import Dict, Any
from classifiers.base_classifier import BaseClassifier


class MockPromptManager:
    """Mock PromptManager class."""

    def __init__(self):
        pass

    def get_prompt(self, prompt_name: str, version: str, context: Dict[str, Any]) -> str:
        if prompt_name == 'error_prompt':
            raise Exception('Mock Prompt Manager Error')
        return f'Rendered prompt for {prompt_name}'


class MockAzureOpenAIStrategy:
    """Mock AzureOpenAIStrategy class with generate methods."""

    def __init__(self, model_name: str, azure_endpoint: str, api_key: str):
        self.model_name = model_name
        self.azure_endpoint = azure_endpoint
        self.api_key = api_key

    def generate(self, prompt: str, max_new_tokens: int) -> tuple:
        """Simulates non-JSON generation."""
        return 'Non-JSON Prediction', 10, 50  # prediction, completion_tokens, prompt_tokens

    def generate_json(self, prompt: str, max_new_tokens: int) -> tuple:
        """Simulates JSON generation."""
        return '{"result": "JSON Prediction"}', 20, 60


# --- Fixture for BaseClassifier ---


@pytest.fixture
def base_classifier_instance():
    """Fixture to create a BaseClassifier instance with mocked dependencies."""

    # Define the full import paths used in base_classifier.py for patching
    AZURE_OPENAI_PATH = 'classifiers.base_classifier.AzureOpenAIStrategy'
    PROMPT_MANAGER_PATH = 'classifiers.base_classifier.PromptManager'

    # Patch the external classes that BaseClassifier instantiates
    with patch(PROMPT_MANAGER_PATH, MockPromptManager), patch(AZURE_OPENAI_PATH, MockAzureOpenAIStrategy):
        # Instantiate BaseClassifier (it will use the mocked classes)
        classifier = BaseClassifier(model_name='mock_model', azure_endpoint='mock_endpoint', api_key='mock_api_key')
        yield classifier


# --- Mocking DEBUG constant for specific tests ---
DEBUG_CONFIG_PATH = 'classifiers.base_classifier.DEBUG'


# =====================================================================
# 1. __init__ Tests
# =====================================================================


def test_base_classifier_init(base_classifier_instance):
    """Test successful initialization and attribute assignment."""
    classifier = base_classifier_instance
    assert classifier.model_name == 'mock_model'
    assert isinstance(classifier.prompt_manager, MockPromptManager)
    assert isinstance(classifier.model, MockAzureOpenAIStrategy)
    assert classifier.model.model_name == 'mock_model'


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


@patch(DEBUG_CONFIG_PATH, False)  # Ensure DEBUG mode is off for real runs
def test_run_prompt_non_json_success(base_classifier_instance):
    """Test successful non-JSON generation (calls model.generate)."""

    prediction, comp_tokens, prompt_tokens = base_classifier_instance._run_prompt(
        prompt_name='test_prompt', context={'data': 123}, json_response_format=False
    )

    assert prediction == 'Non-JSON Prediction'
    assert comp_tokens == 10
    assert prompt_tokens == 50


@patch(DEBUG_CONFIG_PATH, False)  # Ensure DEBUG mode is off for real runs
def test_run_prompt_json_success(base_classifier_instance):
    """Test successful JSON generation (calls model.generate_json)."""

    prediction, comp_tokens, prompt_tokens = base_classifier_instance._run_prompt(
        prompt_name='test_prompt', context={'data': 123}, json_response_format=True
    )

    assert prediction == '{"result": "JSON Prediction"}'
    assert comp_tokens == 20
    assert prompt_tokens == 60


@patch(DEBUG_CONFIG_PATH, True)  # Force DEBUG mode on
def test_run_prompt_debug_mode(base_classifier_instance):
    """Test when the DEBUG flag is True (short-circuits model call)."""

    prediction, comp_tokens, prompt_tokens = base_classifier_instance._run_prompt(
        prompt_name='test_prompt', context={'data': 123}
    )

    assert prediction == 'DEBUG_MODE'
    assert comp_tokens == 0
    assert prompt_tokens == 0


@patch(DEBUG_CONFIG_PATH, False)  # Ensure DEBUG mode is off
def test_run_prompt_prompt_manager_error(base_classifier_instance):
    """Test exception handling during prompt rendering raises RuntimeError."""

    with pytest.raises(RuntimeError, match='Prompt rendering failed'):
        base_classifier_instance._run_prompt(
            prompt_name='error_prompt',
            context={},  # MockPromptManager raises exception for this name
        )


# =====================================================================
# 4. _map_sensitivity Tests
# =====================================================================


def test_map_sensitivity_non_sensitive(base_classifier_instance):
    """Test mapping to NON_SENSITIVE."""
    assert base_classifier_instance._map_sensitivity('This data is non_sensitive.') == 'NON_SENSITIVE'


def test_map_sensitivity_medium_sensitive(base_classifier_instance):
    """Test mapping to MEDIUM_SENSITIVE."""
    assert base_classifier_instance._map_sensitivity('Result: MEDIUM_sensitive') == 'MEDIUM_SENSITIVE'


def test_map_sensitivity_moderate_sensitive(base_classifier_instance):
    """Test mapping to MODERATE_SENSITIVE."""
    assert base_classifier_instance._map_sensitivity('moderate_sensitive') == 'MODERATE_SENSITIVE'


def test_map_sensitivity_high_sensitive(base_classifier_instance):
    """Test mapping to HIGH_SENSITIVE."""
    assert base_classifier_instance._map_sensitivity('high_sensitive') == 'HIGH_SENSITIVE'


def test_map_sensitivity_severe_sensitive(base_classifier_instance):
    """Test mapping to SEVERE_SENSITIVE."""
    assert base_classifier_instance._map_sensitivity('Alert: severe_sensitive information found.') == 'SEVERE_SENSITIVE'


def test_map_sensitivity_undetermined(base_classifier_instance):
    """Test mapping to UNDETERMINED if no keyword is found."""
    assert base_classifier_instance._map_sensitivity('This is some random text.') == 'UNDETERMINED'


# =====================================================================
# 5. _has_alphanumeric Tests
# =====================================================================


def test_has_alphanumeric_with_letters_and_digits():
    """Test a list containing both letters and digits."""
    values = ['abc', '123', '$%^']
    assert BaseClassifier._has_alphanumeric(values) is True


def test_has_alphanumeric_with_mixed_types():
    """Test a list containing mixed types, including numbers and bools."""
    values = [42, 'hello', True]
    assert BaseClassifier._has_alphanumeric(values) is True


def test_has_alphanumeric_only_digits():
    """Test a list containing only numeric values (digits)."""
    values = ['1', 2, '3.0']
    assert BaseClassifier._has_alphanumeric(values) is True


def test_has_alphanumeric_only_punctuation_and_whitespace():
    """Test a list containing only non-alphanumeric characters."""
    # The check includes 'None' which converts to string "None", which has letters
    assert BaseClassifier._has_alphanumeric(['!', '$%^', ' ', '  \t\n', '...']) is False


def test_has_alphanumeric_empty_list():
    """Test an empty list."""
    assert BaseClassifier._has_alphanumeric([]) is False
