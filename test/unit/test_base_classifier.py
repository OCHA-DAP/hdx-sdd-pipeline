import pytest
from unittest.mock import MagicMock, patch
from classifiers.base_classifier import BaseClassifier


# --------------------------------------------------------------
# FIXTURE: Create BaseClassifier instance with mocked dependencies
# --------------------------------------------------------------
@pytest.fixture
def classifier():
    with (
        patch('classifiers.base_classifier.AzureOpenAIStrategy') as mock_model_cls,
        patch('classifiers.base_classifier.PromptManager') as mock_prompt_cls,
    ):

        mock_model = MagicMock()
        mock_model.generate.return_value = ('PREDICTION_OK', 10, 5)
        mock_model_cls.return_value = mock_model

        mock_prompt = MagicMock()
        mock_prompt.get_prompt.return_value = 'Rendered prompt text'
        mock_prompt_cls.return_value = mock_prompt

        c = BaseClassifier(model_name='test-model')
        return c


# --------------------------------------------------------------
# TEST: _standardize_output()
# --------------------------------------------------------------
def test_standardize_output_trims_string():
    result = BaseClassifier._standardize_output(
        classification_type='pii',
        value='NAME',
        raw_model_output='  output text   ',
    )

    assert result['classification_type'] == 'pii'
    assert result['value'] == 'NAME'
    assert result['raw_model_output'] == 'output text'  # trimming verified


def test_standardize_output_keeps_non_string():
    raw_value = {'a': 1}
    result = BaseClassifier._standardize_output('pii', 'NAME', raw_value)

    assert result['raw_model_output'] == raw_value


# --------------------------------------------------------------
# TEST: _run_prompt()
# --------------------------------------------------------------
@patch('classifiers.base_classifier.DEBUG', False)
def test_run_prompt_successful_generation(classifier):
    result = classifier._run_prompt(prompt_name='test', context={'x': 1})

    assert result == ('PREDICTION_OK', 10, 5)
    classifier.prompt_manager.get_prompt.assert_called_once()
    classifier.model.generate.assert_called_once()


@patch('classifiers.base_classifier.DEBUG', True)
def test_run_prompt_debug_mode(classifier):
    result = classifier._run_prompt(prompt_name='test', context={})

    assert result == ('DEBUG_MODE', 0, 0)
    classifier.model.generate.assert_not_called()


def test_run_prompt_handles_prompt_generation_error(classifier):
    classifier.prompt_manager.get_prompt.side_effect = Exception('Template error')

    result = classifier._run_prompt(prompt_name='bad', context={})

    assert result == ('ERROR_GENERATION', 0, 0)


# --------------------------------------------------------------
# TEST: _map_sensitivity()
# --------------------------------------------------------------
@pytest.mark.parametrize(
    'input_text, expected',
    [
        ('This is high_sensitive output', 'HIGH_SENSITIVE'),
        ('Medium_Sensitive risk detected', 'MEDIUM_SENSITIVE'),
        ('User is non_sensitive', 'NON_SENSITIVE'),
        ('UNKNOWN VALUE', 'UNDETERMINED'),
    ],
)
def test_map_sensitivity_mapping(classifier, input_text, expected):
    assert classifier._map_sensitivity(input_text) == expected


# --------------------------------------------------------------
# TEST: _has_alphanumeric()
# --------------------------------------------------------------
def test_has_alphanumeric_with_letters():
    assert BaseClassifier._has_alphanumeric(['hello', 'world']) is True


def test_has_alphanumeric_with_digits():
    assert BaseClassifier._has_alphanumeric(['123', '456']) is True


def test_has_alphanumeric_with_mixed_alphanumeric():
    assert BaseClassifier._has_alphanumeric(['abc123', 'test']) is True


def test_has_alphanumeric_with_only_special_chars():
    assert BaseClassifier._has_alphanumeric(['!!!', '---', '@@@']) is False


def test_has_alphanumeric_with_empty_list():
    assert BaseClassifier._has_alphanumeric([]) is False


def test_has_alphanumeric_with_whitespace_only():
    assert BaseClassifier._has_alphanumeric(['   ', '\n\t', '']) is False


def test_has_alphanumeric_with_numeric_values():
    assert BaseClassifier._has_alphanumeric([123, 456]) is True


def test_has_alphanumeric_mixed_list():
    assert BaseClassifier._has_alphanumeric(['!!!', 'abc', '---']) is True
