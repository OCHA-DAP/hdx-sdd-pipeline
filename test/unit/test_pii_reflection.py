import pytest
from unittest.mock import patch, MagicMock


# --- Mock Azure Strategy (same as in the PIIClassifier tests) ---
class MockAzureOpenAIStrategy:
    """Mock to replace AzureOpenAIStrategy during CI/unit testing."""

    def __init__(self, model_name: str, azure_endpoint: str, api_key: str):
        self.model = model_name
        self.model_name = model_name
        self.azure_endpoint = azure_endpoint
        self.api_key = api_key
        self.client = MagicMock()

    def generate(self, _prompt: str, _temperature: float = 0.3, _max_new_tokens: int = 200):
        return 'mock_generated_text', 1, 1

    def generate_json(self, _prompt: str, _temperature: float = 0.3, _max_new_tokens: int = 200):
        return {'mock_key': 'mock_value'}, 1, 1

    def get_azure_config(self):
        return {'endpoint': 'mock_endpoint', 'model': self.model}


# Mock SDDReport and its behavior
class MockColumn:
    def __init__(self, name, entity_type=None, sensitive=None):
        self.column_name = name
        self.pii = {'entity_type': entity_type, 'sensitive': sensitive}


class MockReport:
    def __init__(self, columns=None):
        self.columns = columns or []
        self.completion_tokens = 0
        self.prompt_tokens = 0
        self.updated = []
        self.pii_reflection_model = None
        self.processing_success = True
        self.error_source = None
        self.error_message = None

    def update_pii_column(self, column_name, entity_type, sensitive):
        self.updated.append((column_name, entity_type, sensitive))

    def set_error(self, error_source: str, error_message: str):
        """Mock set_error method."""
        self.processing_success = False
        self.error_source = error_source
        self.error_message = error_message


@pytest.fixture
def classifier():
    """
    Fixture to create PIIReflectionClassifier with Azure strategy mocked out.
    """
    with patch('classifiers.base_classifier.AzureOpenAIStrategy', MockAzureOpenAIStrategy):
        from classifiers.pii_reflection_classifier import PIIReflectionClassifier

        c = PIIReflectionClassifier(model_name='mock_model', azure_endpoint='mock_endpoint', api_key='mock_api_key')

        # OPTIONAL:
        # Replace _run_prompt with a MagicMock so tests can patch it cleanly
        c._run_prompt = MagicMock()
        return c


def test_classify_column_none_entity(classifier):
    """Ensure classify_column returns NON_SENSITIVE for 'None' entity."""
    with patch.object(classifier, '_standardize_output', return_value={'mock': 'output'}) as mock_std:
        result = classifier.classify_column('col', 'table_md', 'None')
        mock_std.assert_called_once_with('PII_SENSITIVITY', 'NON_SENSITIVE', 'PII Entity = None')
        assert result == {'mock': 'output'}


def test_classify_column_success(classifier):
    """Ensure classify_column returns model prediction on success."""
    with patch.object(classifier, '_run_prompt', return_value=('SENSITIVE', 5, 10)) as mock_run:
        result = classifier.classify_column('col', 'table_md', 'EMAIL')
        mock_run.assert_called_once_with(
            'pii_reflection',
            {'column_name': 'col', 'table_markdown': 'table_md', 'column_entity': 'EMAIL'},
            'v0',
            12,
        )
        assert result == ('SENSITIVE', 5, 10)


@patch('classifiers.pii_reflection_classifier.logger')
def test_classify_column_error_generation_string(mock_logger, classifier):
    """Ensure classify_column raises RuntimeError when ERROR_GENERATION is in prediction."""
    with patch.object(classifier, '_run_prompt', return_value=('ERROR_GENERATION', 0, 0)):
        with pytest.raises(RuntimeError, match='Azure generation returned error'):
            classifier.classify_column('col', 'table_md', 'EMAIL')
        # Verify logger.error was called
        mock_logger.error.assert_called_once()
        error_call = mock_logger.error.call_args[0][0]
        assert 'Azure generation returned error' in error_call


@patch('classifiers.pii_reflection_classifier.logger')
def test_classify_column_exception(mock_logger, classifier):
    """Ensure classify_column raises RuntimeError on exceptions."""
    with patch.object(classifier, '_run_prompt', side_effect=RuntimeError('prompt failed')):
        with pytest.raises(RuntimeError, match='PII reflection classification failed'):
            classifier.classify_column('col', 'table_md', 'EMAIL')
        # Verify logger.exception was called
        mock_logger.exception.assert_called_once()
        error_call = mock_logger.exception.call_args[0][0]
        assert 'PII reflection classification failed' in error_call


def test_classify_df_skips_existing_sensitive(classifier):
    """Ensure classify_df skips columns already having sensitivity set."""
    mock_cols = [MockColumn('col1', 'EMAIL', sensitive=True)]
    report = MockReport(mock_cols)

    result = classifier.classify_df('table_md', report)
    assert report.updated == []
    assert result == report
    # Model is only set when processing columns, so if all are skipped, it won't be set
    # This is the current behavior - model is set inside the loop
    assert result.pii_reflection_model is None


def test_classify_df_stops_on_existing_error(classifier):
    """Ensure classify_df stops processing if report already has an error."""
    mock_cols = [MockColumn('col1', 'EMAIL', None)]
    report = MockReport(mock_cols)
    report.processing_success = False
    report.error_source = 'pii_classification'
    report.error_message = 'Previous error'

    result = classifier.classify_df('table_md', report)

    # Should return early without processing
    assert report.updated == []
    assert result.pii_reflection_model is None


def test_classify_df_error_entity_type(classifier):
    """Ensure classify_df sets sensitive=False for error or None entity types."""
    mock_cols = [
        MockColumn('col1', 'ERROR', None),
        MockColumn('col2', 'None', None),
    ]
    report = MockReport(mock_cols)

    result = classifier.classify_df('table_md', report)

    assert report.updated == [
        ('col1', 'ERROR', False),
        ('col2', 'None', False),
    ]
    assert result.pii_reflection_model == 'mock_model'


@pytest.mark.parametrize(
    'prediction,expected',
    [
        ('SENSITIVE', True),
        ('NON_SENSITIVE', False),
    ],
)
def test_classify_df_runs_prompt_and_updates(classifier, prediction, expected):
    """Ensure classify_df classifies and updates sensitivity correctly."""
    mock_cols = [MockColumn('col1', 'EMAIL', None)]
    report = MockReport(mock_cols)

    with patch.object(classifier, 'classify_column', return_value=(prediction, 2, 3)) as mock_classify:
        result = classifier.classify_df('table_md', report)

    mock_classify.assert_called_once()
    assert report.completion_tokens == 2
    assert report.prompt_tokens == 3
    assert report.updated == [('col1', 'EMAIL', expected)]
    assert result.pii_reflection_model == 'mock_model'


def test_classify_df_handles_prompt_failure(classifier):
    """Ensure classify_df handles classify_column raising RuntimeError."""
    from utils.error_constants import ERROR_SOURCE_PII_REFLECTION

    mock_cols = [MockColumn('col1', 'EMAIL', None)]
    report = MockReport(mock_cols)

    with patch.object(classifier, 'classify_column', side_effect=RuntimeError('prompt failed')):
        result = classifier.classify_df('table_md', report)

    # Verify error was set on report
    assert report.processing_success is False
    assert report.error_source == ERROR_SOURCE_PII_REFLECTION
    assert report.error_message is not None
    assert 'prompt failed' in report.error_message
    assert result.pii_reflection_model is None  # Model not set when error occurs
