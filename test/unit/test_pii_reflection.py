import pytest
import logging
from unittest.mock import patch, MagicMock


# --- Mock Azure Strategy (same as in the PIIClassifier tests) ---
class MockAzureOpenAIStrategy:
    """Mock to replace AzureOpenAIStrategy during CI/unit testing."""

    def __init__(self, model_name: str):
        self.model = model_name
        self.model_name = model_name
        self.client = MagicMock()

    def generate(self, prompt: str, temperature: float = 0.3, max_new_tokens: int = 200):
        return 'mock_generated_text', 1, 1

    def generate_json(self, prompt: str, temperature: float = 0.3, max_new_tokens: int = 200):
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

    def update_pii_column(self, column_name, entity_type, sensitive):
        self.updated.append((column_name, entity_type, sensitive))


@pytest.fixture
def classifier():
    """
    Fixture to create PIIReflectionClassifier with Azure strategy mocked out.
    """
    with patch('llm_model.azure_strategy.AzureOpenAIStrategy', MockAzureOpenAIStrategy):
        from classifiers.pii_reflection_classifier import PIIReflectionClassifier

        c = PIIReflectionClassifier(model_name='mock_model')

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


def test_classify_column_exception(classifier, caplog):
    """Ensure classify_column handles exceptions and logs them."""
    with patch.object(classifier, '_run_prompt', side_effect=RuntimeError('prompt failed')):
        with caplog.at_level(logging.ERROR):
            result = classifier.classify_column('col', 'table_md', 'EMAIL')
        assert result == (False, 0, 0)
        assert 'PII reflection classification failed' in caplog.text


def test_classify_df_skips_existing_sensitive(classifier):
    """Ensure classify_df skips columns already having sensitivity set."""
    mock_cols = [MockColumn('col1', 'EMAIL', sensitive=True)]
    report = MockReport(mock_cols)

    result = classifier.classify_df('table_md', report)
    assert report.updated == []
    assert result == report


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
    """Ensure classify_df handles classify_column returning False."""
    mock_cols = [MockColumn('col1', 'EMAIL', None)]
    report = MockReport(mock_cols)

    with patch.object(classifier, 'classify_column', return_value=(False, 0, 0)):
        result = classifier.classify_df('table_md', report)

    assert report.updated == [('col1', 'EMAIL', False)]
    assert result.pii_reflection_model == 'mock_model'
