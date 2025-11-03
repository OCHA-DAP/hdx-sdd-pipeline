# tests/test_pii_reflection_classifier.py
import pytest
from unittest.mock import MagicMock, patch

from classifiers.pii_reflection_classifier import PIIReflectionClassifier
from models.sdd_report import SDDReport, PIIColumnReport
import os

os.environ['AZURE_OPENAI_API_KEY'] = 'your-azure-api-key'
os.environ['AZURE_OPENAI_API_BASE'] = 'https://your-resource-name.openai.azure.com/'
os.environ['AZURE_OPENAI_API_VERSION'] = '2023-07-01-preview'


# ------------------------------------------------------------
# FIXTURES
# ------------------------------------------------------------
@pytest.fixture
def mock_report():
    """Minimal SDDReport instance with columns for reflection."""
    columns = [
        PIIColumnReport(column_name='col1', sample_values=['val1'], pii={'entity_type': 'NAME'}),
        PIIColumnReport(column_name='col2', sample_values=['val2'], pii={'entity_type': 'None'}),
        PIIColumnReport(column_name='col3', sample_values=['val3'], pii={'entity_type': 'ERROR'}),
    ]
    report = SDDReport(
        resource_id='1',
        file_name='file.csv',
        file_url='http://example.com',
        sheet_name='Sheet1',
        processing_timestamp='now',
        processing_success=True,
        n_records=3,
        n_columns=3,
        columns=columns,
    )
    report.completion_tokens = 0
    report.prompt_tokens = 0
    return report


@pytest.fixture
def classifier():
    """Return PIIReflectionClassifier with model mocked."""
    with patch('classifiers.pii_reflection_classifier.PIIReflectionClassifier'), patch(
        'utils.prompt_manager.PromptManager'
    ):
        return PIIReflectionClassifier(model_name='test-model')


# ------------------------------------------------------------
# TEST: classify_column returns NON_SENSITIVE if entity is None
# ------------------------------------------------------------
def test_classify_column_none_entity(classifier):
    result = classifier.classify_column('col', 'table_md', 'None')
    assert isinstance(result, dict)
    assert result['value'] == 'NON_SENSITIVE'


# ------------------------------------------------------------
# TEST: classify_column runs model and returns tuple
# ------------------------------------------------------------
def test_classify_column_runs_prompt(classifier):
    classifier._run_prompt = MagicMock(return_value=('SENSITIVE', 5, 2))

    pred, comp_tokens, prompt_tokens = classifier.classify_column('col1', 'table_md', 'NAME')

    classifier._run_prompt.assert_called_once()
    assert pred == 'SENSITIVE'
    assert comp_tokens == 5
    assert prompt_tokens == 2


# ------------------------------------------------------------
# TEST: classify_column handles exception
# ------------------------------------------------------------
def test_classify_column_handles_exception(classifier):
    classifier._run_prompt = MagicMock(side_effect=Exception('model crashed'))

    pred, comp_tokens, prompt_tokens = classifier.classify_column('col1', 'table_md', 'NAME')

    assert pred is False
    assert comp_tokens == 0
    assert prompt_tokens == 0


# ------------------------------------------------------------
# TEST: classify_df skips None and ERROR columns, updates others
# ------------------------------------------------------------
def test_classify_df_updates_report(classifier, mock_report):
    # Patch classify_column to return controlled outputs
    classifier.classify_column = MagicMock(side_effect=[('SENSITIVE', 2, 1)])

    updated_report = classifier.classify_df('table_md', mock_report)

    # Only one column should have been updated (col1)
    col1 = updated_report.columns[0]
    col2 = updated_report.columns[1]
    col3 = updated_report.columns[2]

    assert col1.pii['sensitive'] is True  # SENSITIVE → True
    assert col2.pii['sensitive'] is False  # None entity → skipped
    assert col3.pii['sensitive'] is False  # ERROR → skipped

    # Token counters updated
    assert updated_report.completion_tokens == 2
    assert updated_report.prompt_tokens == 1

    # Model name set
    assert updated_report.pii_reflection_model == 'test-model'


# ------------------------------------------------------------
# TEST: classify_df NON_SENSITIVE mapping
# ------------------------------------------------------------
def test_classify_df_non_sensitive(classifier):
    report = SDDReport(
        resource_id='1',
        file_name='file.csv',
        file_url='url',
        processing_timestamp='now',
        processing_success=True,
        n_records=1,
        n_columns=1,
        columns=[PIIColumnReport(column_name='c', sample_values=['v'], pii={'entity_type': 'EMAIL'})],
    )

    classifier.classify_column = MagicMock(return_value=('NON_SENSITIVE', 1, 1))
    updated_report = classifier.classify_df('table_md', report)

    assert updated_report.columns[0].pii['sensitive'] is False
    assert updated_report.completion_tokens == 1
    assert updated_report.prompt_tokens == 1
