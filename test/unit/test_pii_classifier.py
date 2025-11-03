# tests/test_pii_classifier.py
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from classifiers.pii_classifier import PIIClassifier
from models.sdd_report import SDDReport
import os

os.environ['AZURE_OPENAI_API_KEY'] = 'your-azure-api-key'
os.environ['AZURE_OPENAI_API_BASE'] = 'https://your-resource-name.openai.azure.com/'
os.environ['AZURE_OPENAI_API_VERSION'] = '2023-07-01-preview'


# ------------------------------------------------------------
# FIXTURES
# ------------------------------------------------------------
@pytest.fixture
def mock_report():
    """Neutral SDDReport (no PII columns yet)."""
    report = SDDReport(
        resource_id='1',
        file_name='file.csv',
        file_url='http://example.com',
        sheet_name='Sheet1',
        processing_timestamp='now',
        processing_success=True,
        n_records=10,
        n_columns=2,
    )
    report.completion_tokens = 0
    report.prompt_tokens = 0
    return report


@pytest.fixture
def classifier():
    """Return PIIClassifier with model + prompt_manager mocked."""
    with patch('utils.prompt_manager.PromptManager'), patch('classifiers.pii_classifier.PIIClassifier'):
        return PIIClassifier(model_name='test-model')


# ------------------------------------------------------------
# TEST: _prepare_context()
# ------------------------------------------------------------
def test_prepare_context_returns_dict_records(classifier):
    df = pd.DataFrame({'name': ['Alice'], 'age': ['20']})
    context = classifier._prepare_context(df)

    assert isinstance(context, list)
    assert context == [{'name': 'Alice', 'age': '20'}]


# ------------------------------------------------------------
# TEST: classify_column requires report
# ------------------------------------------------------------
def test_classify_column_no_report_raises(classifier):
    with pytest.raises(ValueError):
        classifier._classify_column('column', ['value'], report=None)


# ------------------------------------------------------------
# TEST: classify_column handles non-alphanumeric values → entity=None
# ------------------------------------------------------------
def test_classify_column_no_alphanumeric(classifier, mock_report):
    classifier._has_alphanumeric = MagicMock(return_value=False)

    classifier._classify_column('colA', ['!!!', '---'], report=mock_report)

    assert len(mock_report.columns) == 1
    assert mock_report.columns[0].pii['entity_type'] == 'None'
    assert mock_report.columns[0].column_name == 'colA'


# ------------------------------------------------------------
# TEST: classify_column triggers model + updates report
# ------------------------------------------------------------
@patch('utils.main_config.PII_ENTITIES_LIST', ['NAME', 'AGE'])
def test_classify_column_runs_prompt_and_updates_report(classifier, mock_report):
    classifier._has_alphanumeric = MagicMock(return_value=True)

    classifier._run_prompt = MagicMock(return_value=('NAME detected', 7, 2))

    classifier._classify_column('User', ['Alice', 'Bob'], report=mock_report)

    classifier._run_prompt.assert_called_once()

    assert len(mock_report.columns) == 1
    assert mock_report.columns[0].pii['entity_type'] == 'UNDETERMINED'
    assert mock_report.completion_tokens == 7
    assert mock_report.prompt_tokens == 2


# ------------------------------------------------------------
# TEST: if exception → PII entity marked as ERROR
# ------------------------------------------------------------
def test_classify_column_handles_exception(classifier, mock_report):
    classifier._has_alphanumeric = MagicMock(return_value=True)

    classifier._run_prompt = MagicMock(side_effect=Exception('model crashed'))

    classifier._classify_column('column', ['value1'], report=mock_report)

    assert mock_report.columns[0].pii['entity_type'] == 'ERROR'


# ------------------------------------------------------------
# TEST: classify_df iterates through each column
# ------------------------------------------------------------
def test_classify_df_processes_each_column(classifier, mock_report):
    df = pd.DataFrame({'col1': ['alice'], 'col2': ['bob']})

    classifier._classify_column = MagicMock()

    result = classifier.classify_df(df, mock_report)

    assert classifier._classify_column.call_count == 2
    assert result.pii_classifier_model == 'test-model'


# ------------------------------------------------------------
# TEST: classify_column detects 'none' in prediction → entity_type='None'
# ------------------------------------------------------------
def test_classify_column_detects_none_lowercase(classifier, mock_report):
    """Test that 'none' in prediction (lowercase) sets entity_type to 'None'."""
    classifier._has_alphanumeric = MagicMock(return_value=True)
    classifier._run_prompt = MagicMock(return_value=('none detected', 5, 3))

    classifier._classify_column('column', ['value1'], report=mock_report)

    assert mock_report.columns[0].pii['entity_type'] == 'None'


def test_classify_column_detects_none_uppercase(classifier, mock_report):
    """Test that 'NONE' in prediction (uppercase) sets entity_type to 'None'."""
    classifier._has_alphanumeric = MagicMock(return_value=True)
    classifier._run_prompt = MagicMock(return_value=('NONE found', 5, 3))

    classifier._classify_column('column', ['value1'], report=mock_report)

    assert mock_report.columns[0].pii['entity_type'] == 'None'


def test_classify_column_detects_none_mixed_case(classifier, mock_report):
    """Test that 'NoNe' in prediction (mixed case) sets entity_type to 'None'."""
    classifier._has_alphanumeric = MagicMock(return_value=True)
    classifier._run_prompt = MagicMock(return_value=('NoNe in text', 5, 3))

    classifier._classify_column('column', ['value1'], report=mock_report)

    assert mock_report.columns[0].pii['entity_type'] == 'None'


# ------------------------------------------------------------
# TEST: classify_column matches entity from PII_ENTITIES_LIST
# ------------------------------------------------------------
@patch('utils.main_config.PII_ENTITIES_LIST', ['PERSON_NAME', 'AGE', 'EMAIL_ADDRESS'])
def test_classify_column_matches_person_name_entity(classifier, mock_report):
    """Test that prediction containing 'person_name' sets entity_type to 'PERSON_NAME'."""
    classifier._has_alphanumeric = MagicMock(return_value=True)
    classifier._run_prompt = MagicMock(return_value=('person_name detected', 5, 3))

    classifier._classify_column('column', ['value1'], report=mock_report)

    assert mock_report.columns[0].pii['entity_type'] == 'PERSON_NAME'


@patch('utils.main_config.PII_ENTITIES_LIST', ['PERSON_NAME', 'AGE', 'EMAIL_ADDRESS'])
def test_classify_column_matches_age_entity(classifier, mock_report):
    """Test that prediction containing 'age' sets entity_type to 'AGE' (AGE is prioritized last)."""
    classifier._has_alphanumeric = MagicMock(return_value=True)
    classifier._run_prompt = MagicMock(return_value=('age information', 5, 3))

    classifier._classify_column('column', ['value1'], report=mock_report)

    assert mock_report.columns[0].pii['entity_type'] == 'AGE'


@patch('utils.main_config.PII_ENTITIES_LIST', ['PERSON_NAME', 'AGE', 'EMAIL_ADDRESS'])
def test_classify_column_matches_email_entity_uppercase(classifier, mock_report):
    """Test that prediction containing entity in uppercase still matches."""
    classifier._has_alphanumeric = MagicMock(return_value=True)
    classifier._run_prompt = MagicMock(return_value=('EMAIL_ADDRESS found', 5, 3))

    classifier._classify_column('column', ['value1'], report=mock_report)

    assert mock_report.columns[0].pii['entity_type'] == 'EMAIL_ADDRESS'


@patch('utils.main_config.PII_ENTITIES_LIST', ['PERSON_NAME', 'AGE', 'EMAIL_ADDRESS'])
def test_classify_column_matches_first_entity_when_multiple_present(classifier, mock_report):
    """Test that when multiple entities are in prediction, first one in entity_list matches."""
    classifier._has_alphanumeric = MagicMock(return_value=True)
    classifier._run_prompt = MagicMock(return_value=('email_address and person_name', 5, 3))

    classifier._classify_column('column', ['value1'], report=mock_report)

    # PERSON_NAME should match first (comes before EMAIL_ADDRESS in entity_list after AGE is moved to end)
    assert mock_report.columns[0].pii['entity_type'] == 'PERSON_NAME'
