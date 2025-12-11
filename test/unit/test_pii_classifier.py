import pytest
from unittest.mock import patch, MagicMock

from classifiers.pii_classifier import PIIClassifier
from test.unit.conftest import MockBaseClassifier, MockAzureOpenAIStrategy

MOCK_PII_ENTITIES = ['NAME', 'SSN', 'EMAIL', 'IP ADDRESS', 'AGE']


@pytest.fixture
def pii_classifier_instance(mock_azure_strategy):
    with (
        patch('classifiers.pii_classifier.BaseClassifier', MockBaseClassifier),
        patch('classifiers.base_classifier.AzureOpenAIStrategy', MockAzureOpenAIStrategy),
        patch('classifiers.pii_classifier.PII_ENTITIES_LIST', MOCK_PII_ENTITIES),
    ):
        classifier = PIIClassifier(model=mock_azure_strategy)
        classifier._run_prompt = MagicMock()
        classifier._has_alphanumeric = MagicMock()
        return classifier


# def test_prepare_context(pii_classifier_instance, sample_df):
#     context = pii_classifier_instance._prepare_context(sample_df)
#     assert len(context) == 3
#     assert context[0]['name'] == 'Alice'
#     assert context[0]['age'] == 25
#     assert context[0]['country'] == 'US'
#     assert context[1]['name'] == 'Bob'
#     assert context[1]['age'] == 30
#     assert context[1]['country'] == 'UK'
#     assert context[2]['name'] == 'Charlie'
#     assert context[2]['age'] == 35
#     assert context[2]['country'] == 'DE'


def test_classify_df(pii_classifier_instance):
    mock_sdd_report = {
        'completion_tokens': 0,
        'prompt_tokens': 0,
        'columns': [
            {'column_name': 'name', 'sample_values': ['Alice', 'Bob']},
            {'column_name': 'age', 'sample_values': ['10', '20']},
        ],
    }

    pii_classifier_instance._has_alphanumeric.return_value = True
    pii_classifier_instance._run_prompt.return_value = ('name', 1, 2)

    sdd_report = pii_classifier_instance.classify_df(mock_sdd_report)

    assert len(sdd_report['columns']) == 2
    assert sdd_report['completion_tokens'] == 2
    assert sdd_report['prompt_tokens'] == 4


def test_normalize_none_prediction(pii_classifier_instance, mock_sdd_report):
    pii_classifier_instance._has_alphanumeric.return_value = True
    pii_classifier_instance._run_prompt.return_value = ('none', 1, 2)

    sdd_report = pii_classifier_instance.classify_df(mock_sdd_report)
    assert len(sdd_report['columns']) == 3
    assert sdd_report['columns'][0]['pii']['entity_type'] == 'None'
    assert sdd_report['columns'][1]['pii']['entity_type'] == 'None'
    assert sdd_report['columns'][2]['pii']['entity_type'] == 'None'


def test_normalize_prediction(pii_classifier_instance, sample_df, mock_sdd_report):
    pii_classifier_instance._has_alphanumeric.return_value = True
    pii_classifier_instance._run_prompt.return_value = ('email_address', 1, 2)

    sdd_report = pii_classifier_instance.classify_df(mock_sdd_report)
    assert len(sdd_report['columns']) == 3
    assert sdd_report['columns'][0]['pii']['entity_type'] == 'EMAIL_ADDRESS'
    assert sdd_report['columns'][1]['pii']['entity_type'] == 'EMAIL_ADDRESS'
    assert sdd_report['columns'][2]['pii']['entity_type'] == 'EMAIL_ADDRESS'
