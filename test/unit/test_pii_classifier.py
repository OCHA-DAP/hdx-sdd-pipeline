import pytest
import pandas as pd
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
    df = pd.DataFrame(
        {
            'name': ['Alice', 'Bob'],
            'age': ['10', '20'],
        }
    )

    pii_classifier_instance._has_alphanumeric.return_value = True
    pii_classifier_instance._run_prompt.return_value = ('name', 1, 2)

    predictions, comp, prompt, model_name = pii_classifier_instance.classify_df(df)

    assert len(predictions) == 2
    assert model_name == pii_classifier_instance.model.model_name
    assert comp == 2
    assert prompt == 4


def test_normalize_none_prediction(pii_classifier_instance, sample_df, mock_sdd_report):
    pii_classifier_instance._has_alphanumeric.return_value = True
    pii_classifier_instance._run_prompt.return_value = ('none', 1, 2)

    predictions, comp, prompt, model_name = pii_classifier_instance.classify_df(sample_df)
    assert len(predictions) == 3
    assert predictions[0].pii['entity_type'] == 'None'
    assert predictions[1].pii['entity_type'] == 'None'
    assert predictions[2].pii['entity_type'] == 'None'


def test_normalize_prediction(pii_classifier_instance, sample_df, mock_sdd_report):
    pii_classifier_instance._has_alphanumeric.return_value = True
    pii_classifier_instance._run_prompt.return_value = ('email_address', 1, 2)

    predictions, comp, prompt, model_name = pii_classifier_instance.classify_df(sample_df)
    assert len(predictions) == 3
    assert predictions[0].pii['entity_type'] == 'EMAIL_ADDRESS'
    assert predictions[1].pii['entity_type'] == 'EMAIL_ADDRESS'
    assert predictions[2].pii['entity_type'] == 'EMAIL_ADDRESS'
