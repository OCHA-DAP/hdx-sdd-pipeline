import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from classifiers.pii_reflection_classifier import PIIReflectionClassifier

from test.unit.conftest import MockBaseClassifier, MockAzureOpenAIStrategy


@pytest.fixture
def pii_reflection_classifier_instance(mock_azure_strategy):
    with (
        patch('classifiers.pii_reflection_classifier.BaseClassifier', MockBaseClassifier),
        patch('classifiers.base_classifier.AzureOpenAIStrategy', MockAzureOpenAIStrategy),
    ):
        classifier = PIIReflectionClassifier(model=mock_azure_strategy)
        classifier._run_prompt = MagicMock()
        classifier.model_name = "mock-model"
        return classifier


def test_classify_df(pii_reflection_classifier_instance, sample_report, sample_table_markdown):

    pii_reflection_classifier_instance._run_prompt.return_value = ('SENSITIVE', 10, 20)
    predictions, comp, prompt, model_name = pii_reflection_classifier_instance.classify_df(
        ' test table markdown', sample_report.columns
    )

    assert len(predictions) == 3
    assert model_name == pii_reflection_classifier_instance.model.model_name
    assert comp == 30
    assert prompt == 60

    pii_reflection_classifier_instance._run_prompt.return_value = ('NON_SENSITIVE', 10, 20)
    predictions, comp, prompt, model_name = pii_reflection_classifier_instance.classify_df(
        sample_table_markdown, sample_report.columns
    )
    assert predictions[0].pii['sensitive'] == False
    assert predictions[1].pii['sensitive'] == False
    assert predictions[2].pii['sensitive'] == False


def test_classify_df_none_column(pii_reflection_classifier_instance, sample_report, sample_table_markdown):
    sample_report.columns[0].pii['entity_type'] = 'None'
    pii_reflection_classifier_instance._run_prompt.return_value = ('SENSITIVE', 10, 20)
    predictions, comp, prompt, model_name = pii_reflection_classifier_instance.classify_df(
        sample_table_markdown, sample_report.columns
    )
    assert predictions[0].pii['sensitive'] == False
    assert predictions[1].pii['sensitive'] == True
    assert predictions[2].pii['sensitive'] == True
    assert comp == 20
    assert prompt == 40


def test_classify_column_none_column(pii_reflection_classifier_instance):
    prediction, comp, prompt = pii_reflection_classifier_instance.classify_column('test', "tablemarkdown", "None")
    assert prediction == 'NON_SENSITIVE'
    assert comp == 0
    assert prompt == 0
