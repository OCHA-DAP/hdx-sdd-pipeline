import pytest
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
        classifier.model_name = 'mock-model'
        return classifier


def test_classify_df(pii_reflection_classifier_instance, sample_report):
    pii_reflection_classifier_instance._run_prompt.return_value = ('SENSITIVE', 10, 20)
    sdd_report = pii_reflection_classifier_instance.classify_df(sample_report)

    assert len(sdd_report['columns']) == 3
    assert sdd_report['completion_tokens'] == 30
    assert sdd_report['prompt_tokens'] == 60

    pii_reflection_classifier_instance._run_prompt.return_value = ('NON_SENSITIVE', 10, 20)
    sdd_report = pii_reflection_classifier_instance.classify_df(sample_report)
    assert sdd_report['columns'][0]['personal_data']['sensitive'] == False
    assert sdd_report['columns'][1]['personal_data']['sensitive'] == False
    assert sdd_report['columns'][2]['personal_data']['sensitive'] == False


def test_classify_df_none_column(pii_reflection_classifier_instance, sample_report):
    sample_report['columns'][0]['personal_data']['entity_type'] = 'None'
    pii_reflection_classifier_instance._run_prompt.return_value = ('SENSITIVE', 10, 20)
    sdd_report = pii_reflection_classifier_instance.classify_df(sample_report)
    assert sdd_report['columns'][0]['personal_data']['sensitive'] == False
    assert sdd_report['columns'][1]['personal_data']['sensitive'] == True
    assert sdd_report['columns'][2]['personal_data']['sensitive'] == True
    assert sdd_report['completion_tokens'] == 20
    assert sdd_report['prompt_tokens'] == 40


def test_classify_column_none_column(pii_reflection_classifier_instance):
    prediction, comp, prompt = pii_reflection_classifier_instance.classify_column('test', 'tablemarkdown', 'None')
    assert prediction == 'NON_SENSITIVE'
    assert comp == 0
    assert prompt == 0
