# test/unit/test_non_pii_classifier.py

import pytest
from unittest.mock import patch, MagicMock

from classifiers.non_pii_classifier import NonPIIClassifier

from test.unit.conftest import MockBaseClassifier, MockAzureOpenAIStrategy
from utils.error_constants import ERROR_SOURCE_NON_PII_CLASSIFICATION
import utils.exception_handler

TEST_ISP = {'mock_isp': {'config': 'data'}}
TEST_TABLE = 'markdown_table_content'
TEST_MAX_TOKENS = 512
TEST_VERSION = 'v1'


@pytest.fixture
def non_pii_classifier_instance(mock_azure_strategy):
    with (
        patch('classifiers.non_pii_classifier.BaseClassifier', MockBaseClassifier),
        patch('classifiers.base_classifier.AzureOpenAIStrategy', MockAzureOpenAIStrategy),
    ):
        classifier = NonPIIClassifier(model=mock_azure_strategy)
        classifier._run_prompt = MagicMock()
        return classifier


# =====================================================================
# 1. format_prediction Tests
# =====================================================================


@pytest.mark.parametrize(
    'raw_pred, expected',
    [
        ('This is the high_sensitive result.', 'HIGH_SENSITIVE'),
        ('First line is moderate_sensitive\nsecond line is ignored', 'MODERATE_SENSITIVE'),
        ('non_sensitive is good to go.', 'NON_SENSITIVE'),
        ('Unknown classification level here.', 'UNDETERMINED'),
        ('\n\nhigh_sensitive but after newlines', 'UNDETERMINED'),
    ],
)
def test_format_prediction(non_pii_classifier_instance, raw_pred, expected):
    assert non_pii_classifier_instance.format_prediction(raw_pred) == expected


def test_format_prediction_only_first_line_is_considered(non_pii_classifier_instance):
    raw_pred = 'UNDETERMINED\n\nbut the second line contains: high_sensitive'
    assert non_pii_classifier_instance.format_prediction(raw_pred) == 'UNDETERMINED'


def test_classify(non_pii_classifier_instance, sample_report):
    non_pii_classifier_instance._run_prompt.return_value = (
        {
            'sensitivity': 'HIGH_SENSITIVE',
            'sensitive_columns': ['name'],
            'cited_isp_rules': ['name is sensitive'],
            'explanation': 'name is sensitive',
        },
        10,
        20,
    )
    non_pii_report = non_pii_classifier_instance.classify(sample_report, TEST_ISP)
    assert non_pii_report['non_personal_data']['sensitivity'] == 'HIGH_SENSITIVE'
    assert non_pii_report['non_personal_data']['sensitive_columns'] == ['name']
    assert non_pii_report['non_personal_data']['cited_isp_rules'] == ['name is sensitive']
    assert non_pii_report['non_personal_data']['explanation'] == 'name is sensitive'


def test_classify_non_dict_output(non_pii_classifier_instance, sample_report):
    non_pii_classifier_instance._run_prompt.return_value = ({'sensitive columns found'}, 10, 20)
    with pytest.raises(utils.exception_handler.ContextualError):
        report = non_pii_classifier_instance.classify(sample_report, TEST_ISP)
        assert report['error_source'] == ERROR_SOURCE_NON_PII_CLASSIFICATION
        assert report['error_message']
