# test/unit/test_non_pii_classifier.py

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from classifiers.readme_scan import ReadMeScanClassifier

from test.unit.conftest import MockBaseClassifier, MockAzureOpenAIStrategy

TEST_TABLE = 'markdown_table_content'
TEST_MAX_TOKENS = 512
TEST_VERSION = 'v1'


@pytest.fixture
def readme_scan_classifier_instance(mock_azure_strategy):
    with (
        patch('classifiers.non_pii_classifier.BaseClassifier', MockBaseClassifier),
        patch('classifiers.base_classifier.AzureOpenAIStrategy', MockAzureOpenAIStrategy),
    ):
        classifier = ReadMeScanClassifier(model=mock_azure_strategy)
        classifier._run_prompt = MagicMock()
        return classifier


def test_classify_readme(readme_scan_classifier_instance):
    readme_string = 'This is a test readme string with PII information.'
    readme_scan_classifier_instance._run_prompt.return_value = (
        {
            'contains_pii': True,
            'pii_types': ['email', 'phone number'],
            'evidence': ['This is a test readme string with PII information.'],
        },
        1,
        1,
    )
    prediction, completion_tokens, prompt_tokens = readme_scan_classifier_instance.classify(readme_string)
    assert prediction == {
        'contains_pii': True,
        'pii_types': ['email', 'phone number'],
        'evidence': ['This is a test readme string with PII information.'],
    }
    assert completion_tokens == 1
    assert prompt_tokens == 1


def test_classify_readme_error(readme_scan_classifier_instance):
    readme_string = 'This is a test readme string with PII information.'
    readme_scan_classifier_instance._run_prompt.side_effect = Exception('Test error')
    with pytest.raises(Exception):
        readme_scan_classifier_instance.classify(readme_string)
