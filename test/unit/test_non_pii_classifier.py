# test/unit/test_non_pii_classifier.py

import pytest
from unittest.mock import patch, MagicMock
from typing import Any, Optional

from classifiers.non_pii_classifier import NonPIIClassifier

# --- Mock Classes for Dependencies ---


class MockBaseClassifier:
    def __init__(self, model_name: str):
        self.model_name = model_name


class MockNonPIIReport:
    def __init__(self, model_name: str, isp_used: str, sensitivity: str, explanation: str):
        self.model_name = model_name
        self.isp_used = isp_used
        self.sensitivity = sensitivity
        self.explanation = explanation


class MockSDDReport:
    def __init__(self, non_pii: Optional[Any] = None, completion_tokens: int = 0, prompt_tokens: int = 0):
        self.non_pii = non_pii
        self.completion_tokens = completion_tokens
        self.prompt_tokens = prompt_tokens
        self.non_pii_reports = []

    def add_non_pii_report(self, report: MockNonPIIReport):
        self.non_pii_reports.append(report)


# --- Fixtures ---


@pytest.fixture
def non_pii_classifier_instance():
    """
    This fixture automatically uses the MockAzureOpenAIStrategy
    because conftest.py patches it globally.
    """
    with patch('classifiers.non_pii_classifier.BaseClassifier', MockBaseClassifier):
        classifier = NonPIIClassifier(model_name='mock-non-pii-model')
        classifier._run_prompt = MagicMock()
        return classifier


@pytest.fixture
def mock_report():
    return MockSDDReport(completion_tokens=5, prompt_tokens=15)


# --- Test Data ---

TEST_ISP = {'mock_isp': {'config': 'data'}}
TEST_TABLE = 'markdown_table_content'
TEST_MAX_TOKENS = 512
TEST_VERSION = 'v1'


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
        ('\n\nhigh_sensitive but after newlines', 'UNDETERMINED'),  # Newlines make the first split element empty
    ],
)
def test_format_prediction(non_pii_classifier_instance, raw_pred, expected):
    """Test various raw prediction outputs are correctly mapped."""
    assert non_pii_classifier_instance.format_prediction(raw_pred) == expected


def test_format_prediction_only_first_line_is_considered(non_pii_classifier_instance):
    """Ensure only the first line determines the classification."""
    raw_pred = 'UNDETERMINED\n\nbut the second line contains: high_sensitive'
    assert non_pii_classifier_instance.format_prediction(raw_pred) == 'UNDETERMINED'


# =====================================================================
# 2. classify Tests
# =====================================================================


def test_classify_requires_isp(non_pii_classifier_instance, mock_report):
    """Test ValueError is raised if ISP is None."""
    with pytest.raises(ValueError, match='ISP is required'):
        non_pii_classifier_instance.classify(table_markdown=TEST_TABLE, report=mock_report, isp=None)


def test_classify_short_circuits_if_non_pii_exists(non_pii_classifier_instance):
    """Test that classification is skipped if report.non_pii is already set."""
    # Initialize report with a non_pii value
    pre_existing_non_pii = {'sensitivity': 'HIGH'}
    report = MockSDDReport(non_pii=pre_existing_non_pii)

    # Run classify
    result_report = non_pii_classifier_instance.classify(
        table_markdown=TEST_TABLE,
        report=report,
        isp=TEST_ISP,
    )

    # Assert against the MagicMock instance
    non_pii_classifier_instance._run_prompt.assert_not_called()
    # Assert the original report is returned
    assert result_report.non_pii == pre_existing_non_pii


# Patch the data model classes globally for the successful path test
@patch('classifiers.non_pii_classifier.NonPIIReport', MockNonPIIReport)
@patch('classifiers.non_pii_classifier.SDDReport', MockSDDReport)
def test_classify_success(non_pii_classifier_instance, mock_report):
    """Test the complete successful classification flow."""

    # Mock _run_prompt to return a successful prediction
    llm_prediction = 'The table contains high_sensitive information.'
    comp_tokens_llm = 50
    prompt_tokens_llm = 100

    # Set return_value on the mock object instance attribute
    non_pii_classifier_instance._run_prompt.return_value = (llm_prediction, comp_tokens_llm, prompt_tokens_llm)

    initial_comp_tokens = mock_report.completion_tokens
    initial_prompt_tokens = mock_report.prompt_tokens

    # Execute classification
    result_report = non_pii_classifier_instance.classify(
        table_markdown=TEST_TABLE,
        report=mock_report,
        isp=TEST_ISP,
        max_new_tokens=TEST_MAX_TOKENS,
        version=TEST_VERSION,
    )

    # 1. Verify _run_prompt call
    non_pii_classifier_instance._run_prompt.assert_called_once_with(
        'non_pii_detection', {'table_markdown': TEST_TABLE, 'isp': TEST_ISP['mock_isp']}, TEST_VERSION, TEST_MAX_TOKENS
    )

    # 2. Verify token counts are updated
    assert result_report.completion_tokens == initial_comp_tokens + comp_tokens_llm
    assert result_report.prompt_tokens == initial_prompt_tokens + prompt_tokens_llm

    # 3. Verify report was added and correctly formatted
    assert len(result_report.non_pii_reports) == 1
    report_added = result_report.non_pii_reports[0]

    assert report_added.model_name == non_pii_classifier_instance.model_name
    assert report_added.isp_used == 'mock_isp'
    assert report_added.sensitivity == 'HIGH_SENSITIVE'
    assert report_added.explanation == llm_prediction


@patch('classifiers.non_pii_classifier.logger')
def test_classify_exception_handling(mock_logger, non_pii_classifier_instance, mock_report):
    """Test the exception handling path returns the report and logs the error."""

    # Mock _run_prompt to raise an exception
    mock_error = RuntimeError('LLM service failed')
    # Set side_effect on the mock object instance attribute
    non_pii_classifier_instance._run_prompt.side_effect = mock_error

    # Execute classification
    result_report = non_pii_classifier_instance.classify(
        table_markdown=TEST_TABLE,
        report=mock_report,
        isp=TEST_ISP,
    )

    # Assert the original report instance is returned
    assert result_report is mock_report

    # Assert the exception was logged
    mock_logger.exception.assert_called_once()
    log_message = mock_logger.exception.call_args[0][0]
    assert 'Non-PII table sensitivity classification failed' in log_message
