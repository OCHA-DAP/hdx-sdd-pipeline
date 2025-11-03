import pytest
from unittest.mock import MagicMock, patch

from classifiers.non_pii_classifier import NonPIIClassifier
from models.sdd_report import SDDReport, NonPIIReport


# ------------------------------------------------------------
# FIXTURES
# ------------------------------------------------------------
@pytest.fixture
def mock_report():
    """Minimal SDDReport instance (neutral state: no non-PII yet)."""
    report = SDDReport(
        resource_id='123',
        file_name='file.csv',
        file_url='http://example.com',
        sheet_name='Sheet1',
        processing_timestamp='now',
        processing_success=True,
        n_records=5,
        n_columns=3,
    )
    report.completion_tokens = 0
    report.prompt_tokens = 0
    return report


@pytest.fixture
def classifier():
    """Return NonPIIClassifier with model + prompt_manager mocked."""
    with patch('classifiers.non_pii_classifier.NonPIIClassifier'), patch('utils.prompt_manager.PromptManager'):
        return NonPIIClassifier(model_name='test-model')


# ------------------------------------------------------------
# TEST: format_prediction()
# ------------------------------------------------------------
@pytest.mark.parametrize(
    'prediction, expected',
    [
        ('high_sensitive\nextra text', 'HIGH_SENSITIVE'),
        ('something MODERATE_SENSITIVE blah', 'MODERATE_SENSITIVE'),
        ('non_sensitive', 'NON_SENSITIVE'),
        ('unknown text', 'UNDETERMINED'),
    ],
)
def test_format_prediction(prediction, expected, classifier):
    assert classifier.format_prediction(prediction) == expected


# ------------------------------------------------------------
# TEST: classify() requires ISP
# ------------------------------------------------------------
def test_classify_requires_isp(classifier, mock_report):
    with pytest.raises(ValueError):
        classifier.classify('dummy-table', mock_report, isp=None)


# ------------------------------------------------------------
# TEST: classify() does nothing if report already has non_pii
# ------------------------------------------------------------
def test_classify_skips_if_existing_report(classifier, mock_report):
    mock_report.non_pii = NonPIIReport(
        model_name='existing-model',
        isp_used='isp1',
        sensitivity='NON_SENSITIVE',
        explanation='already done',
    )

    result = classifier.classify('table', mock_report, isp={'default': {}})

    assert result is mock_report  # unchanged
    # _run_prompt should NOT be called


# ------------------------------------------------------------
# TEST: classify() calls model + updates report
# ------------------------------------------------------------
def test_classify_runs_prompt_and_updates_report(classifier, mock_report):

    # mock BaseClassifier._run_prompt return value
    classifier._run_prompt = MagicMock(return_value=('non_sensitive', 12, 3))

    isp = {'default': {'country': 'TestCountry'}}

    result = classifier.classify('table_md', mock_report, isp=isp)

    # verify internal call
    classifier._run_prompt.assert_called_once()

    assert isinstance(result.non_pii, NonPIIReport)
    assert result.non_pii.sensitivity == 'NON_SENSITIVE'
    assert result.non_pii.model_name == 'test-model'
    assert result.completion_tokens == 12
    assert result.prompt_tokens == 3


# ------------------------------------------------------------
# TEST: classify() handles errors gracefully (log + return same report)
# ------------------------------------------------------------
def test_classify_handles_exception(classifier, mock_report):
    classifier._run_prompt = MagicMock(side_effect=Exception('model crashed'))

    isp = {'default': {}}
    result = classifier.classify('data', mock_report, isp=isp)

    assert result is mock_report  # report should be unchanged on exception
    assert result.non_pii is None  # no report added
