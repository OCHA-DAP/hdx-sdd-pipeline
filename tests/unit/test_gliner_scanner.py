"""Unit tests for GliNERScanner (FR-SDD-057)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.infrastructure.gliner_scanner import (
    GliNERScanResult,
    GliNERScanner,
    _cell_to_str,
    _find_column,
    _safe_attr,
)

# ---------------------------------------------------------------------------
# GliNERScanResult
# ---------------------------------------------------------------------------


class TestGliNERScanResult:
    def test_defaults_not_flagged(self):
        result = GliNERScanResult()
        assert result.flagged is False
        assert result.evidence == []

    def test_add_hit_sets_flagged(self):
        result = GliNERScanResult()
        result.add_hit(column='Name', row_idx=0, text='Ahmed Al-Rashid', label='person name', score=0.9)
        assert result.flagged is True
        assert len(result.evidence) == 1

    def test_add_hit_records_all_fields(self):
        result = GliNERScanResult()
        result.add_hit(column='Email', row_idx=3, text='x@y.com', label='email address', score=1.0, method='regex')
        hit = result.evidence[0]
        assert hit['column'] == 'Email'
        assert hit['row_idx'] == 3
        assert hit['text'] == 'x@y.com'
        assert hit['label'] == 'email address'
        assert hit['score'] == 1.0
        assert hit['method'] == 'regex'

    def test_multiple_hits_accumulated(self):
        result = GliNERScanResult()
        result.add_hit(column='A', row_idx=0, text='Jane', label='person name', score=0.8)
        result.add_hit(column='B', row_idx=1, text='Bob', label='person name', score=0.75)
        assert len(result.evidence) == 2


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_cell_to_str_none(self):
        assert _cell_to_str(None) == ''

    def test_cell_to_str_nan_string(self):
        assert _cell_to_str('nan') == ''
        assert _cell_to_str('NaN') == ''
        assert _cell_to_str('none') == ''
        assert _cell_to_str('None') == ''
        assert _cell_to_str('null') == ''

    def test_cell_to_str_empty(self):
        assert _cell_to_str('') == ''
        assert _cell_to_str('   ') == ''

    def test_cell_to_str_normal(self):
        assert _cell_to_str('John Smith') == 'John Smith'
        assert _cell_to_str(42) == '42'

    def test_safe_attr_plain(self):
        assert _safe_attr('name') == 'name'

    def test_safe_attr_spaces(self):
        assert _safe_attr('first name') == 'first_name'

    def test_safe_attr_leading_digit(self):
        result = _safe_attr('1column')
        assert result.startswith('_')

    def test_find_column_exact(self):
        col_map = [('Name', 0, 10), ('City', 11, 20)]
        assert _find_column(col_map, 0, 10) == 'Name'
        assert _find_column(col_map, 11, 20) == 'City'

    def test_find_column_overlap(self):
        col_map = [('Name', 0, 15), ('Addr', 16, 35)]
        # Overlap in first column
        assert _find_column(col_map, 5, 12) == 'Name'

    def test_find_column_empty_map(self):
        assert _find_column([], 0, 5) == 'unknown'


# ---------------------------------------------------------------------------
# GliNERScanner — email regex fast-path (no model needed)
# ---------------------------------------------------------------------------


class TestGliNERScannerEmailRegex:
    @pytest.fixture
    def scanner(self):
        return GliNERScanner(model_name='dummy', threshold=0.5, batch_size=256)

    def test_email_detected_without_model(self, scanner):
        """Email fast-path must fire without loading GLiNER."""
        df = pd.DataFrame({'Contact': ['john.doe@example.com', 'no-email-here', '']})
        # Patch _scan_with_gliner to ensure it does not affect test
        scanner._scan_with_gliner = MagicMock(return_value=None)

        result = GliNERScanResult()
        scanner._scan_emails_regex(df, result)

        assert result.flagged is True
        assert any(h['label'] == 'email address' for h in result.evidence)
        assert any(h['method'] == 'regex' for h in result.evidence)

    def test_no_email_no_hit(self, scanner):
        df = pd.DataFrame({'Name': ['John Smith', 'Maria García'], 'Age': [30, 25]})
        result = GliNERScanResult()
        scanner._scan_emails_regex(df, result)
        assert result.flagged is False

    def test_multiple_emails_in_one_cell(self, scanner):
        df = pd.DataFrame({'CC': ['a@b.com, c@d.com']})
        result = GliNERScanResult()
        scanner._scan_emails_regex(df, result)
        assert len(result.evidence) == 2

    def test_email_in_second_column(self, scanner):
        df = pd.DataFrame({'Name': ['Alice'], 'Email': ['alice@example.org']})
        result = GliNERScanResult()
        scanner._scan_emails_regex(df, result)
        assert result.evidence[0]['column'] == 'Email'


# ---------------------------------------------------------------------------
# GliNERScanner — GLiNER model integration (mocked)
# ---------------------------------------------------------------------------


class TestGliNERScannerGliNER:
    @pytest.fixture
    def mock_gliner_model(self):
        model = MagicMock()
        return model

    @pytest.fixture
    def scanner_with_model(self, mock_gliner_model):
        scanner = GliNERScanner(model_name='gliner-community/gliner_small-v2.5', threshold=0.5, batch_size=256)
        scanner._model = mock_gliner_model  # inject pre-loaded model
        return scanner, mock_gliner_model

    def test_clean_dataframe_not_flagged(self, scanner_with_model):
        scanner, model = scanner_with_model
        model.predict_entities_batch.return_value = [[], []]
        df = pd.DataFrame({'City': ['Nairobi', 'Kabul'], 'Count': [100, 200]})

        result = scanner.scan_dataframe(df)

        assert result.flagged is False
        assert result.evidence == []

    def test_person_name_detected(self, scanner_with_model):
        scanner, model = scanner_with_model
        # Simulate GLiNER returning a person name hit on row 0
        model.predict_entities_batch.return_value = [
            [{'start': 0, 'end': 10, 'text': 'John Smith', 'label': 'person name', 'score': 0.92}],
        ]
        df = pd.DataFrame({'Name': ['John Smith']})

        result = scanner.scan_dataframe(df)

        assert result.flagged is True
        assert any(h['label'] == 'person name' for h in result.evidence)

    def test_address_detected(self, scanner_with_model):
        scanner, model = scanner_with_model
        model.predict_entities_batch.return_value = [
            [{'start': 0, 'end': 20, 'text': '15 Baker Street', 'label': 'street address', 'score': 0.87}],
        ]
        df = pd.DataFrame({'Address': ['15 Baker Street, London']})

        result = scanner.scan_dataframe(df)

        assert result.flagged is True
        assert any(h['label'] == 'street address' for h in result.evidence)

    def test_non_western_name_detected(self, scanner_with_model):
        """Non-Western names are returned by the (mocked) model exactly as any other name."""
        scanner, model = scanner_with_model
        model.predict_entities_batch.return_value = [
            [{'start': 0, 'end': 9, 'text': '张伟', 'label': 'person name', 'score': 0.88}],
        ]
        df = pd.DataFrame({'Recipient': ['张伟']})

        result = scanner.scan_dataframe(df)

        assert result.flagged is True
        hit = result.evidence[0]
        assert hit['text'] == '张伟'
        assert hit['label'] == 'person name'

    def test_score_below_threshold_not_flagged(self, scanner_with_model):
        scanner, model = scanner_with_model
        # GLiNER is called with the threshold; we simulate it filtering internally.
        # The scanner respects whatever entities GLiNER returns — here none.
        model.predict_entities_batch.return_value = [[]]
        df = pd.DataFrame({'Name': ['Ali']})

        result = scanner.scan_dataframe(df)

        assert result.flagged is False

    def test_empty_dataframe_returns_clean(self, scanner_with_model):
        scanner, model = scanner_with_model
        df = pd.DataFrame()

        result = scanner.scan_dataframe(df)

        assert result.flagged is False
        model.predict_entities_batch.assert_not_called()

    def test_batching_calls_model_multiple_times(self, scanner_with_model):
        scanner, model = scanner_with_model
        scanner.batch_size = 3  # small batch to force multiple calls
        model.predict_entities_batch.return_value = [[], [], []]  # per-batch

        df = pd.DataFrame({'col': [f'value{i}' for i in range(7)]})

        scanner.scan_dataframe(df)

        # 7 rows with batch_size=3 → 3 batches (3+3+1)
        assert model.predict_entities_batch.call_count == 3

    def test_model_failure_falls_through(self, scanner_with_model):
        """Batch prediction failure must not crash the scanner — returns clean result."""
        scanner, model = scanner_with_model
        model.predict_entities_batch.side_effect = RuntimeError('model error')
        df = pd.DataFrame({'Name': ['Fatima']})

        result = scanner.scan_dataframe(df)

        # No crash; email regex didn't fire either → clean
        assert result.flagged is False

    def test_lazy_model_load(self):
        """Model is loaded only on first scan, not at construction."""
        scanner = GliNERScanner(model_name='dummy')
        assert scanner._model is None

    def test_import_error_raised_when_gliner_missing(self):
        """ImportError is raised with helpful message when gliner is not installed."""
        scanner = GliNERScanner(model_name='dummy')
        with patch.dict('sys.modules', {'gliner': None}):
            with pytest.raises(ImportError, match='GLiNER is not installed'):
                scanner._load_model()


# ---------------------------------------------------------------------------
# GliNERScanner — combined scan_dataframe path
# ---------------------------------------------------------------------------


class TestScanDataframeIntegration:
    def test_email_from_regex_and_name_from_model(self):
        """scan_dataframe combines regex and model hits."""
        scanner = GliNERScanner(model_name='dummy', threshold=0.5, batch_size=256)
        mock_model = MagicMock()
        # Model returns a name hit on the same row
        mock_model.predict_entities_batch.return_value = [
            [{'start': 0, 'end': 10, 'text': 'John Smith', 'label': 'person name', 'score': 0.9}],
        ]
        scanner._model = mock_model

        df = pd.DataFrame({'Name': ['John Smith'], 'Email': ['john@acme.org']})
        result = scanner.scan_dataframe(df)

        assert result.flagged is True
        labels = {h['label'] for h in result.evidence}
        assert 'person name' in labels
        assert 'email address' in labels
