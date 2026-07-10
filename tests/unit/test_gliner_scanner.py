"""Unit tests for GliNERScanner (FR-SDD-057) — column-level scan strategy."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.infrastructure.gliner_scanner import (
    GliNERScanResult,
    GliNERScanner,
    _cell_to_str,
    _find_column,
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

    def test_find_column_exact(self):
        col_map = [('Name', 0, 10), ('City', 11, 20)]
        assert _find_column(col_map, 0, 10) == 'Name'
        assert _find_column(col_map, 11, 20) == 'City'

    def test_find_column_overlap(self):
        col_map = [('Name', 0, 15), ('Addr', 16, 35)]
        assert _find_column(col_map, 5, 12) == 'Name'

    def test_find_column_empty_map(self):
        assert _find_column([], 0, 5) == 'unknown'


# ---------------------------------------------------------------------------
# Email regex fast-path (vectorized, no model needed)
# ---------------------------------------------------------------------------


class TestGliNERScannerEmailRegex:
    @pytest.fixture
    def scanner(self):
        return GliNERScanner(model_name='dummy', threshold=0.5, batch_size=256)

    def test_email_detected_without_model(self, scanner):
        """Email fast-path fires without loading GLiNER."""
        df = pd.DataFrame({'Contact': ['john.doe@example.com', 'no-email-here', '']})
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

    def test_row_idx_tracked_correctly(self, scanner):
        """Email regex tracks the correct row index for each hit."""
        df = pd.DataFrame({'Email': ['clean', 'a@b.com', 'clean2']})
        result = GliNERScanResult()
        scanner._scan_emails_regex(df, result)
        assert result.evidence[0]['row_idx'] == 1


# ---------------------------------------------------------------------------
# Column-level GLiNER scan (mocked model)
# ---------------------------------------------------------------------------


class TestGliNERScannerGliNER:
    @pytest.fixture
    def mock_model(self):
        return MagicMock()

    @pytest.fixture
    def scanner(self, mock_model):
        s = GliNERScanner(model_name='gliner-community/gliner_small-v2.5', threshold=0.5, batch_size=256)
        s._model = mock_model
        return s, mock_model

    def test_clean_dataframe_not_flagged(self, scanner):
        s, model = scanner
        model.predict_entities.return_value = []
        df = pd.DataFrame({'City': ['Nairobi', 'Kabul'], 'Count': [100, 200]})

        result = s.scan_dataframe(df)

        assert result.flagged is False
        assert result.evidence == []

    def test_person_name_detected(self, scanner):
        s, model = scanner
        model.predict_entities.return_value = [
            {'start': 0, 'end': 10, 'text': 'John Smith', 'label': 'person name', 'score': 0.92}
        ]
        df = pd.DataFrame({'Name': ['John Smith', 'Jane Doe']})

        result = s.scan_dataframe(df)

        assert result.flagged is True
        assert any(h['label'] == 'person name' for h in result.evidence)

    def test_gliner_hit_row_idx_is_minus_one(self, scanner):
        """GLiNER column-level hits do not track a specific row (row_idx=-1)."""
        s, model = scanner
        model.predict_entities.return_value = [
            {'start': 0, 'end': 5, 'text': 'Alice', 'label': 'person name', 'score': 0.85}
        ]
        df = pd.DataFrame({'Name': ['Alice']})

        result = s.scan_dataframe(df)

        assert result.evidence[0]['row_idx'] == -1

    def test_address_detected(self, scanner):
        s, model = scanner
        model.predict_entities.return_value = [
            {'start': 0, 'end': 20, 'text': '15 Baker Street', 'label': 'street address', 'score': 0.87}
        ]
        df = pd.DataFrame({'Address': ['15 Baker Street, London']})

        result = s.scan_dataframe(df)

        assert result.flagged is True
        assert any(h['label'] == 'street address' for h in result.evidence)

    def test_non_western_name_detected(self, scanner):
        s, model = scanner
        model.predict_entities.return_value = [
            {'start': 0, 'end': 2, 'text': '张伟', 'label': 'person name', 'score': 0.88}
        ]
        df = pd.DataFrame({'Recipient': ['张伟', '李明']})

        result = s.scan_dataframe(df)

        assert result.flagged is True
        assert result.evidence[0]['text'] == '张伟'

    def test_no_entities_below_threshold(self, scanner):
        """If GLiNER returns no entities (threshold filtered), nothing is flagged."""
        s, model = scanner
        model.predict_entities.return_value = []
        df = pd.DataFrame({'Name': ['Ali']})

        result = s.scan_dataframe(df)

        assert result.flagged is False

    def test_empty_dataframe_returns_clean(self, scanner):
        s, model = scanner
        df = pd.DataFrame()

        result = s.scan_dataframe(df)

        assert result.flagged is False
        model.predict_entities.assert_not_called()

    def test_column_with_only_nulls_skipped(self, scanner):
        s, model = scanner
        model.predict_entities.return_value = []
        df = pd.DataFrame({'A': [None, None], 'B': ['data', 'more']})

        s.scan_dataframe(df)

        # predict_entities called once: only for column B
        assert model.predict_entities.call_count == 1

    def test_duplicate_values_deduplicated_before_scan(self, scanner):
        """Unique deduplication reduces calls: 1000 rows with same value → 1 chunk."""
        s, model = scanner
        model.predict_entities.return_value = []
        # 1000 identical rows — after dedup only 1 unique value
        df = pd.DataFrame({'Name': ['Alice'] * 1000})

        s.scan_dataframe(df)

        # Only one call: one unique value fits in one chunk
        assert model.predict_entities.call_count == 1

    def test_early_exit_after_first_hit_in_column(self, scanner):
        """Once a hit is found in a column, remaining chunks are skipped."""
        s, model = scanner
        s.batch_size = 2  # force small chunks so multiple would exist

        # First call returns a hit; second should never happen for that column.
        model.predict_entities.side_effect = [
            [{'start': 0, 'end': 5, 'text': 'Alice', 'label': 'person name', 'score': 0.9}],
            [],  # this should not be called
        ]
        # Three unique values but batch_size=2 → caps to 2 unique values
        df = pd.DataFrame({'Name': ['Alice', 'Bob', 'Carol']})

        result = s.scan_dataframe(df)

        assert result.flagged is True
        # With batch_size=2, only 2 unique values are sampled, fitting in one chunk
        assert model.predict_entities.call_count == 1

    def test_model_failure_falls_through(self, scanner):
        """Per-chunk prediction failure must not crash the scanner."""
        s, model = scanner
        model.predict_entities.side_effect = RuntimeError('model error')
        df = pd.DataFrame({'Name': ['Fatima']})

        result = s.scan_dataframe(df)

        assert result.flagged is False

    def test_batch_size_caps_unique_values(self, scanner):
        """When unique values exceed batch_size, only batch_size values are scanned."""
        s, model = scanner
        s.batch_size = 5
        model.predict_entities.return_value = []

        # 100 unique values — only 5 should be sampled
        df = pd.DataFrame({'Name': [f'person_{i}' for i in range(100)]})

        s.scan_dataframe(df)

        # All 5 values fit in one chunk → exactly 1 call
        assert model.predict_entities.call_count == 1

    def test_per_column_calls_not_per_row(self, scanner):
        """With 3 columns and many rows, predict_entities is called per column, not per row."""
        s, model = scanner
        model.predict_entities.return_value = []
        # 10 000 rows, 3 columns — should be ~3 calls (one per column, each with 1 chunk)
        n = 10_000
        df = pd.DataFrame(
            {
                'City': ['Nairobi'] * n,  # 1 unique → 1 chunk
                'Count': ['100'] * n,  # 1 unique → 1 chunk
                'Status': ['Active'] * n,  # 1 unique → 1 chunk
            }
        )

        s.scan_dataframe(df)

        # One GLiNER call per column (1 unique value each = 1 chunk each)
        assert model.predict_entities.call_count == 3

    def test_lazy_model_load(self):
        s = GliNERScanner(model_name='dummy')
        assert s._model is None

    def test_import_error_raised_when_gliner_missing(self):
        s = GliNERScanner(model_name='dummy')
        with patch.dict('sys.modules', {'gliner': None}):
            with pytest.raises(ImportError, match='GLiNER is not installed'):
                s._load_model()

    def test_multiple_columns_each_scanned(self, scanner):
        """Each non-empty column gets its own predict_entities call."""
        s, model = scanner
        model.predict_entities.return_value = []
        df = pd.DataFrame({'A': ['foo'], 'B': ['bar'], 'C': ['baz']})

        s.scan_dataframe(df)

        assert model.predict_entities.call_count == 3


# ---------------------------------------------------------------------------
# Combined scan_dataframe path
# ---------------------------------------------------------------------------


class TestScanDataframeIntegration:
    def test_email_from_regex_and_name_from_model(self):
        """scan_dataframe combines regex and GLiNER hits."""
        scanner = GliNERScanner(model_name='dummy', threshold=0.5, batch_size=256)
        mock_model = MagicMock()
        mock_model.predict_entities.return_value = [
            {'start': 0, 'end': 10, 'text': 'John Smith', 'label': 'person name', 'score': 0.9}
        ]
        scanner._model = mock_model

        df = pd.DataFrame({'Name': ['John Smith'], 'Email': ['john@acme.org']})
        result = scanner.scan_dataframe(df)

        assert result.flagged is True
        labels = {h['label'] for h in result.evidence}
        assert 'person name' in labels
        assert 'email address' in labels

    def test_large_dataframe_uses_column_strategy(self):
        """44k-row table should result in O(columns) calls, not O(rows)."""
        scanner = GliNERScanner(model_name='dummy', threshold=0.5, batch_size=256)
        mock_model = MagicMock()
        mock_model.predict_entities.return_value = []
        scanner._model = mock_model

        n = 44_000
        df = pd.DataFrame(
            {
                'ID': range(n),  # all unique → capped at batch_size=256 → 1 chunk
                'Region': ['East Africa'] * n,  # 1 unique → 1 chunk
                'Count': [42] * n,  # 1 unique → 1 chunk
            }
        )

        scanner.scan_dataframe(df)

        # 3 columns → at most a few chunks each, not 44k calls
        assert mock_model.predict_entities.call_count <= 10
