"""Unit tests for ProcessDatasetUseCase."""

import pytest
from unittest.mock import Mock

from src.application.process_dataset import ProcessDatasetUseCase
from src.domain.entities import SheetReport, Column
from src.domain.value_objects import PIIEntityType, SensitivityLevel
from src.domain.exceptions import DataProcessingError
import pandas as pd


class TestProcessDatasetUseCase:
    """Test suite for ProcessDatasetUseCase."""

    @pytest.fixture
    def mock_data_loader(self):
        """Create mock data loader."""
        loader = Mock()
        loader.load_from_url = Mock()
        loader.load_from_file = Mock()
        loader.sample_dataframe = Mock()
        return loader

    @pytest.fixture
    def mock_llm_provider(self):
        """Create mock LLM provider."""
        llm = Mock()
        llm.model_name = 'test-model'
        llm.generate = Mock()
        return llm

    @pytest.fixture
    def mock_prompt_manager(self):
        """Create mock prompt manager."""
        manager = Mock()
        manager.get_prompt = Mock(return_value='Test prompt')
        return manager

    @pytest.fixture
    def use_case(self, mock_data_loader, mock_llm_provider, mock_prompt_manager):
        """Create ProcessDatasetUseCase instance."""
        return ProcessDatasetUseCase(
            data_loader=mock_data_loader,
            pii_llm_provider=mock_llm_provider,
            pii_reflection_llm_provider=mock_llm_provider,
            non_pii_llm_provider=mock_llm_provider,
            prompt_manager=mock_prompt_manager,
            sample_size=5,
        )

    def test_initialization(self, use_case):
        """Test use case initialization."""
        assert use_case is not None
        assert use_case.sample_size == 5

    def test_is_readme_sheet(self, use_case):
        """Test README sheet detection."""
        assert use_case._is_readme_sheet('README') is True
        assert use_case._is_readme_sheet('readme') is True
        assert use_case._is_readme_sheet('Read Me') is True
        assert use_case._is_readme_sheet('Instructions') is True
        assert use_case._is_readme_sheet('Metadata') is True
        assert use_case._is_readme_sheet('Data') is False
        assert use_case._is_readme_sheet('Sheet1') is False

    def test_create_readme_report(self, use_case):
        """Test creating README report."""
        df = pd.DataFrame({'col1': [1, 2, 3]})

        report = use_case._create_readme_report(sheet_name='README', source='test.xlsx', resource_id='test-123', df=df)

        assert report.sheet_name == 'README'
        assert report.is_readme is True
        assert report.file_name == 'test.xlsx'
        assert report.resource_id == 'test-123'
        assert report.n_records == 3

    def test_create_data_report_basic(self, use_case, mock_data_loader, mock_llm_provider):
        """Test creating basic data report."""
        # Setup mocks
        df = pd.DataFrame({'Name': ['John', 'Jane'], 'Age': [25, 30]})
        mock_data_loader.sample_dataframe.return_value = {
            'Name': ['John', 'Jane', '', '', ''],
            'Age': ['25', '30', '', '', ''],
        }
        mock_llm_provider.generate.return_value = ('PERSON_NAME', 10, 20)

        report = use_case._create_data_report(
            sheet_name='Sheet1', source='test.csv', resource_id='test-123', df=df, isp_rules=None
        )

        assert report.sheet_name == 'Sheet1'
        assert report.n_columns == 2
        assert len(report.columns) == 2
        assert report.pii_classifier_model == 'test-model'

    def test_classify_pii_valid_column(self, use_case, mock_llm_provider):
        """Test PII classification for valid column."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        column = Column(name='email', sample_values=['test@example.com'])
        report.add_column(column)

        mock_llm_provider.generate.return_value = ('EMAIL_ADDRESS', 10, 20)

        result = use_case._classify_pii(report)

        assert result.columns[0].pii_classification.entity_type == PIIEntityType.EMAIL_ADDRESS
        assert result.completion_tokens == 10
        assert result.prompt_tokens == 20

    def test_classify_pii_empty_column(self, use_case):
        """Test PII classification for empty column."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        column = Column(name='empty', sample_values=['', '', ''])
        report.add_column(column)

        result = use_case._classify_pii(report)

        assert result.columns[0].pii_classification.entity_type == PIIEntityType.NONE

    def test_classify_pii_heuristic_geo_coordinates(self, use_case):
        """Test heuristic classification for latitude and longitude."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        lat_col = Column(name='Latitude', sample_values=['40.7128'])
        lon_col = Column(name='longitude ', sample_values=['-74.0060'])  # test strip and case
        report.add_column(lat_col)
        report.add_column(lon_col)

        # Should not call LLM
        result = use_case._classify_pii(report)

        assert result.columns[0].pii_classification.entity_type == PIIEntityType.GEO_COORDINATES
        assert result.columns[1].pii_classification.entity_type == PIIEntityType.GEO_COORDINATES
        assert result.completion_tokens == 0
        assert result.prompt_tokens == 0

    def test_classify_pii_error_handling(self, use_case, mock_llm_provider):
        """Test PII classification handles errors."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        column = Column(name='test', sample_values=['value'])
        report.add_column(column)

        mock_llm_provider.generate.side_effect = Exception('API error')

        result = use_case._classify_pii(report)

        # Should mark as UNKNOWN and sensitive on error
        assert result.columns[0].pii_classification.entity_type == PIIEntityType.UNKNOWN
        assert result.columns[0].pii_classification.sensitive is True

    def test_classify_pii_sensitivity(self, use_case, mock_llm_provider):
        """Test PII sensitivity classification with sensitive PII entities (should NOT skip LLM)."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        column = Column(name='email', sample_values=['test@example.com'])
        column.pii_classification.entity_type = PIIEntityType.EMAIL_ADDRESS
        report.add_column(column)

        mock_llm_provider.generate_json.return_value = (
            {'sensitivity': 'SEVERE_SENSITIVE', 'explanation': 'Contains direct email addresses'},
            15,
            30,
        )

        result = use_case._classify_pii_sensitivity(report)

        assert result.columns[0].pii_classification.sensitive is True
        assert result.completion_tokens == 15
        assert result.prompt_tokens == 30
        assert result.personal_data_sensitive is True
        assert result.personal_data_classification.sensitivity == SensitivityLevel.SEVERE_SENSITIVE
        assert result.personal_data_classification.explanation == 'Contains direct email addresses'
        assert result.pii_reflection_model == 'test-model'

    def test_classify_pii_sensitivity_non_sensitive(self, use_case, mock_llm_provider):
        """Test PII sensitivity classification for non-sensitive."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        column = Column(name='Area Code', sample_values=['206', '254'])
        column.pii_classification.entity_type = PIIEntityType.PHONE_NUMBER  # Use actual PII type
        report.add_column(column)

        mock_llm_provider.generate_json.return_value = (
            {'sensitivity': 'NON_SENSITIVE', 'explanation': 'Only area codes, not personal phone numbers'},
            10,
            20,
        )

        result = use_case._classify_pii_sensitivity(report)

        assert result.columns[0].pii_classification.sensitive is False
        assert result.personal_data_sensitive is False
        assert result.personal_data_classification.sensitivity == SensitivityLevel.NON_SENSITIVE

    def test_classify_pii_sensitivity_error_handling(self, use_case, mock_llm_provider):
        """Test PII sensitivity handles errors."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        column = Column(name='id', sample_values=['12345'])
        column.pii_classification.entity_type = PIIEntityType.ID_NUMBER  # Use non-sensitive PII type
        report.add_column(column)

        mock_llm_provider.generate_json.side_effect = Exception('API error')

        result = use_case._classify_pii_sensitivity(report)

        # Should default to sensitive on error (err on side of caution) and add explanation
        assert result.columns[0].pii_classification.sensitive is True
        assert result.personal_data_sensitive is True
        assert result.personal_data_classification.explanation == 'Classification failed due to an error: API error'

    def test_classify_non_pii(self, use_case, mock_llm_provider):
        """Test non-PII classification."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        column = Column(name='city', sample_values=['NYC'])
        report.add_column(column)

        mock_llm_provider.generate_json.return_value = (
            {'sensitivity': 'HIGH_SENSITIVE', 'explanation': 'Test explanation', 'confidence': 0.9},
            10,
            20,
        )

        result = use_case._classify_non_pii(report, isp_rules=None)

        assert result.non_pii_classification.sensitivity == SensitivityLevel.HIGH_SENSITIVE
        assert result.completion_tokens == 10
        assert result.prompt_tokens == 20

    def test_classify_non_pii_with_isp_rules(self, use_case, mock_llm_provider):
        """Test non-PII classification with ISP rules."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')

        isp_rules = {'country': 'Ukraine', 'rules': {'location_data': 'HIGH_SENSITIVE'}}

        mock_llm_provider.generate_json.return_value = (
            {'sensitivity': 'MODERATE_SENSITIVE', 'explanation': 'Test explanation', 'confidence': 0.8},
            10,
            20,
        )

        result = use_case._classify_non_pii(report, isp_rules=isp_rules)

        assert result.non_pii_classification.sensitivity == SensitivityLevel.MODERATE_SENSITIVE

    def test_classify_non_pii_error_handling(self, use_case, mock_llm_provider):
        """Test non-PII classification handles errors."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')

        mock_llm_provider.generate_json.side_effect = Exception('API error')

        result = use_case._classify_non_pii(report, isp_rules=None)

        # Should mark as SEVERE_SENSITIVE on error and include exception details in explanation
        assert result.non_pii_classification.sensitivity == SensitivityLevel.SEVERE_SENSITIVE
        assert 'Classification failed due to an error: API error' in result.non_pii_classification.explanation

    def test_classify_non_pii_max_tokens_minimum(self, use_case, mock_llm_provider):
        """Test non-PII classification uses 2000 output tokens when column count * 5 <= 2000."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        for i in range(10):
            report.add_column(Column(name=f'col{i}', sample_values=[f'val{i}']))

        mock_llm_provider.generate_json.return_value = (
            {'sensitivity': 'NON_SENSITIVE', 'explanation': 'Test', 'confidence': 0.9},
            10,
            20,
        )

        use_case._classify_non_pii(report, isp_rules=None)

        mock_llm_provider.generate_json.assert_called_once()
        args, kwargs = mock_llm_provider.generate_json.call_args
        assert kwargs.get('max_tokens') == 2000

    def test_classify_non_pii_max_tokens_scaled(self, use_case, mock_llm_provider):
        """Test non-PII classification scales max_tokens when column count * 5 > 2000."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        use_case._generate_table_markdown = Mock(return_value='')  # avoid expensive markdown generation
        for i in range(500):
            report.add_column(Column(name=f'col{i}', sample_values=[f'val{i}']))
        mock_llm_provider.generate_json.return_value = (
            {'sensitivity': 'NON_SENSITIVE', 'explanation': 'Test', 'confidence': 0.9},
            10,
            20,
        )

        use_case._classify_non_pii(report, isp_rules=None)

        mock_llm_provider.generate_json.assert_called_once()
        args, kwargs = mock_llm_provider.generate_json.call_args
        assert kwargs.get('max_tokens') == 2500

    def test_execute_from_url(self, use_case, mock_data_loader, mock_llm_provider):
        """Test execute with URL source."""
        # Setup mocks
        df = pd.DataFrame({'Name': ['John'], 'Age': ['25']})
        mock_data_loader.load_from_url.return_value = {'Sheet1': df}
        mock_data_loader.sample_dataframe.return_value = {
            'Name': ['John', '', '', '', ''],
            'Age': ['25', '', '', '', ''],
        }
        mock_llm_provider.generate.return_value = ('PERSON_NAME', 10, 20)

        reports = use_case.execute(source='https://example.com/data.csv', resource_id='test-123', is_url=True)

        assert len(reports) == 1
        assert reports[0].resource_id == 'test-123'
        mock_data_loader.load_from_url.assert_called_once()

    def test_execute_from_file(self, use_case, mock_data_loader, mock_llm_provider):
        """Test execute with file source."""
        df = pd.DataFrame({'Name': ['John'], 'Age': ['25']})
        mock_data_loader.load_from_file.return_value = {'Sheet1': df}
        mock_data_loader.sample_dataframe.return_value = {
            'Name': ['John', '', '', '', ''],
            'Age': ['25', '', '', '', ''],
        }
        mock_llm_provider.generate.return_value = ('PERSON_NAME', 10, 20)

        reports = use_case.execute(source='/path/to/data.csv', resource_id='test-123', is_url=False)

        assert len(reports) == 1
        mock_data_loader.load_from_file.assert_called_once()

    def test_execute_with_readme_sheet(self, use_case, mock_data_loader, mock_llm_provider):
        """Test execute handles README sheets."""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        mock_data_loader.load_from_file.return_value = {'README': df, 'Data': df}
        mock_data_loader.sample_dataframe.return_value = {'col1': ['1', '2', '3', '', '']}
        mock_llm_provider.generate.return_value = ('PERSON_NAME', 10, 20)

        reports = use_case.execute(source='/path/to/data.xlsx', is_url=False)

        # Should have 2 reports: README and Data
        assert len(reports) == 2
        readme_report = [r for r in reports if r.is_readme][0]
        assert readme_report.sheet_name == 'README'

    def test_execute_error_handling(self, use_case, mock_data_loader):
        """Test execute handles errors."""
        mock_data_loader.load_from_url.side_effect = Exception('Network error')

        with pytest.raises(DataProcessingError, match='Dataset processing failed'):
            use_case.execute(source='https://example.com/data.csv', is_url=True)

    def test_execute_multiple_sheets(self, use_case, mock_data_loader, mock_llm_provider):
        """Test execute with multiple sheets."""
        df1 = pd.DataFrame({'Name': ['John']})
        df2 = pd.DataFrame({'City': ['NYC']})

        mock_data_loader.load_from_file.return_value = {'Sheet1': df1, 'Sheet2': df2}
        mock_data_loader.sample_dataframe.return_value = {'Name': ['John', '', '', '', '']}
        mock_llm_provider.generate.return_value = ('PERSON_NAME', 10, 20)

        reports = use_case.execute(source='/path/to/data.xlsx', is_url=False)

        assert len(reports) == 2
        assert reports[0].sheet_name == 'Sheet1'
        assert reports[1].sheet_name == 'Sheet2'

    def test_execute_metadata_truncation(self, use_case, mock_data_loader, mock_llm_provider):
        """Test that execute truncates long dataset and resource descriptions in metadata."""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        mock_data_loader.load_from_file.return_value = {'Sheet1': df}
        mock_data_loader.sample_dataframe.return_value = {'col1': ['1', '2', '3', '', '']}
        mock_llm_provider.generate.return_value = ('NONE', 10, 20)

        long_desc = 'a' * 1200
        metadata = {'dataset_description': long_desc, 'resource_description': long_desc, 'dataset_title': 'short title'}

        # We patch _create_data_report to verify the metadata passed to it
        use_case._create_data_report = Mock(wraps=use_case._create_data_report)

        use_case.execute(source='/path/to/data.xlsx', is_url=False, metadata=metadata)

        # Verify the original metadata dictionary wasn't mutated
        assert len(metadata['dataset_description']) == 1200

        # Verify the metadata passed to _create_data_report has truncated descriptions
        use_case._create_data_report.assert_called_once()
        args, _ = use_case._create_data_report.call_args
        passed_metadata = args[5]

        assert len(passed_metadata['dataset_description']) == 1000
        assert passed_metadata['dataset_description'] == 'a' * 1000
        assert len(passed_metadata['resource_description']) == 1000
        assert passed_metadata['resource_description'] == 'a' * 1000
        assert passed_metadata['dataset_title'] == 'short title'

    def test_execute_metadata_location_cleaning(self, use_case, mock_data_loader, mock_llm_provider):
        """Test that execute omits dataset_location when there are > 5 locations."""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        mock_data_loader.load_from_file.return_value = {'Sheet1': df}
        mock_data_loader.sample_dataframe.return_value = {'col1': ['1', '2', '3', '', '']}
        mock_llm_provider.generate.return_value = ('NONE', 10, 20)
        use_case._create_data_report = Mock(wraps=use_case._create_data_report)

        metadata = {'dataset_location': 'loc1, loc2, loc3, loc4, loc5, loc6'}
        use_case.execute(source='/path/to/data.xlsx', is_url=False, metadata=metadata)

        use_case._create_data_report.assert_called_once()
        args, _ = use_case._create_data_report.call_args
        passed_metadata = args[5]

        # Verified metadata passed down has None for dataset_location (omitted)
        assert passed_metadata['dataset_location'] is None


# ---------------------------------------------------------------------------
# GLiNER pre-scan integration tests (FR-SDD-057)
# ---------------------------------------------------------------------------


class TestGliNERScanIntegration:
    """Tests for the GLiNER fast full-table PII pre-scan wired into ProcessDatasetUseCase."""

    @pytest.fixture
    def mock_data_loader(self):
        loader = Mock()
        loader.load_from_url = Mock()
        loader.load_from_file = Mock()
        loader.sample_dataframe = Mock(return_value={'Name': ['John', '', '', '', '']})
        return loader

    @pytest.fixture
    def mock_llm_provider(self):
        llm = Mock()
        llm.model_name = 'test-model'
        llm.generate = Mock(return_value=('NONE', 0, 0))
        llm.generate_json = Mock(
            return_value=(
                {'sensitivity': 'NON_SENSITIVE', 'explanation': 'clean', 'confidence': 0.9},
                0,
                0,
            )
        )
        return llm

    @pytest.fixture
    def mock_prompt_manager(self):
        m = Mock()
        m.get_prompt = Mock(return_value='prompt')
        return m

    @pytest.fixture
    def mock_gliner_scanner_flagged(self):
        """GLiNER scanner that always reports PII detected."""
        from src.infrastructure.gliner_scanner import GliNERScanResult

        scanner = Mock()
        scanner.model_name = 'gliner-community/gliner_small-v2.5'
        hit_result = GliNERScanResult()
        hit_result.add_hit(column='Name', row_idx=0, text='Ahmed Al-Rashid', label='person name', score=0.91)
        scanner.scan_dataframe = Mock(return_value=hit_result)
        return scanner

    @pytest.fixture
    def mock_gliner_scanner_clean(self):
        """GLiNER scanner that reports no PII."""
        from src.infrastructure.gliner_scanner import GliNERScanResult

        scanner = Mock()
        scanner.model_name = 'gliner-community/gliner_small-v2.5'
        scanner.scan_dataframe = Mock(return_value=GliNERScanResult())
        return scanner

    def _make_use_case(self, data_loader, llm_provider, prompt_manager, gliner_scanner=None):
        return ProcessDatasetUseCase(
            data_loader=data_loader,
            pii_llm_provider=llm_provider,
            pii_reflection_llm_provider=llm_provider,
            non_pii_llm_provider=llm_provider,
            prompt_manager=prompt_manager,
            sample_size=5,
            gliner_scanner=gliner_scanner,
        )

    # ------------------------------------------------------------------
    # _run_gliner_scan unit
    # ------------------------------------------------------------------

    def test_run_gliner_scan_none_scanner_returns_false(self, mock_data_loader, mock_llm_provider, mock_prompt_manager):
        """When gliner_scanner is None, _run_gliner_scan must return False."""
        use_case = self._make_use_case(mock_data_loader, mock_llm_provider, mock_prompt_manager, gliner_scanner=None)
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        df = pd.DataFrame({'col': ['value']})

        result = use_case._run_gliner_scan(df, report)

        assert result is False
        assert report.personal_data_sensitive is False

    def test_run_gliner_scan_clean_returns_false(
        self, mock_data_loader, mock_llm_provider, mock_prompt_manager, mock_gliner_scanner_clean
    ):
        use_case = self._make_use_case(
            mock_data_loader, mock_llm_provider, mock_prompt_manager, gliner_scanner=mock_gliner_scanner_clean
        )
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        df = pd.DataFrame({'City': ['Nairobi', 'Kabul']})

        result = use_case._run_gliner_scan(df, report)

        assert result is False
        assert report.personal_data_sensitive is False

    def test_run_gliner_scan_flagged_populates_report(
        self, mock_data_loader, mock_llm_provider, mock_prompt_manager, mock_gliner_scanner_flagged
    ):
        use_case = self._make_use_case(
            mock_data_loader, mock_llm_provider, mock_prompt_manager, gliner_scanner=mock_gliner_scanner_flagged
        )
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        col = Column(name='Name', sample_values=['Ahmed'])
        report.add_column(col)
        df = pd.DataFrame({'Name': ['Ahmed Al-Rashid']})

        flagged = use_case._run_gliner_scan(df, report)

        assert flagged is True
        assert report.personal_data_sensitive is True
        assert report.personal_data_classification.sensitivity == SensitivityLevel.SEVERE_SENSITIVE
        assert len(report.gliner_scan_evidence) == 1
        assert report.gliner_scan_evidence[0]['label'] == 'person name'
        # Explanation must be full and grouped per column (no '… and N more').
        assert 'GLiNER' in report.personal_data_classification.explanation
        assert '…' not in report.personal_data_classification.explanation
        assert "'Name'" in report.personal_data_classification.explanation
        assert report.pii_reflection_model == 'skipped - GLiNER pre-scan detected personal data'
        assert 'gliner:' in report.pii_classifier_model
        # Column entity type must be set from the dominant GLiNER label.
        assert report.columns[0].pii_classification.entity_type == PIIEntityType.PERSON_NAME
        assert report.columns[0].pii_classification.sensitive is True

    def test_run_gliner_scan_marks_columns_sensitive(
        self, mock_data_loader, mock_llm_provider, mock_prompt_manager, mock_gliner_scanner_flagged
    ):
        use_case = self._make_use_case(
            mock_data_loader, mock_llm_provider, mock_prompt_manager, gliner_scanner=mock_gliner_scanner_flagged
        )
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        for name in ['Name', 'City', 'Age']:
            report.add_column(Column(name=name, sample_values=['val']))
        df = pd.DataFrame({'Name': ['Ahmed'], 'City': ['Cairo'], 'Age': ['30']})

        use_case._run_gliner_scan(df, report)

        assert report.columns[0].pii_classification.sensitive is True  # Name
        assert report.columns[1].pii_classification.sensitive is False  # City
        assert report.columns[2].pii_classification.sensitive is False  # Age

    def test_run_gliner_scan_exception_falls_through(self, mock_data_loader, mock_llm_provider, mock_prompt_manager):
        """Scanner exception must not crash the pipeline; returns False."""
        scanner = Mock()
        scanner.model_name = 'dummy'
        scanner.scan_dataframe = Mock(side_effect=RuntimeError('GPU OOM'))

        use_case = self._make_use_case(mock_data_loader, mock_llm_provider, mock_prompt_manager, gliner_scanner=scanner)
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        df = pd.DataFrame({'Name': ['Ali']})

        result = use_case._run_gliner_scan(df, report)

        assert result is False
        assert report.personal_data_sensitive is False

    # ------------------------------------------------------------------
    # End-to-end: create_data_report with GLiNER
    # ------------------------------------------------------------------

    def test_create_data_report_gliner_skips_llm_pii_steps(
        self, mock_data_loader, mock_llm_provider, mock_prompt_manager, mock_gliner_scanner_flagged
    ):
        """When GLiNER fires, LLM generate() must NOT be called for PII detection."""
        use_case = self._make_use_case(
            mock_data_loader, mock_llm_provider, mock_prompt_manager, gliner_scanner=mock_gliner_scanner_flagged
        )
        df = pd.DataFrame({'Name': ['Ahmed Al-Rashid', 'Fatima Zahra']})

        report = use_case._create_data_report(
            sheet_name='Sheet1', source='test.csv', resource_id='r1', df=df, isp_rules=None
        )

        # LLM PII generate() must not have been called
        mock_llm_provider.generate.assert_not_called()
        # Non-PII LLM still runs
        mock_llm_provider.generate_json.assert_called()
        assert report.personal_data_sensitive is True
        assert report.personal_data_classification.sensitivity == SensitivityLevel.SEVERE_SENSITIVE

    def test_create_data_report_clean_gliner_proceeds_to_llm(
        self, mock_data_loader, mock_llm_provider, mock_prompt_manager, mock_gliner_scanner_clean
    ):
        """When GLiNER is clean, LLM PII classification must still run."""
        use_case = self._make_use_case(
            mock_data_loader, mock_llm_provider, mock_prompt_manager, gliner_scanner=mock_gliner_scanner_clean
        )
        mock_llm_provider.generate.return_value = ('NONE', 5, 10)
        df = pd.DataFrame({'City': ['Nairobi', 'Kabul']})
        mock_data_loader.sample_dataframe.return_value = {'City': ['Nairobi', 'Kabul', '', '', '']}

        report = use_case._create_data_report(
            sheet_name='Sheet1', source='test.csv', resource_id='r1', df=df, isp_rules=None
        )

        # LLM PII step should have been called
        mock_llm_provider.generate.assert_called()
        assert report.gliner_scan_evidence == []

    def test_create_data_report_no_scanner_backward_compat(
        self, mock_data_loader, mock_llm_provider, mock_prompt_manager
    ):
        """Without a scanner, existing behaviour is preserved (backward compat)."""
        use_case = self._make_use_case(mock_data_loader, mock_llm_provider, mock_prompt_manager, gliner_scanner=None)
        mock_llm_provider.generate.return_value = ('NONE', 5, 10)
        df = pd.DataFrame({'City': ['Nairobi']})

        report = use_case._create_data_report(
            sheet_name='Sheet1', source='test.csv', resource_id='r1', df=df, isp_rules=None
        )

        mock_llm_provider.generate.assert_called()
        assert report.gliner_scan_evidence == []

    def test_pii_detection_latest_contains_false_positive_mitigation_instructions(self):
        """Verify the latest PII detection template contains the new instructions
        for PHONE_NUMBER false positive mitigation.
        """
        from src.shared.utils.prompt_manager import PromptManager

        pm = PromptManager()
        prompt = pm.get_prompt(
            'pii_detection',
            version=None,
            context={'column_name': 'Area Code', 'sample_values': ['206', '254', '120']},
        )
        assert 'FAOSTAT geographic area codes (such as 206 for Sudan (former))' in prompt
        assert 'NOT PHONE_NUMBER' in prompt
        assert 'Area Code' in prompt
