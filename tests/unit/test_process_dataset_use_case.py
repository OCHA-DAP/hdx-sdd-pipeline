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
        """Test PII sensitivity classification with sensitive PII entities (should skip LLM)."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        column = Column(name='email', sample_values=['test@example.com'])
        column.pii_classification.entity_type = PIIEntityType.EMAIL_ADDRESS
        report.add_column(column)

        # LLM should not be called due to sensitive PII detection
        result = use_case._classify_pii_sensitivity(report)

        assert result.columns[0].pii_classification.sensitive is True
        assert result.completion_tokens == 0  # No LLM call made
        assert result.prompt_tokens == 0  # No LLM call made
        assert (
            result.pii_reflection_model
            == 'skipped - sensitive PII entities detected (email, phone number, or person names)'
        )

    def test_classify_pii_sensitivity_non_sensitive(self, use_case, mock_llm_provider):
        """Test PII sensitivity classification for non-sensitive."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        column = Column(name='phone', sample_values=['555-1234'])
        column.pii_classification.entity_type = PIIEntityType.PHONE_NUMBER  # Use actual PII type
        report.add_column(column)

        mock_llm_provider.generate.return_value = ('non_sensitive', 10, 20)

        result = use_case._classify_pii_sensitivity(report)

        assert result.columns[0].pii_classification.sensitive is True

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
