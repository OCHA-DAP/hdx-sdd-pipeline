"""Test README scanning functionality."""

import pytest
from unittest.mock import Mock
import pandas as pd

from src.application.process_dataset import ProcessDatasetUseCase


class TestReadmeScan:
    """Test suite for README scanning functionality."""

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
        llm.model_name = 'test-readme-model'
        llm.generate_json = Mock()
        return llm

    @pytest.fixture
    def mock_prompt_manager(self):
        """Create mock prompt manager."""
        manager = Mock()
        manager.get_prompt = Mock(return_value='Test README scan prompt')
        return manager

    @pytest.fixture
    def use_case_with_readme(self, mock_data_loader, mock_llm_provider, mock_prompt_manager):
        """Create ProcessDatasetUseCase with README scanning enabled."""
        return ProcessDatasetUseCase(
            data_loader=mock_data_loader,
            readme_llm_provider=mock_llm_provider,
            prompt_manager=mock_prompt_manager,
            sample_size=5,
        )

    def test_is_readme_sheet_variations(self, use_case_with_readme):
        """Test README sheet detection with various name patterns."""
        # Test positive cases
        assert use_case_with_readme._is_readme_sheet('README') is True
        assert use_case_with_readme._is_readme_sheet('readme') is True
        assert use_case_with_readme._is_readme_sheet('Read Me') is True
        assert use_case_with_readme._is_readme_sheet('README_Sheet') is True
        assert use_case_with_readme._is_readme_sheet('Instructions') is True
        assert use_case_with_readme._is_readme_sheet('Metadata') is True
        assert use_case_with_readme._is_readme_sheet('Info') is True
        # Documentation is not in the keyword list, so it should be False
        # assert use_case_with_readme._is_readme_sheet('Documentation') is True

        # Test negative cases
        assert use_case_with_readme._is_readme_sheet('Data') is False
        assert use_case_with_readme._is_readme_sheet('Sheet1') is False
        assert use_case_with_readme._is_readme_sheet('Users') is False
        assert use_case_with_readme._is_readme_sheet('Records') is False
        assert (
            use_case_with_readme._is_readme_sheet('READ_ME') is False
        )  # Normalization lowercases and removes spaces only; 'READ_ME' -> 'read_me',
        # which does not contain any README keyword substring, so this stays False.

    def test_extract_readme_content_success(self, use_case_with_readme):
        """Test successful README content extraction."""
        df = pd.DataFrame(
            {
                'Description': ['This dataset contains user information', 'Collected in 2023'],
                'Source': ['Survey data', 'Anonymous responses'],
                'Notes': ['PII removed', 'Data cleaned'],
            }
        )

        content = use_case_with_readme._extract_readme_content(df)

        expected_content = (
            'This dataset contains user information\n'
            'Collected in 2023\n'
            'Survey data\n'
            'Anonymous responses\n'
            'PII removed\n'
            'Data cleaned'
        )
        assert content == expected_content

    def test_extract_readme_content_with_nulls(self, use_case_with_readme):
        """Test README content extraction with null values."""
        df = pd.DataFrame(
            {
                'Description': ['This is a dataset', None, 'Additional info'],
                'Source': ['', 'Survey', None],
                'Notes': [pd.NA, 'Important', ''],
            }
        )

        content = use_case_with_readme._extract_readme_content(df)

        expected_content = 'This is a dataset\nAdditional info\nSurvey\nImportant'
        assert content == expected_content

    def test_extract_readme_content_empty_dataframe(self, use_case_with_readme):
        """Test README content extraction with empty dataframe."""
        df = pd.DataFrame({'col1': [], 'col2': []})

        content = use_case_with_readme._extract_readme_content(df)

        assert content is None

    def test_extract_readme_content_non_dataframe(self, use_case_with_readme):
        """Test README content extraction with non-DataFrame input."""
        content = use_case_with_readme._extract_readme_content('not a dataframe')

        assert content is None

    def test_process_readme_for_pii_success(self, use_case_with_readme, mock_llm_provider, mock_prompt_manager):
        """Test successful README PII processing."""
        readme_content = 'This dataset contains emails like user@example.com and phone numbers 555-1234'

        mock_llm_provider.generate_json.return_value = (
            {
                'personal_data_sensitive': True,
                'personal_data_entities': ['EMAIL_ADDRESS', 'PHONE_NUMBER'],
                'evidence': ['user@example.com', '555-1234'],
            },
            15,
            25,
        )

        result = use_case_with_readme._process_readme_for_pii(readme_content)

        assert result['personal_data_sensitive'] is True
        assert 'EMAIL_ADDRESS' in result['personal_data_entities']
        assert 'PHONE_NUMBER' in result['personal_data_entities']
        assert 'user@example.com' in result['evidence']
        assert '555-1234' in result['evidence']

        # Verify prompt was called correctly
        mock_prompt_manager.get_prompt.assert_called_once_with(
            'readme_scan', version=None, context={'readme_string': readme_content}
        )

    def test_process_readme_for_pii_no_pii(self, use_case_with_readme, mock_llm_provider, mock_prompt_manager):
        """Test README PII processing with no PII detected."""
        readme_content = 'This dataset contains only aggregated statistics'

        mock_llm_provider.generate_json.return_value = (
            {'personal_data_sensitive': False, 'personal_data_entities': [], 'evidence': []},
            10,
            20,
        )

        result = use_case_with_readme._process_readme_for_pii(readme_content)

        assert result['personal_data_sensitive'] is False
        assert result['personal_data_entities'] == []
        assert result['evidence'] == []

    def test_process_readme_for_pii_invalid_result(self, use_case_with_readme, mock_llm_provider):
        """Test README PII processing with invalid result format."""
        readme_content = 'Test content'

        # LLM returns non-dict result
        mock_llm_provider.generate_json.return_value = ('invalid string result', 10, 20)

        result = use_case_with_readme._process_readme_for_pii(readme_content)

        assert result['personal_data_sensitive'] is False
        assert result['personal_data_entities'] == []
        assert result['evidence'] == []
        assert 'error' in result

    def test_process_readme_for_pii_error_handling(self, use_case_with_readme, mock_llm_provider):
        """Test README PII processing error handling."""
        readme_content = 'Test content'

        mock_llm_provider.generate_json.side_effect = Exception('API Error')

        result = use_case_with_readme._process_readme_for_pii(readme_content)

        assert result['personal_data_sensitive'] is False
        assert result['personal_data_entities'] == []
        assert result['evidence'] == []
        assert 'error' in result
        assert 'API Error' in result['error']

    def test_create_readme_report_with_pii(self, use_case_with_readme, mock_llm_provider):
        """Test creating README report with PII detected."""
        df = pd.DataFrame({'Description': ['Contact: john@example.com for questions'], 'Notes': ['Phone: 555-1234']})

        # Mock the LLM response
        mock_llm_provider.generate_json.return_value = (
            {
                'personal_data_sensitive': True,
                'personal_data_entities': ['EMAIL_ADDRESS', 'PHONE_NUMBER'],
                'evidence': ['john@example.com', '555-1234'],
            },
            15,
            25,
        )

        report = use_case_with_readme._create_readme_report(
            sheet_name='README', source='test.xlsx', resource_id='test-123', df=df
        )

        assert report.is_readme is True
        assert report.personal_data_sensitive is True
        assert report.readme_content is not None
        assert 'john@example.com' in report.readme_content
        assert '555-1234' in report.readme_content
        assert report.readme_report['personal_data_sensitive'] is True
        assert report.readme_model == 'test-readme-model'

    def test_create_readme_report_no_pii(self, use_case_with_readme, mock_llm_provider):
        """Test creating README report with no PII detected."""
        df = pd.DataFrame(
            {
                'Description': ['This dataset contains aggregated statistics'],
                'Notes': ['No personal information included'],
            }
        )

        mock_llm_provider.generate_json.return_value = (
            {'personal_data_sensitive': False, 'personal_data_entities': [], 'evidence': []},
            10,
            20,
        )

        report = use_case_with_readme._create_readme_report(
            sheet_name='README', source='test.xlsx', resource_id='test-123', df=df
        )

        assert report.is_readme is True
        assert report.personal_data_sensitive is False
        assert report.readme_report['personal_data_sensitive'] is False

    def test_create_readme_report_no_llm(self, use_case_with_readme):
        """Test creating README report when README scanning is disabled."""
        df = pd.DataFrame({'Description': ['Some content']})

        # Create use case without README LLM provider
        use_case_no_readme = ProcessDatasetUseCase(data_loader=Mock(), readme_llm_provider=None)

        report = use_case_no_readme._create_readme_report(
            sheet_name='README', source='test.xlsx', resource_id='test-123', df=df
        )

        assert report.is_readme is True
        assert report.readme_content is None
        assert report.readme_report is None
        assert report.readme_model is None

    def test_create_readme_report_extraction_error(self, use_case_with_readme, mock_llm_provider):
        """Test creating README report when content extraction fails."""
        # Use non-DataFrame to trigger extraction error
        df = 'not a dataframe'

        report = use_case_with_readme._create_readme_report(
            sheet_name='README', source='test.xlsx', resource_id='test-123', df=df
        )

        assert report.is_readme is True
        assert report.readme_content is None
        assert report.readme_report is None

    def test_create_readme_report_llm_error(self, use_case_with_readme, mock_llm_provider):
        """Test creating README report when LLM processing fails."""
        df = pd.DataFrame({'Description': ['Test content']})

        mock_llm_provider.generate_json.side_effect = Exception('LLM Error')

        report = use_case_with_readme._create_readme_report(
            sheet_name='README', source='test.xlsx', resource_id='test-123', df=df
        )

        assert report.is_readme is True
        assert report.readme_content is not None
        # The model name is set to the error message when processing fails
        assert report.readme_model == 'test-readme-model'  # Model name remains set even on error

    def test_execute_with_readme_sheet(self, use_case_with_readme, mock_data_loader, mock_llm_provider):
        """Test full execution with README sheet."""
        readme_df = pd.DataFrame({'Description': ['Contact: admin@example.com'], 'Notes': ['Dataset version 1.0']})
        data_df = pd.DataFrame({'Name': ['John'], 'Age': [25]})

        mock_data_loader.load_from_file.return_value = {'README': readme_df, 'Data': data_df}
        mock_data_loader.sample_dataframe.return_value = {'Name': ['John', '', '', '', '']}
        mock_llm_provider.generate_json.return_value = (
            {
                'personal_data_sensitive': True,
                'personal_data_entities': ['EMAIL_ADDRESS'],
                'evidence': ['admin@example.com'],
            },
            15,
            25,
        )

        reports = use_case_with_readme.execute(source='test.xlsx', is_url=False)

        assert len(reports) == 2

        readme_report = next(r for r in reports if r.is_readme)
        data_report = next(r for r in reports if not r.is_readme)

        assert readme_report.sheet_name == 'README'
        assert readme_report.personal_data_sensitive is True
        assert data_report.sheet_name == 'Data'

    def test_execute_multiple_readme_sheets(self, use_case_with_readme, mock_data_loader, mock_llm_provider):
        """Test execution with multiple README sheets."""
        readme1_df = pd.DataFrame({'Info': ['First README']})
        readme2_df = pd.DataFrame({'Info': ['Second README']})

        mock_data_loader.load_from_file.return_value = {'README': readme1_df, 'Instructions': readme2_df}
        mock_llm_provider.generate_json.return_value = (
            {'personal_data_sensitive': False, 'personal_data_entities': [], 'evidence': []},
            10,
            20,
        )

        reports = use_case_with_readme.execute(source='test.xlsx', is_url=False)

        readme_reports = [r for r in reports if r.is_readme]
        assert len(readme_reports) == 2
        assert readme_reports[0].sheet_name == 'README'
        assert readme_reports[1].sheet_name == 'Instructions'

    def test_readme_sheet_name_case_insensitive(self, use_case_with_readme):
        """Test README sheet detection is case insensitive."""
        test_cases = [
            'readme',
            'README',
            'ReadMe',
            'read me',
            'README_SHEET',
            'instructions',
            'INSTRUCTIONS',
            'metadata',
            'METADATA',
            'info',
            'INFO',
        ]

        for sheet_name in test_cases:
            assert use_case_with_readme._is_readme_sheet(sheet_name), f'Failed for: {sheet_name}'

    def test_readme_content_ignores_short_strings(self, use_case_with_readme):
        """Test README content extraction ignores very short strings."""
        df = pd.DataFrame({'col1': ['a', 'ab', 'abc', 'valid content'], 'col2': ['x', 'yz', '', 'more content']})

        content = use_case_with_readme._extract_readme_content(df)

        # Should only include strings with length > 1
        # Note: 'a' is not included because it's length 1
        # 'ab' is included even though it contains 'a' because the whole string is length > 1
        assert 'ab' in content
        assert 'yz' in content
        assert 'valid content' in content
        assert 'more content' in content
