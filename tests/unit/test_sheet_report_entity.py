"""Unit tests for SheetReport entity."""

import pytest

from src.domain.entities import SheetReport, Column
from src.domain.value_objects import PIIEntityType, SensitivityLevel


class TestSheetReport:
    """Test suite for SheetReport entity."""

    def test_create_basic_report(self):
        """Test creating a basic report."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')

        assert report.file_name == 'test.csv'
        assert report.sheet_name == 'Sheet1'
        assert report.n_columns == 0
        assert report.columns == []

    def test_create_report_empty_filename_raises_error(self):
        """Test that empty filename raises error."""
        with pytest.raises(ValueError, match='file_name is required'):
            SheetReport(file_name='', sheet_name='Sheet1')

    def test_add_column(self):
        """Test adding columns to report."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        column = Column(name='email', sample_values=['test@example.com'])

        report.add_column(column)

        assert report.n_columns == 1
        assert len(report.columns) == 1
        assert report.columns[0].name == 'email'

    def test_has_pii_columns_true(self):
        """Test has_pii_columns returns True when PII exists."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        column = Column(name='email', sample_values=['test@example.com'])
        column.pii_classification.entity_type = PIIEntityType.EMAIL_ADDRESS
        report.add_column(column)

        assert report.has_pii_columns() is True

    def test_has_pii_columns_false(self):
        """Test has_pii_columns returns False when no PII."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        column = Column(name='count', sample_values=['1', '2', '3'])
        column.pii_classification.entity_type = PIIEntityType.NONE
        report.add_column(column)

        assert report.has_pii_columns() is False

    def test_has_sensitive_pii_true(self):
        """Test has_sensitive_pii returns True for sensitive PII."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        column = Column(name='email', sample_values=['test@example.com'])
        column.pii_classification.entity_type = PIIEntityType.EMAIL_ADDRESS
        column.pii_classification.sensitive = True
        report.add_column(column)

        assert report.has_sensitive_pii() is True

    def test_has_sensitive_pii_false(self):
        """Test has_sensitive_pii returns False for non-sensitive."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        column = Column(name='age', sample_values=['25'])
        column.pii_classification.entity_type = PIIEntityType.AGE
        column.pii_classification.sensitive = False
        report.add_column(column)

        assert report.has_sensitive_pii() is False

    def test_update_pii_sensitivity(self):
        """Test updating PII sensitivity flag."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        column = Column(name='email', sample_values=['test@example.com'])
        column.pii_classification.entity_type = PIIEntityType.EMAIL_ADDRESS
        column.pii_classification.sensitive = True
        report.add_column(column)

        report.update_pii_sensitivity()

        assert report.personal_data_sensitive is True

    def test_update_non_pii_sensitivity(self):
        """Test updating non-PII sensitivity flag."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        report.non_pii_classification.sensitivity = SensitivityLevel.HIGH_SENSITIVE

        report.update_non_pii_sensitivity()

        assert report.non_personal_data_sensitive is True

    def test_is_sensitive_pii_only(self):
        """Test is_sensitive with PII only."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        report.personal_data_sensitive = True
        report.non_personal_data_sensitive = False

        assert report.is_sensitive() is True

    def test_is_sensitive_non_pii_only(self):
        """Test is_sensitive with non-PII only."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        report.personal_data_sensitive = False
        report.non_personal_data_sensitive = True

        assert report.is_sensitive() is True

    def test_is_sensitive_both(self):
        """Test is_sensitive with both PII and non-PII."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        report.personal_data_sensitive = True
        report.non_personal_data_sensitive = True

        assert report.is_sensitive() is True

    def test_is_sensitive_neither(self):
        """Test is_sensitive with neither."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        report.personal_data_sensitive = False
        report.non_personal_data_sensitive = False

        assert report.is_sensitive() is False

    def test_total_tokens(self):
        """Test total tokens calculation."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        report.completion_tokens = 100
        report.prompt_tokens = 200

        assert report.total_tokens() == 300

    def test_has_error_true(self):
        """Test has_error returns True when error exists."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        report.error_source = 'PII_CLASSIFICATION'
        report.error_message = 'API error'

        assert report.has_error() is True

    def test_has_error_false(self):
        """Test has_error returns False when no error."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')

        assert report.has_error() is False

    def test_to_dict_basic(self):
        """Test converting report to dictionary."""
        report = SheetReport(
            resource_id='test-123',
            file_name='test.csv',
            file_url='https://example.com/test.csv',
            sheet_name='Sheet1',
            n_records=100,
            n_columns=5,
        )

        result = report.to_dict()

        assert result['resource_id'] == 'test-123'
        assert result['file_name'] == 'test.csv'
        assert result['file_url'] == 'https://example.com/test.csv'
        assert result['sheet_name'] == 'Sheet1'
        assert result['n_records'] == 100
        assert result['n_columns'] == 5

    def test_to_dict_with_columns(self):
        """Test to_dict includes columns."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        column = Column(name='email', sample_values=['test@example.com'])
        report.add_column(column)

        result = report.to_dict()

        assert len(result['columns']) == 1
        assert result['columns'][0]['column_name'] == 'email'

    def test_to_dict_with_models(self):
        """Test to_dict includes model names."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        report.pii_classifier_model = 'gpt-4.1-nano'
        report.pii_reflection_model = 'gpt-4.1-nano'
        report.non_pii_model = 'gpt-4.1-nano'

        result = report.to_dict()

        assert result['pii_classifier_model'] == 'gpt-4.1-nano'
        assert result['pii_reflection_model'] == 'gpt-4.1-nano'
        assert result['non_pii_model'] == 'gpt-4.1-nano'

    def test_to_dict_with_error(self):
        """Test to_dict includes error information."""
        report = SheetReport(file_name='test.csv', sheet_name='Sheet1')
        report.error_source = 'PII_CLASSIFICATION'
        report.error_message = 'API error'

        result = report.to_dict()

        assert result['error_source'] == 'PII_CLASSIFICATION'
        assert result['error_message'] == 'API error'

    def test_to_dict_readme(self):
        """Test to_dict for README sheet."""
        report = SheetReport(file_name='test.csv', sheet_name='README')
        report.is_readme = True
        report.readme_content = 'This is a README'

        result = report.to_dict()

        assert result['is_readme'] is True
        assert 'readme' not in result

    def test_from_dict_basic(self):
        """Test creating report from dictionary."""
        data = {
            'resource_id': 'test-123',
            'file_name': 'test.csv',
            'file_url': 'https://example.com/test.csv',
            'sheet_name': 'Sheet1',
            'processing_timestamp': '2024-01-15 10:30:00',
            'processing_success': True,
            'n_records': 100,
            'n_columns': 5,
            'completion_tokens': 150,
            'prompt_tokens': 500,
            'personal_data_sensitive': True,
            'non_personal_data_sensitive': False,
            'columns': [],
            'non_personal_data': {'sensitivity': 'NON_SENSITIVE'},
        }

        report = SheetReport.from_dict(data)

        assert report.resource_id == 'test-123'
        assert report.file_name == 'test.csv'
        assert report.sheet_name == 'Sheet1'
        assert report.n_records == 100
        assert report.personal_data_sensitive is True

    def test_from_dict_with_columns(self):
        """Test from_dict includes columns."""
        data = {
            'file_name': 'test.csv',
            'sheet_name': 'Sheet1',
            'processing_timestamp': '2024-01-15 10:30:00',
            'columns': [
                {
                    'column_name': 'email',
                    'sample_values': ['test@example.com'],
                    'personal_data': {'entity_type': 'EMAIL_ADDRESS', 'sensitive': True},
                }
            ],
            'non_personal_data': {'sensitivity': 'NON_SENSITIVE'},
        }

        report = SheetReport.from_dict(data)

        assert len(report.columns) == 1
        assert report.columns[0].name == 'email'
        assert report.columns[0].pii_classification.entity_type == PIIEntityType.EMAIL_ADDRESS

    def test_round_trip_serialization(self):
        """Test that to_dict and from_dict are inverses."""
        original = SheetReport(
            resource_id='test-123', file_name='test.csv', sheet_name='Sheet1', n_records=100, n_columns=2
        )

        column1 = Column(name='email', sample_values=['test@example.com'])
        column1.pii_classification.entity_type = PIIEntityType.EMAIL_ADDRESS
        column1.pii_classification.sensitive = True
        original.add_column(column1)

        column2 = Column(name='name', sample_values=['John Doe'])
        column2.pii_classification.entity_type = PIIEntityType.PERSON_NAME
        column2.pii_classification.sensitive = True
        original.add_column(column2)

        original.non_pii_classification.sensitivity = SensitivityLevel.HIGH_SENSITIVE

        # Convert to dict and back
        data = original.to_dict()
        restored = SheetReport.from_dict(data)

        assert restored.resource_id == original.resource_id
        assert restored.file_name == original.file_name
        assert restored.sheet_name == original.sheet_name
        assert len(restored.columns) == len(original.columns)
        assert restored.columns[0].name == original.columns[0].name
        assert restored.non_pii_classification.sensitivity == original.non_pii_classification.sensitivity
