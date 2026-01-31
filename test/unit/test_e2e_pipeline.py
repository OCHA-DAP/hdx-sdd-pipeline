"""
End-to-end test for the SDD pipeline.

This test validates the complete pipeline flow from DataFrame input to final report,
using mocked Azure OpenAI responses to avoid actual API calls.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from main import sheet_processor
from utils.processing import create_report
from test.unit.conftest import MockAzureOpenAIStrategy


@pytest.fixture
def sample_csv_path(tmp_path):
    """Create a temporary CSV file for testing."""
    csv_file = tmp_path / "test_data.csv"
    df = pd.DataFrame({
        'Name': ['Alice', 'Bob', 'Charlie'],
        'Email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
        'Age': [25, 30, 35],
        'Country': ['USA', 'UK', 'Canada']
    })
    df.to_csv(csv_file, index=False)
    return str(csv_file)


@pytest.fixture
def mock_isp():
    """Mock ISP configuration."""
    return {
        'default': {
            'sensitivity_rules': {
                'SEVERE_SENSITIVE': ['Personal data of beneficiaries'],
                'HIGH_SENSITIVE': ['Survey data at household level'],
                'MODERATE_SENSITIVE': ['Aggregated data'],
                'NON_SENSITIVE': ['Public statistics']
            }
        }
    }


class TestEndToEndPipeline:
    """End-to-end tests for the complete SDD pipeline."""

    def test_complete_pipeline_with_mocked_azure(self, sample_csv_path, mock_isp):
        """
        Test the complete pipeline flow:
        1. Create report from CSV
        2. Run PII classification (mocked)
        3. Run PII reflection (mocked)
        4. Run non-PII classification (mocked)
        5. Validate final report structure
        """
        
        # Step 1: Create initial report from CSV
        reports = create_report(sample_csv_path, resource_id='test-123', download_url='http://test.com/data.csv')
        
        assert len(reports) == 1, "Should create one report for CSV file"
        sdd_report = reports[0]
        
        # Validate initial report structure
        assert sdd_report['resource_id'] == 'test-123'
        assert sdd_report['file_name'] == sample_csv_path
        assert sdd_report['file_url'] == 'http://test.com/data.csv'
        assert sdd_report['processing_success'] is True
        assert sdd_report['n_columns'] == 4
        assert len(sdd_report['columns']) == 4
        assert sdd_report['personal_data_sensitive'] is False
        assert sdd_report['non_personal_data_sensitive'] is False
        
        # Step 2-4: Process through the pipeline with mocked Azure responses
        with patch('main.AzureOpenAIStrategy', MockAzureOpenAIStrategy):
            # Mock PII classifier to return EMAIL_ADDRESS for Email column
            with patch('classifiers.pii_classifier.PIIClassifier._run_prompt') as mock_pii:
                mock_pii.side_effect = [
                    ('PERSON_NAME', 5, 10),  # Name column
                    ('EMAIL_ADDRESS', 5, 10),  # Email column
                    ('AGE', 5, 10),  # Age column
                    ('None', 5, 10),  # Country column
                ]
                
                # Mock PII reflection to mark EMAIL_ADDRESS and AGE as sensitive
                with patch('classifiers.pii_reflection_classifier.PIIReflectionClassifier._run_prompt') as mock_reflect:
                    mock_reflect.side_effect = [
                        ('SENSITIVE', 3, 8),  # Name - sensitive
                        ('SENSITIVE', 3, 8),  # Email - sensitive
                        ('SENSITIVE', 3, 8),  # Age - sensitive
                        ('NON_SENSITIVE', 0, 0),  # Country - not sensitive (None entity)
                    ]
                    
                    # Mock non-PII classifier
                    with patch('classifiers.non_pii_classifier.NonPIIClassifier._run_prompt') as mock_non_pii:
                        mock_non_pii.return_value = (
                            {
                                'sensitivity': 'SEVERE_SENSITIVE',
                                'sensitive_columns': ['Name - PERSON_NAME', 'Email - EMAIL_ADDRESS', 'Age - AGE'],
                                'cited_isp_rules': ['Personal data of beneficiaries'],
                                'explanation': 'Contains personal identifiable information'
                            },
                            50,
                            100
                        )
                        
                        # Process the report through the pipeline
                        final_report = sheet_processor(sdd_report, mock_isp, model='gpt-4.1-nano')
        
        # Step 5: Validate final report structure and content
        assert final_report is not None, "Pipeline should return a report"
        assert final_report['processing_success'] is True, "Processing should succeed"
        
        # Check all required top-level keys exist
        required_keys = [
            'resource_id',
            'file_name',
            'file_url',
            'sheet_name',
            'processing_timestamp',
            'processing_success',
            'n_records',
            'n_columns',
            'completion_tokens',
            'prompt_tokens',
            'personal_data_sensitive',
            'non_personal_data_sensitive',
            'columns',
            'pii_classifier_model',
            'pii_reflection_model',
            'non_pii_model',
            'non_personal_data'
        ]
        
        for key in required_keys:
            assert key in final_report, f"Report should contain '{key}' key"
        
        # Validate column-level data
        assert len(final_report['columns']) == 4, "Should have 4 columns"
        
        for col in final_report['columns']:
            assert 'column_name' in col
            assert 'sample_values' in col
            assert 'personal_data' in col
            assert 'entity_type' in col['personal_data']
            assert 'sensitive' in col['personal_data']
        
        # Check specific column classifications
        name_col = next(c for c in final_report['columns'] if c['column_name'] == 'Name')
        assert name_col['personal_data']['entity_type'] == 'PERSON_NAME'
        assert name_col['personal_data']['sensitive'] is True
        
        email_col = next(c for c in final_report['columns'] if c['column_name'] == 'Email')
        assert email_col['personal_data']['entity_type'] == 'EMAIL_ADDRESS'
        assert email_col['personal_data']['sensitive'] is True
        
        age_col = next(c for c in final_report['columns'] if c['column_name'] == 'Age')
        assert age_col['personal_data']['entity_type'] == 'AGE'
        assert age_col['personal_data']['sensitive'] is True
        
        country_col = next(c for c in final_report['columns'] if c['column_name'] == 'Country')
        assert country_col['personal_data']['entity_type'] == 'None'
        assert country_col['personal_data']['sensitive'] is False
        
        # Validate sensitivity flags
        assert final_report['personal_data_sensitive'] is True, "Should be marked as PII sensitive"
        assert final_report['non_personal_data_sensitive'] is True, "Should be marked as non-PII sensitive"
        
        # Validate non-PII classification results
        assert 'non_personal_data' in final_report
        assert final_report['non_personal_data']['sensitivity'] == 'SEVERE_SENSITIVE'
        assert 'sensitive_columns' in final_report['non_personal_data']
        assert 'cited_isp_rules' in final_report['non_personal_data']
        assert 'explanation' in final_report['non_personal_data']
        
        # Validate token counts were aggregated
        assert final_report['completion_tokens'] > 0, "Should have completion tokens"
        assert final_report['prompt_tokens'] > 0, "Should have prompt tokens"
        
        # Validate model names are set
        assert final_report['pii_classifier_model'] == 'gpt-4.1-nano'
        assert final_report['pii_reflection_model'] == 'gpt-4.1-nano'
        assert final_report['non_pii_model'] == 'gpt-4.1-nano'


    def test_pipeline_with_non_sensitive_data(self, tmp_path, mock_isp):
        """Test pipeline with data that should not be marked as sensitive."""
        
        # Create a CSV with non-sensitive data
        csv_file = tmp_path / "non_sensitive.csv"
        df = pd.DataFrame({
            'Region': ['North', 'South', 'East'],
            'Population': [1000, 2000, 1500],
            'Year': [2020, 2021, 2022]
        })
        df.to_csv(csv_file, index=False)
        
        reports = create_report(str(csv_file))
        sdd_report = reports[0]
        
        with patch('main.AzureOpenAIStrategy', MockAzureOpenAIStrategy):
            with patch('classifiers.pii_classifier.PIIClassifier._run_prompt') as mock_pii:
                mock_pii.side_effect = [
                    ('None', 5, 10),  # Region
                    ('None', 5, 10),  # Population
                    ('None', 5, 10),  # Year
                ]
                
                with patch('classifiers.pii_reflection_classifier.PIIReflectionClassifier._run_prompt') as mock_reflect:
                    # No reflection needed for None entities
                    mock_reflect.return_value = ('NON_SENSITIVE', 0, 0)
                    
                    with patch('classifiers.non_pii_classifier.NonPIIClassifier._run_prompt') as mock_non_pii:
                        mock_non_pii.return_value = (
                            {
                                'sensitivity': 'NON_SENSITIVE',
                                'sensitive_columns': [],
                                'cited_isp_rules': ['Public statistics'],
                                'explanation': 'Aggregated regional data'
                            },
                            20,
                            40
                        )
                        
                        final_report = sheet_processor(sdd_report, mock_isp)
        
        # Should not be marked as sensitive
        assert final_report['personal_data_sensitive'] is False
        assert final_report['non_personal_data_sensitive'] is False
        assert final_report['non_personal_data']['sensitivity'] == 'NON_SENSITIVE'
        assert final_report['processing_success'] is True


    def test_pipeline_handles_errors_gracefully(self, sample_csv_path, mock_isp):
        """Test that pipeline handles errors and sets processing_success to False."""
        
        reports = create_report(sample_csv_path)
        sdd_report = reports[0]
        
        with patch('main.AzureOpenAIStrategy', MockAzureOpenAIStrategy):
            # Simulate an error in PII classification
            with patch('classifiers.pii_classifier.PIIClassifier.classify_df') as mock_classify:
                mock_classify.side_effect = Exception("Simulated Azure API error")
                
                final_report = sheet_processor(sdd_report, mock_isp)
        
        # Should have error information
        assert 'error_source' in final_report
        assert 'error_message' in final_report
        assert final_report['processing_success'] is False
