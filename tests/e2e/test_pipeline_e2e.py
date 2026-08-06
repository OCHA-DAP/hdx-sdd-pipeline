"""
End-to-end test for the SDD pipeline using the ProcessDatasetUseCase.

This test validates the complete pipeline flow from DataFrame input to final report,
using mocked Azure OpenAI responses to avoid actual API calls.
"""

from src.application.process_dataset import ProcessDatasetUseCase
from src.infrastructure.data_loader import SmartDataLoader
from src.shared.utils.prompt_manager import PromptManager

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock


@pytest.fixture
def sample_csv_path(tmp_path):
    """Create a temporary CSV file for testing."""
    csv_file = tmp_path / 'test_data.csv'
    df = pd.DataFrame(
        {
            'Name': ['Alice', 'Bob', 'Charlie'],
            'Email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
            'Age': [25, 30, 35],
            'Country': ['USA', 'UK', 'Canada'],
        }
    )
    df.to_csv(csv_file, index=False)
    return str(csv_file)


@pytest.fixture
def mock_llm_provider():
    """Create a mock LLM provider."""
    mock = MagicMock()
    mock.model_name = 'gpt-4.1-nano'
    return mock


@pytest.fixture
def mock_isp_rules():
    """Mock ISP rules configuration matching actual ISP structure."""
    return {
        'default': {
            'sensitivity_rules': {
                'SEVERE_SENSITIVE': {'data and information type': ['Personal data of beneficiaries']},
                'HIGH_SENSITIVE': {'data and information type': ['Survey data at household level']},
                'MODERATE_SENSITIVE': {'data and information type': ['Aggregated data']},
                'LOW/NON_SENSITIVE': {'data and information type': ['Public statistics']},
            }
        }
    }


class TestEndToEndPipeline:
    """End-to-end tests for the complete SDD pipeline."""

    def test_complete_pipeline_with_mocked_azure(self, sample_csv_path, mock_llm_provider, mock_isp_rules):
        """
        Test the complete pipeline flow:
        1. Load data from CSV
        2. Run PII classification (mocked)
        3. Run PII reflection (mocked)
        4. Run non-PII classification (mocked)
        5. Validate final report structure

        This test verifies that:
        - DataFrame is correctly loaded and sampled
        - PII classification identifies entity types
        - PII reflection determines sensitivity
        - Final report has all expected keys
        - Processing success flag is set correctly
        """

        # Setup data loader
        data_loader = SmartDataLoader(max_rows=1000)
        prompt_manager = PromptManager(prompts_dir='src/prompts')

        # Create use case with mocked LLM providers
        use_case = ProcessDatasetUseCase(
            data_loader=data_loader,
            pii_llm_provider=mock_llm_provider,
            pii_reflection_llm_provider=mock_llm_provider,
            non_pii_llm_provider=mock_llm_provider,
            prompt_manager=prompt_manager,
            sample_size=5,
        )

        # Mock the LLM responses
        with (
            patch.object(mock_llm_provider, 'generate') as mock_generate,
            patch.object(mock_llm_provider, 'generate_json') as mock_generate_json,
        ):
            # Setup mock responses for PII classification (4 columns)
            mock_generate.side_effect = [
                ('PERSON_NAME', 5, 10),  # Name column
                ('EMAIL_ADDRESS', 5, 10),  # Email column
                ('AGE', 5, 10),  # Age column
                ('None', 5, 10),  # Country column
            ]
            # Setup mock responses for generate_json (Reflection + Non-PII)
            mock_generate_json.side_effect = [
                # PII reflection response (ONE call for table)
                (
                    {
                        'sensitivity': 'SEVERE_SENSITIVE',
                        'explanation': 'Contains personal identifiable information',
                    },
                    10,
                    20,
                ),
                # Non-PII classification response
                (
                    {
                        'sensitivity': 'SEVERE_SENSITIVE',
                        'sensitive_columns': ['Name', 'Email', 'Age'],
                        'cited_isp_rules': ['Personal data of beneficiaries'],
                        'explanation': 'Contains personal identifiable information',
                    },
                    50,
                    100,
                ),
            ]

            # Execute the pipeline
            reports = use_case.execute(
                source=sample_csv_path,
                resource_id='test-123',
                is_url=False,
                isp_rules=mock_isp_rules['default'],  # Pass flattened rules
            )

        # Validate results
        assert len(reports) == 1, 'Should create one report for CSV file'
        report = reports[0]

        # Convert to dict if it's a SheetReport object
        if hasattr(report, 'to_dict'):
            report_dict = report.to_dict()
        else:
            report_dict = report

        # Check all required top-level keys exist
        required_keys = [
            'resource_id',
            'file_name',
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
            'non_personal_data',
        ]

        for key in required_keys:
            assert key in report_dict, f"Report should contain '{key}' key"

        # Validate processing success
        assert report_dict['processing_success'] is True, 'Processing should succeed'
        assert report_dict['resource_id'] == 'test-123'

        # Validate column-level data
        assert report_dict['n_columns'] == 4, 'Should have 4 columns'
        assert len(report_dict['columns']) == 4, 'Should have 4 column reports'

        for col in report_dict['columns']:
            assert 'column_name' in col
            assert 'sample_values' in col
            assert 'personal_data' in col
            assert 'entity_type' in col['personal_data']
            assert 'sensitive' in col['personal_data']

        # Check specific column classifications
        name_col = next(c for c in report_dict['columns'] if c['column_name'] == 'Name')
        assert name_col['personal_data']['entity_type'] == 'PERSON_NAME'
        assert name_col['personal_data']['sensitive'] is True

        email_col = next(c for c in report_dict['columns'] if c['column_name'] == 'Email')
        assert email_col['personal_data']['entity_type'] == 'EMAIL_ADDRESS'
        assert email_col['personal_data']['sensitive'] is True

        age_col = next(c for c in report_dict['columns'] if c['column_name'] == 'Age')
        assert age_col['personal_data']['entity_type'] == 'AGE'
        assert age_col['personal_data']['sensitive'] is True

        country_col = next(c for c in report_dict['columns'] if c['column_name'] == 'Country')
        assert country_col['personal_data']['entity_type'] == 'None'
        assert country_col['personal_data']['sensitive'] is False

        # Validate PII sensitivity flag
        assert report_dict['personal_data_sensitive'] is True, 'Should be marked as PII sensitive'

        # Validate non-PII classification results exist (may be UNDETERMINED if template fails)
        assert 'non_personal_data' in report_dict
        assert 'sensitivity' in report_dict['non_personal_data']

    def test_pipeline_with_non_sensitive_data(self, tmp_path, mock_llm_provider, mock_isp_rules):
        """Test pipeline with data that should not be marked as PII sensitive."""

        # Create a CSV with non-sensitive data
        csv_file = tmp_path / 'non_sensitive.csv'
        df = pd.DataFrame(
            {'Region': ['North', 'South', 'East'], 'Population': [1000, 2000, 1500], 'Year': [2020, 2021, 2022]}
        )
        df.to_csv(csv_file, index=False)

        # Setup pipeline
        data_loader = SmartDataLoader(max_rows=1000)
        prompt_manager = PromptManager(prompts_dir='src/prompts')

        use_case = ProcessDatasetUseCase(
            data_loader=data_loader,
            pii_llm_provider=mock_llm_provider,
            pii_reflection_llm_provider=mock_llm_provider,
            non_pii_llm_provider=mock_llm_provider,
            prompt_manager=prompt_manager,
            sample_size=5,
        )

        with (
            patch.object(mock_llm_provider, 'generate') as mock_generate,
            patch.object(mock_llm_provider, 'generate_json') as mock_generate_json,
        ):
            # All columns classified as None (3 columns)
            mock_generate.side_effect = [
                ('None', 5, 10),  # Region
                ('None', 5, 10),  # Population
                ('None', 5, 10),  # Year
            ]
            # Setup mock response for generate_json (Non-PII Classification)
            mock_generate_json.side_effect = [
                (
                    {
                        'sensitivity': 'NON_SENSITIVE',
                        'explanation': 'Aggregated regional data',
                    },
                    20,
                    40,
                ),
            ]

            reports = use_case.execute(
                source=str(csv_file), resource_id='test-456', is_url=False, isp_rules=mock_isp_rules['default']
            )

        report = reports[0]
        if hasattr(report, 'to_dict'):
            report_dict = report.to_dict()
        else:
            report_dict = report

        # Should not be marked as PII sensitive
        assert report_dict['personal_data_sensitive'] is False
        assert report_dict['processing_success'] is True

        # All columns should have None entity type
        for col in report_dict['columns']:
            assert col['personal_data']['entity_type'] == 'None'
            assert col['personal_data']['sensitive'] is False
