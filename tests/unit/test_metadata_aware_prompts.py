from unittest.mock import MagicMock, patch
from src.application.process_dataset import ProcessDatasetUseCase
from src.shared.utils.prompt_manager import PromptManager
import pandas as pd


def test_prompt_rendering_with_metadata():
    pm = PromptManager()

    # 1. Test pii_reflection v4 template rendering
    metadata = {
        'dataset_title': 'Test Dataset Title',
        'dataset_description': 'Test Dataset Description',
        'dataset_source': 'Test Source',
        'dataset_location': 'Afghanistan',
        'organization_title': 'Test Org',
        'resource_name': 'test_resource.csv',
        'resource_description': 'Test Resource Description',
    }

    prompt = pm.get_prompt(
        'pii_reflection', version='v4', context={'table_markdown': '| col1 | col2 |\n|---|---|\n| 1 | 2 |', **metadata}
    )

    for key, val in metadata.items():
        assert val in prompt
    assert '| col1 | col2 |' in prompt

    # 2. Test non_pii_classification v3 (standard) template rendering
    isp_rules = {
        'sensitivity_rules': {
            'SEVERE_SENSITIVE': {'data and information type': 'Severe Rule'},
            'HIGH_SENSITIVE': {'data and information type': 'High Rule'},
            'MODERATE_SENSITIVE': {'data and information type': 'Moderate Rule'},
            'LOW/NON_SENSITIVE': {'data and information type': 'Non-sensitive Rule'},
        }
    }
    prompt = pm.get_prompt(
        'non_pii_classification',
        version='v3',
        context={'table_markdown': '| col1 | col2 |\n|---|---|\n| 1 | 2 |', 'isp': isp_rules, **metadata},
    )
    for key, val in metadata.items():
        assert val in prompt
    assert 'Severe Rule' in prompt

    # 3. Test non_pii_classification/default v1 template rendering
    prompt = pm.get_prompt(
        'non_pii_classification/default',
        version='v1',
        context={'table_markdown': '| col1 | col2 |\n|---|---|\n| 1 | 2 |', **metadata},
    )
    for key, val in metadata.items():
        assert val in prompt
    assert 'Household or Individual Data' in prompt


def test_prompt_rendering_with_missing_metadata():
    pm = PromptManager()

    # Test pii_reflection v4 with missing fields
    prompt = pm.get_prompt(
        'pii_reflection',
        version='v4',
        context={
            'table_markdown': '| col1 | col2 |\n|---|---|\n| 1 | 2 |',
            'dataset_title': 'Only Title',
            'dataset_description': None,
            'dataset_source': '',
        },
    )

    assert 'Only Title' in prompt
    assert 'Dataset Description:' not in prompt
    assert 'Dataset Source:' not in prompt


def test_pipeline_integration_metadata_propagation():
    # Mock dependencies
    loader = MagicMock()
    loader.sample_dataframe.return_value = {'col1': ['val1', 'val2']}

    pii_llm = MagicMock()
    pii_llm.model_name = 'pii-model'
    # return a non-NONE entity to trigger reflection
    pii_llm.generate.return_value = ('SEX', 5, 5)

    pii_reflection_llm = MagicMock()
    pii_reflection_llm.model_name = 'reflection-model'
    pii_reflection_llm.generate_json.return_value = (
        {'sensitivity': 'HIGH_SENSITIVE', 'explanation': 'PII explanation'},
        10,
        10,
    )

    non_pii_llm = MagicMock()
    non_pii_llm.model_name = 'non-pii-model'
    non_pii_llm.generate_json.return_value = (
        {
            'sensitivity': 'NON_SENSITIVE',
            'sensitive_columns': [],
            'cited_isp_rules': [],
            'explanation': 'Non-PII explanation',
        },
        15,
        15,
    )

    pm = PromptManager()

    use_case = ProcessDatasetUseCase(
        data_loader=loader,
        pii_llm_provider=pii_llm,
        pii_reflection_llm_provider=pii_reflection_llm,
        non_pii_llm_provider=non_pii_llm,
        prompt_manager=pm,
    )

    metadata = {
        'dataset_title': 'My Unique Dataset Title',
        'dataset_description': 'My Unique Dataset Description',
        'resource_name': 'test.csv',
    }

    # Mock LLM calls to capture prompt rendered
    with (
        patch.object(pii_reflection_llm, 'generate_json', wraps=pii_reflection_llm.generate_json) as mock_refl,
        patch.object(non_pii_llm, 'generate_json', wraps=non_pii_llm.generate_json) as mock_non_pii,
    ):
        # Run classification pipeline steps directly
        use_case._create_data_report(
            sheet_name='Sheet1',
            source='test.csv',
            resource_id='res-123',
            df=pd.DataFrame(),
            isp_rules={'country': 'default'},
            metadata=metadata,
        )

        # Verify PII reflection call received the prompt with metadata
        assert mock_refl.called
        args, kwargs = mock_refl.call_args
        prompt_sent = args[0]
        assert 'My Unique Dataset Title' in prompt_sent
        assert 'My Unique Dataset Description' in prompt_sent
        assert 'Resource Name: test.csv' in prompt_sent

        # Verify Non-PII classification call received the prompt with metadata
        assert mock_non_pii.called
        args, kwargs = mock_non_pii.call_args
        prompt_sent = args[0]
        assert 'My Unique Dataset Title' in prompt_sent
        assert 'My Unique Dataset Description' in prompt_sent
        assert 'Resource Name: test.csv' in prompt_sent
