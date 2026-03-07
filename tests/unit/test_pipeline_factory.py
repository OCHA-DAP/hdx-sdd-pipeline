"""Tests for Pipeline Factory."""

import pytest
from unittest.mock import Mock, patch
from src.infrastructure.factories.pipeline_factory import PipelineFactory
from src.application.use_cases.process_dataset import ProcessDatasetUseCase
from config.config import Config


class TestPipelineFactory:
    """Test suite for PipelineFactory."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock configuration."""
        config = Mock(spec=Config)
        config.PERSONAL_DATA_DETECTION = True
        config.PERSONAL_DATA_REFLECTION = True
        config.NON_PERSONAL_DATA_DETECTION = True
        config.README_SCAN = True
        config.CKAN_UPDATE = True
        config.PII_DETECT_MODEL = 'gpt-4.1-nano'
        config.PII_REFLECT_MODEL = 'gpt-4.1-nano'
        config.NON_PII_DETECT_MODEL = 'gpt-4.1-nano'
        config.AZURE_OPENAI_ENDPOINT = 'https://test.openai.azure.com'
        config.AZURE_OPENAI_API_KEY = 'test-key'
        return config

    @pytest.fixture
    def factory(self, mock_config):
        """Create a PipelineFactory instance."""
        return PipelineFactory(mock_config)

    def test_initialization(self, mock_config):
        """Test factory initialization."""
        factory = PipelineFactory(mock_config)
        assert factory.config == mock_config

    def test_log_configuration(self, mock_config, caplog):
        """Test that configuration is logged during initialization."""
        import logging

        with caplog.at_level(logging.INFO, logger='src.infrastructure.factories.pipeline_factory'):
            _ = PipelineFactory(mock_config)

        assert 'Pipeline Configuration' in caplog.text
        assert 'Personal data detection' in caplog.text

    @patch('src.infrastructure.factories.pipeline_factory.SmartDataLoader')
    @patch('src.infrastructure.factories.pipeline_factory.AzureOpenAIProvider')
    @patch('src.infrastructure.factories.pipeline_factory.PromptManager')
    def test_create_pipeline_all_enabled(self, mock_prompt_manager, mock_azure, mock_data_loader, factory):
        """Test pipeline creation with all features enabled."""
        # Setup mocks
        mock_data_loader_instance = Mock()
        mock_data_loader.return_value = mock_data_loader_instance

        mock_llm_instance = Mock()
        mock_azure.return_value = mock_llm_instance

        mock_prompt_manager_instance = Mock()
        mock_prompt_manager.return_value = mock_prompt_manager_instance

        # Create pipeline
        pipeline = factory.create_pipeline(sample_size=5)

        # Verify pipeline was created
        assert isinstance(pipeline, ProcessDatasetUseCase)

        # Verify data loader was created
        mock_data_loader.assert_called_once_with(max_rows=1000)

        # Verify all three LLM providers were created
        assert mock_azure.call_count == 4

        # Verify prompt manager was created
        mock_prompt_manager.assert_called_once_with(prompts_dir='src/prompts')

    @patch('src.infrastructure.factories.pipeline_factory.SmartDataLoader')
    @patch('src.infrastructure.factories.pipeline_factory.AzureOpenAIProvider')
    @patch('src.infrastructure.factories.pipeline_factory.PromptManager')
    def test_create_pipeline_pii_detection_disabled(
        self, mock_prompt_manager, mock_azure, mock_data_loader, mock_config
    ):
        """Test pipeline creation with PII detection disabled."""
        mock_config.PERSONAL_DATA_DETECTION = False
        factory = PipelineFactory(mock_config)

        # Setup mocks
        mock_data_loader.return_value = Mock()
        mock_azure.return_value = Mock()
        mock_prompt_manager.return_value = Mock()

        # Create pipeline
        _ = factory.create_pipeline()

        # Verify only 2 LLM providers were created (not PII detection)
        assert mock_azure.call_count == 3

    @patch('src.infrastructure.factories.pipeline_factory.SmartDataLoader')
    @patch('src.infrastructure.factories.pipeline_factory.AzureOpenAIProvider')
    @patch('src.infrastructure.factories.pipeline_factory.PromptManager')
    def test_create_pipeline_pii_reflection_disabled(
        self, mock_prompt_manager, mock_azure, mock_data_loader, mock_config
    ):
        """Test pipeline creation with PII reflection disabled."""
        mock_config.PERSONAL_DATA_REFLECTION = False
        factory = PipelineFactory(mock_config)

        # Setup mocks
        mock_data_loader.return_value = Mock()
        mock_azure.return_value = Mock()
        mock_prompt_manager.return_value = Mock()

        # Create pipeline
        _ = factory.create_pipeline()

        # Verify only 2 LLM providers were created (not PII reflection)
        assert mock_azure.call_count == 3

    @patch('src.infrastructure.factories.pipeline_factory.SmartDataLoader')
    @patch('src.infrastructure.factories.pipeline_factory.AzureOpenAIProvider')
    @patch('src.infrastructure.factories.pipeline_factory.PromptManager')
    def test_create_pipeline_non_pii_disabled(self, mock_prompt_manager, mock_azure, mock_data_loader, mock_config):
        """Test pipeline creation with non-PII detection disabled."""
        mock_config.NON_PERSONAL_DATA_DETECTION = False
        factory = PipelineFactory(mock_config)

        # Setup mocks
        mock_data_loader.return_value = Mock()
        mock_azure.return_value = Mock()
        mock_prompt_manager.return_value = Mock()

        # Create pipeline
        _ = factory.create_pipeline()

        # Verify only 2 LLM providers were created (not non-PII)
        assert mock_azure.call_count == 3

    @patch('src.infrastructure.factories.pipeline_factory.SmartDataLoader')
    @patch('src.infrastructure.factories.pipeline_factory.AzureOpenAIProvider')
    @patch('src.infrastructure.factories.pipeline_factory.PromptManager')
    def test_create_pipeline_all_disabled(self, mock_prompt_manager, mock_azure, mock_data_loader, mock_config):
        """Test pipeline creation with all LLM features disabled."""
        mock_config.PERSONAL_DATA_DETECTION = False
        mock_config.PERSONAL_DATA_REFLECTION = False
        mock_config.NON_PERSONAL_DATA_DETECTION = False
        mock_config.README_SCAN = False
        factory = PipelineFactory(mock_config)

        # Setup mocks
        mock_data_loader.return_value = Mock()
        mock_prompt_manager.return_value = Mock()

        # Create pipeline
        pipeline = factory.create_pipeline()

        # Verify pipeline was created
        assert isinstance(pipeline, ProcessDatasetUseCase)

        # Verify no LLM providers were created
        mock_azure.assert_not_called()

    @patch('src.infrastructure.factories.pipeline_factory.SmartDataLoader')
    @patch('src.infrastructure.factories.pipeline_factory.AzureOpenAIProvider')
    @patch('src.infrastructure.factories.pipeline_factory.PromptManager')
    def test_create_pipeline_custom_sample_size(self, mock_prompt_manager, mock_azure, mock_data_loader, factory):
        """Test pipeline creation with custom sample size."""
        # Setup mocks
        mock_data_loader.return_value = Mock()
        mock_azure.return_value = Mock()
        mock_prompt_manager.return_value = Mock()

        # Create pipeline with custom sample size
        pipeline = factory.create_pipeline(sample_size=10)

        # Verify pipeline was created
        assert isinstance(pipeline, ProcessDatasetUseCase)

    @patch('src.infrastructure.factories.pipeline_factory.AzureOpenAIProvider')
    def test_create_pii_llm(self, mock_azure, factory):
        """Test PII LLM provider creation."""
        mock_llm = Mock()
        mock_azure.return_value = mock_llm

        result = factory._create_pii_llm()

        mock_azure.assert_called_once_with(
            model_name='gpt-4.1-nano', azure_endpoint='https://test.openai.azure.com', api_key='test-key'
        )
        assert result == mock_llm

    @patch('src.infrastructure.factories.pipeline_factory.AzureOpenAIProvider')
    def test_create_pii_reflection_llm(self, mock_azure, factory):
        """Test PII reflection LLM provider creation."""
        mock_llm = Mock()
        mock_azure.return_value = mock_llm

        result = factory._create_pii_reflection_llm()

        mock_azure.assert_called_once_with(
            model_name='gpt-4.1-nano', azure_endpoint='https://test.openai.azure.com', api_key='test-key'
        )
        assert result == mock_llm

    @patch('src.infrastructure.factories.pipeline_factory.AzureOpenAIProvider')
    def test_create_non_pii_llm(self, mock_azure, factory):
        """Test non-PII LLM provider creation."""
        mock_llm = Mock()
        mock_azure.return_value = mock_llm

        result = factory._create_non_pii_llm()

        mock_azure.assert_called_once_with(
            model_name='gpt-4.1-nano', azure_endpoint='https://test.openai.azure.com', api_key='test-key'
        )
        assert result == mock_llm

    @patch('src.infrastructure.factories.pipeline_factory.SmartDataLoader')
    @patch('src.infrastructure.factories.pipeline_factory.AzureOpenAIProvider')
    @patch('src.infrastructure.factories.pipeline_factory.PromptManager')
    def test_create_pipeline_with_different_models(
        self, mock_prompt_manager, mock_azure, mock_data_loader, mock_config
    ):
        """Test pipeline creation with different model names for each step."""
        mock_config.PII_DETECT_MODEL = 'gpt-4.1'
        mock_config.PII_REFLECT_MODEL = 'gpt-4.1-mini'
        mock_config.NON_PII_DETECT_MODEL = 'gpt-5-nano'
        factory = PipelineFactory(mock_config)

        # Setup mocks
        mock_data_loader.return_value = Mock()
        mock_azure.return_value = Mock()
        mock_prompt_manager.return_value = Mock()

        # Create pipeline
        _ = factory.create_pipeline()

        # Verify all three LLM providers were created with different models
        assert mock_azure.call_count == 4
        calls = mock_azure.call_args_list
        assert calls[0][1]['model_name'] == 'gpt-4.1'
        assert calls[1][1]['model_name'] == 'gpt-4.1-mini'
        assert calls[2][1]['model_name'] == 'gpt-5-nano'

    def test_factory_logging(self, mock_config, caplog):
        """Test that factory logs appropriately during pipeline creation."""
        import logging

        with (
            patch('src.infrastructure.factories.pipeline_factory.SmartDataLoader'),
            patch('src.infrastructure.factories.pipeline_factory.AzureOpenAIProvider'),
            patch('src.infrastructure.factories.pipeline_factory.PromptManager'),
        ):
            factory = PipelineFactory(mock_config)

            with caplog.at_level(logging.INFO, logger='src.infrastructure.factories.pipeline_factory'):
                _ = factory.create_pipeline()

            assert 'Creating processing pipeline' in caplog.text
            assert 'Pipeline created successfully' in caplog.text
