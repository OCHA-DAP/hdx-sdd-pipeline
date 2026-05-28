"""Pipeline factory for creating configured processing pipelines."""

import logging
from typing import Optional

from src.application.process_dataset import ProcessDatasetUseCase
from src.infrastructure.openai_provider import OpenAIProvider
from src.infrastructure.data_loader import SmartDataLoader
from src.shared.utils.prompt_manager import PromptManager
from config.config import Config

logger = logging.getLogger(__name__)


class PipelineFactory:
    """
    Factory for creating processing pipelines with configuration-based dependencies.

    This centralizes all the conditional logic for enabling/disabling pipeline steps,
    making the main event processor cleaner and more maintainable.
    """

    def __init__(self, config: Config):
        """
        Initialize factory with configuration.

        Args:
            config: Configuration object with feature flags
        """
        self.config = config
        self._log_configuration()

    def _log_configuration(self):
        """Log the current pipeline configuration."""
        logger.info('===========================================')
        logger.info('Pipeline Configuration:')
        logger.info(f'  Personal data detection: {self.config.PERSONAL_DATA_DETECTION}')
        logger.info(f'  Personal data reflection: {self.config.PERSONAL_DATA_REFLECTION}')
        logger.info(f'  Non-personal data detection: {self.config.NON_PERSONAL_DATA_DETECTION}')
        logger.info(f'  README scan: {self.config.README_SCAN}')
        logger.info(f'  CKAN update: {self.config.CKAN_UPDATE}')
        logger.info('===========================================')

    def create_pipeline(self, sample_size: int = 5) -> ProcessDatasetUseCase:
        """
        Create a configured processing pipeline.

        Args:
            sample_size: Number of samples per column

        Returns:
            Configured ProcessDatasetUseCase instance
        """
        logger.info('Creating processing pipeline...')

        # Always create data loader
        data_loader = SmartDataLoader(
            max_rows=1000,
            user_agent=self.config.SDD_USER_AGENT,
            hdx_base_url=self.config.HDX_URL,
        )

        # Create LLM providers based on config
        pii_llm = self._create_pii_llm() if self.config.PERSONAL_DATA_DETECTION else None
        pii_reflection_llm = self._create_pii_reflection_llm() if self.config.PERSONAL_DATA_REFLECTION else None
        non_pii_llm = self._create_non_pii_llm() if self.config.NON_PERSONAL_DATA_DETECTION else None
        readme_llm = self._create_readme_llm() if self.config.README_SCAN else None

        # Create prompt manager
        prompt_manager = PromptManager(prompts_dir='src/prompts')

        # Create and return use case
        pipeline = ProcessDatasetUseCase(
            data_loader=data_loader,
            pii_llm_provider=pii_llm,
            pii_reflection_llm_provider=pii_reflection_llm,
            non_pii_llm_provider=non_pii_llm,
            readme_llm_provider=readme_llm,
            prompt_manager=prompt_manager,
            sample_size=sample_size,
        )

        logger.info('Pipeline created successfully')
        return pipeline

    def _create_pii_llm(self) -> Optional[OpenAIProvider]:
        """Create PII detection LLM provider."""
        return self._get_llm_provider(self.config.PII_DETECT_MODEL)

    def _create_pii_reflection_llm(self) -> Optional[OpenAIProvider]:
        """Create PII reflection LLM provider."""
        return self._get_llm_provider(self.config.PII_REFLECT_MODEL)

    def _create_non_pii_llm(self) -> Optional[OpenAIProvider]:
        """Create non-PII detection LLM provider."""
        return self._get_llm_provider(self.config.NON_PII_DETECT_MODEL)

    def _create_readme_llm(self) -> Optional[OpenAIProvider]:
        """Create README scan LLM provider."""
        return self._get_llm_provider(self.config.README_SCAN_MODEL)

    def _get_llm_provider(self, model_name: str) -> OpenAIProvider:
        """Resolve and create the appropriate LLM provider."""

        logger.info(f'Creating OpenAIProvider for model {model_name}')
        return OpenAIProvider(
            model_name=model_name,
            endpoint=self.config.OPENAI_ENDPOINT,
            api_key=self.config.OPENAI_API_KEY,
        )
