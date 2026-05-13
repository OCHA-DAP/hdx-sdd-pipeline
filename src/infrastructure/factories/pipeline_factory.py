"""Pipeline factory for creating configured processing pipelines."""

import logging
from typing import Optional

from src.application.use_cases.process_dataset import ProcessDatasetUseCase
from src.infrastructure.llm.azure_openai_provider import AzureOpenAIProvider
from src.infrastructure.storage.data_loader import SmartDataLoader
from src.shared.utils.prompt_manager import PromptManager
from config.config import Config

logger = logging.getLogger(__name__)


class PipelineFactory:
    """
    Factory for creating processing pipelines with configuration-based dependencies.
    """

    def __init__(self, config: Config):
        self.config = config
        self._log_configuration()

    def _log_configuration(self):
        logger.info('===========================================')
        logger.info('Pipeline Configuration:')
        logger.info(f'  Personal data detection: {self.config.PERSONAL_DATA_DETECTION}')
        logger.info(f'  Personal data reflection: {self.config.PERSONAL_DATA_REFLECTION}')
        logger.info(f'  Non-personal data detection: {self.config.NON_PERSONAL_DATA_DETECTION}')
        logger.info(f'  README scan: {self.config.README_SCAN}')
        logger.info(f'  CKAN update: {self.config.CKAN_UPDATE}')
        logger.info('===========================================')

    def create_pipeline(self, sample_size: int = 5) -> ProcessDatasetUseCase:
        logger.info('Creating processing pipeline...')

        data_loader = SmartDataLoader(
            max_rows=1000,
            user_agent=self.config.SDD_USER_AGENT,
            hdx_base_url=self.config.HDX_URL,
        )

        pii_llm            = self._create_pii_llm()            if self.config.PERSONAL_DATA_DETECTION    else None
        pii_reflection_llm = self._create_pii_reflection_llm() if self.config.PERSONAL_DATA_REFLECTION   else None
        non_pii_llm        = self._create_non_pii_llm()        if self.config.NON_PERSONAL_DATA_DETECTION else None
        readme_llm         = self._create_readme_llm()         if self.config.README_SCAN                 else None

        prompt_manager = PromptManager(prompts_dir='src/prompts')

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

    # ------------------------------------------------------------------
    # Private helpers — all share the same _make_provider call
    # ------------------------------------------------------------------

    def _make_provider(self, model_name: str) -> AzureOpenAIProvider:
        """Construct a provider, injecting Foundry credentials when available."""
        return AzureOpenAIProvider(
            model_name=model_name,
            azure_endpoint=self.config.AZURE_OPENAI_ENDPOINT,
            api_key=self.config.AZURE_OPENAI_API_KEY,
            foundry_endpoint=getattr(self.config, 'AZURE_FOUNDRY_ENDPOINT', None),
            foundry_api_key=getattr(self.config, 'AZURE_FOUNDRY_API_KEY', None),
        )

    def _create_pii_llm(self) -> Optional[AzureOpenAIProvider]:
        logger.debug(f'Creating PII detection LLM: {self.config.PII_DETECT_MODEL}')
        return self._make_provider(self.config.PII_DETECT_MODEL)

    def _create_pii_reflection_llm(self) -> Optional[AzureOpenAIProvider]:
        logger.debug(f'Creating PII reflection LLM: {self.config.PII_REFLECT_MODEL}')
        return self._make_provider(self.config.PII_REFLECT_MODEL)

    def _create_non_pii_llm(self) -> Optional[AzureOpenAIProvider]:
        logger.debug(f'Creating non-PII detection LLM: {self.config.NON_PII_DETECT_MODEL}')
        return self._make_provider(self.config.NON_PII_DETECT_MODEL)

    def _create_readme_llm(self) -> Optional[AzureOpenAIProvider]:
        logger.debug(f'Creating README scan LLM: {self.config.README_SCAN_MODEL}')
        return self._make_provider(self.config.README_SCAN_MODEL)