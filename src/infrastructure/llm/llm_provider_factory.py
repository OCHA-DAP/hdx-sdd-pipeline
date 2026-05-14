"""Factory for creating LLM provider instances."""

import logging
from enum import Enum

from .llm_provider import ILLMProvider
from .azure_openai_provider import AzureOpenAIProvider
from .deepseek_provider import DeepSeekProvider

logger = logging.getLogger(__name__)


class LLMProviderType(str, Enum):
    AZURE_OPENAI = 'azure_openai'
    DEEPSEEK = 'deepseek'


class LLMProviderFactory:
    """Instantiates the correct ILLMProvider from a config object.

    Add new providers here — callers never import concrete classes directly.

    Usage
    -----
    provider = LLMProviderFactory.create(LLMProviderType.DEEPSEEK, config)
    text, ct, pt = provider.generate("Hello")
    """

    @staticmethod
    def create(provider_type: LLMProviderType | str, config, model: str = None) -> ILLMProvider:
        """Create and return an LLM provider.

        Args:
            provider_type: One of the LLMProviderType enum values (or its string value).
            config:        App config object exposing the required credentials.

        Returns:
            An ILLMProvider-compliant instance.

        Raises:
            ValueError: for unknown provider types.
        """
        key = LLMProviderType(provider_type)
        logger.info('Creating LLM provider: %s', key)

        if key == LLMProviderType.AZURE_OPENAI:
            return AzureOpenAIProvider(
                model_name=config.AZURE_OPENAI_MODEL if model is None else model,
                azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
                api_key=config.AZURE_OPENAI_API_KEY,
                api_version=getattr(config, 'AZURE_OPENAI_API_VERSION', '2024-02-15-preview'),
            )

        if key == LLMProviderType.DEEPSEEK:
            return DeepSeekProvider(
                endpoint=config.DEEPSEEK_ENDPOINT if model is None else model,
                api_key=config.AZURE_OPENAI_API_KEY,
            )

        raise ValueError(f'Unknown LLM provider type: {provider_type!r}')

    @staticmethod
    def register(provider_type: str, constructor):
        """Dynamically register a new provider type at runtime.

        Args:
            provider_type: String key (will be added to LLMProviderType if not present).
            constructor:   Callable(config) -> ILLMProvider.

        Example::

            LLMProviderFactory.register("my_provider", lambda cfg: MyProvider(cfg.MY_KEY))
            provider = LLMProviderFactory.create("my_provider", config)
        """
        LLMProviderFactory._registry[provider_type] = constructor

    # Runtime extension registry (populated via .register())
    _registry: dict = {}

    @classmethod
    def _create_from_registry(cls, key: str, config) -> ILLMProvider | None:
        if key in cls._registry:
            return cls._registry[key](config)
        return None
