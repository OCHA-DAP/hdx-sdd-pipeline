"""Azure OpenAI LLM Provider implementation."""

import json
import logging
from typing import Tuple, Dict, Any

from openai import AzureOpenAI, OpenAI

from ...application.interfaces.llm_provider import ILLMProvider
from ...domain.exceptions import LLMProviderError

logger = logging.getLogger(__name__)

DETERMINISTIC_SEED = 42

# Models served via Azure AI Foundry (serverless/inference endpoints).
# These do not support all Azure OpenAI parameters (e.g. seed).
_FOUNDRY_MODELS = {'deepseek-v4-flash'}


def _is_foundry_model(model_name: str) -> bool:
    return model_name.lower() in _FOUNDRY_MODELS


def _is_reasoning_model(model_name: str) -> bool:
    return 'gpt-5' in model_name.lower()


class AzureOpenAIProvider(ILLMProvider):
    """
    Azure OpenAI implementation of ILLMProvider.

    Supports both:
    - Azure OpenAI Service models (GPT-4o, GPT-5, …) via AzureOpenAI client
    - Azure AI Foundry serverless models (DeepSeek-V4-Flash, …) via OpenAI client
    """

    def __init__(
        self,
        model_name: str,
        azure_endpoint: str,
        api_key: str,
        api_version: str = '2024-02-15-preview',
        foundry_endpoint: str | None = None,
        foundry_api_key: str | None = None,
    ):
        """
        Initialize Azure OpenAI provider.

        Args:
            model_name: Name of the model to use
            azure_endpoint: Azure OpenAI Service endpoint URL
            api_key: API key for Azure OpenAI Service
            api_version: API version for Azure OpenAI Service
            foundry_endpoint: Azure AI Foundry endpoint URL (required for Foundry models)
            foundry_api_key: API key for Azure AI Foundry (falls back to api_key if omitted)
        """
        self._model_name = model_name
        self.azure_endpoint = azure_endpoint
        self.api_key = api_key
        self.api_version = api_version

        logger.info(
            'Initializing Azure OpenAI provider: model=%s, endpoint=%s, api_version=%s',
            model_name,
            azure_endpoint,
            api_version,
        )

        try:
            self.client = AzureOpenAI(
                api_key=api_key,
                api_version=api_version,
                azure_endpoint=azure_endpoint,
            )

            if foundry_endpoint:
                self.foundry_client = OpenAI(
                    base_url=foundry_endpoint.rstrip('/') + '/',
                    api_key=foundry_api_key or api_key,
                )
                logger.info("Initialized Foundry client for endpoint '%s'", foundry_endpoint)
            else:
                self.foundry_client = None

            logger.info("Successfully initialized Azure OpenAI client for model '%s'", model_name)
        except Exception as e:
            logger.error('Failed to initialize Azure OpenAI client: %s', e, exc_info=True)
            raise LLMProviderError(f'Azure OpenAI initialization failed: {e}') from e

    def _get_client(self) -> AzureOpenAI | OpenAI:
        """Return the appropriate client for the current model."""
        if _is_foundry_model(self._model_name):
            if self.foundry_client is None:
                raise LLMProviderError(
                    f"Model '{self._model_name}' requires a Foundry client. "
                    "Pass foundry_endpoint (and optionally foundry_api_key) to the constructor."
                )
            logger.debug(
                'Using Foundry client: base_url=%s, model=%s (seed param disabled)',
                self.foundry_client.base_url,
                self._model_name,
            )
            return self.foundry_client
        return self.client

    @property
    def model_name(self) -> str:
        """Get the model name being used."""
        return self._model_name

    def generate(
        self,
        prompt: str,
        max_tokens: int = 256,
        temperature: float = 0.0,
        seed: int = DETERMINISTIC_SEED,
        **kwargs,
    ) -> Tuple[str, int, int]:
        """
        Generate text completion from prompt.

        Args:
            prompt: Input prompt
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature
            seed: Random seed (ignored for Foundry models)
            **kwargs: Additional provider-specific parameters

        Returns:
            Tuple of (generated_text, completion_tokens, prompt_tokens)

        Raises:
            LLMProviderError: If API call fails
        """
        logger.debug(
            'Generating text: model=%s, max_tokens=%s, temperature=%s, prompt_length=%s',
            self._model_name, max_tokens, temperature, len(prompt),
        )

        try:
            client = self._get_client()
            is_foundry = _is_foundry_model(self._model_name)

            if _is_reasoning_model(self._model_name):
                response = client.chat.completions.create(
                    model=self._model_name,
                    messages=[{'role': 'user', 'content': prompt}],
                    max_completion_tokens=max_tokens + 512,
                    reasoning_effort='minimal',
                    seed=seed,
                )
            else:
                response = client.chat.completions.create(
                    model=self._model_name,
                    messages=[{'role': 'user', 'content': prompt}],
                    max_tokens=max_tokens,
                    temperature=temperature,
                    **({} if is_foundry else {'seed': seed}),
                    **kwargs,
                )

            # in generate
            generated_text = response.choices[0].message.content
            completion_tokens = response.usage.completion_tokens if response.usage else 0
            prompt_tokens = response.usage.prompt_tokens if response.usage else 0
            total_tokens = completion_tokens + prompt_tokens

            logger.debug(
                'Generation successful: completion_tokens=%s, prompt_tokens=%s, total_tokens=%s',
                completion_tokens, prompt_tokens, total_tokens,
            )
            if total_tokens > 1000:
                logger.warning('High token usage: %s tokens (model=%s)', total_tokens, self._model_name)


            return generated_text, completion_tokens, prompt_tokens

        except LLMProviderError:
            raise
        except Exception as e:
            logger.error(
                'Azure OpenAI generation failed: %s', e, exc_info=True,
                extra={'model': self._model_name, 'prompt_length': len(prompt)},
            )
            raise LLMProviderError(f'LLM generation failed: {e}') from e

    def generate_json(
        self,
        prompt: str,
        max_tokens: int = 512,
        temperature: float = 0.0,
        seed: int = DETERMINISTIC_SEED,
        **kwargs,
    ) -> Tuple[Dict[str, Any], int, int]:
        """
        Generate JSON response from prompt.

        Args:
            prompt: Input prompt
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature
            seed: Random seed (ignored for Foundry models)
            **kwargs: Additional provider-specific parameters

        Returns:
            Tuple of (json_response, completion_tokens, prompt_tokens)

        Raises:
            LLMProviderError: If API call fails or response is not valid JSON dictionary
        """
        logger.debug(
            'Generating JSON: model=%s, max_tokens=%s, temperature=%s, prompt_length=%s',
            self._model_name, max_tokens, temperature, len(prompt),
        )

        try:
            client = self._get_client()
            is_foundry = _is_foundry_model(self._model_name)

            if _is_reasoning_model(self._model_name):
                response = client.chat.completions.create(
                    model=self._model_name,
                    messages=[{'role': 'user', 'content': prompt}],
                    max_completion_tokens=max_tokens + 1024,
                    reasoning_effort='minimal',
                    response_format={'type': 'json_object'},
                    seed=seed,
                )
            else:
                response = client.chat.completions.create(
                    model=self._model_name,
                    messages=[{'role': 'user', 'content': prompt}],
                    max_tokens=max_tokens,
                    temperature=temperature,
                    response_format={'type': 'json_object'},
                    **({} if is_foundry else {'seed': seed}),
                    **kwargs,
                )

            generated_text = response.choices[0].message.content
            completion_tokens = response.usage.completion_tokens if response.usage else 0
            prompt_tokens = response.usage.prompt_tokens if response.usage else 0

            try:
                json_response = json.loads(generated_text)

                if not isinstance(json_response, dict):
                    logger.error(
                        'JSON response is not a dictionary: got %s',
                        type(json_response).__name__,
                        extra={'response_text': generated_text[:200]},
                    )
                    raise LLMProviderError(
                        f'Expected JSON object/dictionary, got {type(json_response).__name__}'
                    )

                logger.debug(
                    'JSON generation successful: completion_tokens=%s, prompt_tokens=%s, keys=%s',
                    completion_tokens, prompt_tokens, list(json_response.keys()),
                )

            except json.JSONDecodeError as e:
                logger.error(
                    'Failed to parse JSON response: %s', e,
                    extra={'response_text': generated_text[:200]},
                )
                raise LLMProviderError(f'Invalid JSON response: {e}') from e

            return json_response, completion_tokens, prompt_tokens

        except LLMProviderError:
            raise
        except Exception as e:
            logger.error(
                'Azure OpenAI JSON generation failed: %s', e, exc_info=True,
                extra={'model': self._model_name, 'prompt_length': len(prompt)},
            )
            raise LLMProviderError(f'LLM JSON generation failed: {e}') from e