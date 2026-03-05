"""Azure OpenAI LLM Provider implementation."""

import json
import logging
from typing import Tuple, Dict, Any

from openai import AzureOpenAI

from ...application.interfaces.llm_provider import ILLMProvider
from ...domain.exceptions import LLMProviderError

logger = logging.getLogger(__name__)

DETERMINISTIC_SEED = 42


class AzureOpenAIProvider(ILLMProvider):
    """
    Azure OpenAI implementation of ILLMProvider.

    This adapter wraps the Azure OpenAI API and implements our domain interface.
    """

    def __init__(self, model_name: str, azure_endpoint: str, api_key: str, api_version: str = '2024-02-15-preview'):
        """
        Initialize Azure OpenAI provider.

        Args:
            model_name: Name of the model to use
            azure_endpoint: Azure OpenAI endpoint URL
            api_key: API key for authentication
            api_version: API version to use
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
            self.client = AzureOpenAI(api_key=api_key, api_version=api_version, azure_endpoint=azure_endpoint)
            logger.info("Successfully initialized Azure OpenAI client for model '%s'", model_name)
        except Exception as e:
            logger.error(
                'Failed to initialize Azure OpenAI client: %s',
                e,
                exc_info=True,
                extra={'model': model_name, 'endpoint': azure_endpoint},
            )
            raise LLMProviderError(f'Azure OpenAI initialization failed: {e}') from e

    @property
    def model_name(self) -> str:
        """Get the model name being used."""
        return self._model_name

    def generate(
        self, prompt: str, max_tokens: int = 256, temperature: float = 0.0, seed: int = DETERMINISTIC_SEED, **kwargs
    ) -> Tuple[str, int, int]:
        """
        Generate text completion from prompt.

        Args:
            prompt: Input prompt
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature
            **kwargs: Additional Azure-specific parameters

        Returns:
            Tuple of (generated_text, completion_tokens, prompt_tokens)

        Raises:
            LLMProviderError: If API call fails
        """
        logger.debug(
            'Generating text: model=%s, max_tokens=%s, temperature=%s, prompt_length=%s',
            self._model_name,
            max_tokens,
            temperature,
            len(prompt),
        )

        try:
            if 'gpt-5' in self.model_name:
                response = self.client.chat.completions.create(
                    messages=[{'role': 'user', 'content': prompt}],
                    max_completion_tokens=max_tokens + 512,
                    reasoning_effort='minimal',
                    model=self.model_name,
                    seed=seed,
                )
            else:
                response = self.client.chat.completions.create(
                    model=self._model_name,
                    messages=[{'role': 'user', 'content': prompt}],
                    max_tokens=max_tokens,
                    temperature=temperature,
                    seed=seed,
                    **kwargs,
                )

            generated_text = response.choices[0].message.content
            completion_tokens = response.usage.completion_tokens
            prompt_tokens = response.usage.prompt_tokens
            total_tokens = completion_tokens + prompt_tokens

            logger.debug(
                'Generation successful: completion_tokens=%s, prompt_tokens=%s, total_tokens=%s',
                completion_tokens,
                prompt_tokens,
                total_tokens,
            )

            if total_tokens > 1000:
                logger.warning('High token usage: %s tokens (model=%s)', total_tokens, self._model_name)

            return generated_text, completion_tokens, prompt_tokens

        except Exception as e:
            logger.error(
                'Azure OpenAI generation failed: %s',
                e,
                exc_info=True,
                extra={'model': self._model_name, 'prompt_length': len(prompt)},
            )
            raise LLMProviderError(f'LLM generation failed: {e}') from e

    def generate_json(
        self, prompt: str, max_tokens: int = 512, temperature: float = 0.0, seed: int = DETERMINISTIC_SEED, **kwargs
    ) -> Tuple[Dict[str, Any], int, int]:
        """
        Generate JSON response from prompt.

        Args:
            prompt: Input prompt
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature
            **kwargs: Additional Azure-specific parameters

        Returns:
            Tuple of (json_response, completion_tokens, prompt_tokens)

        Raises:
            LLMProviderError: If API call fails or response is not valid JSON dictionary
        """
        logger.debug(
            'Generating JSON: model=%s, max_tokens=%s, temperature=%s, prompt_length=%s',
            self._model_name,
            max_tokens,
            temperature,
            len(prompt),
        )

        try:
            if 'gpt-5' in self.model_name.lower():
                response = self.client.chat.completions.create(
                    messages=[{'role': 'user', 'content': prompt}],
                    max_completion_tokens=max_tokens + 1024,
                    reasoning_effort='minimal',
                    model=self.model_name,
                    response_format={'type': 'json_object'},
                    seed=seed,
                )
            else:
                response = self.client.chat.completions.create(
                    model=self._model_name,
                    messages=[{'role': 'user', 'content': prompt}],
                    max_tokens=max_tokens,
                    temperature=temperature,
                    response_format={'type': 'json_object'},
                    seed=seed,
                    **kwargs,
                )

            generated_text = response.choices[0].message.content

            completion_tokens = response.usage.completion_tokens
            prompt_tokens = response.usage.prompt_tokens

            # Parse JSON response
            try:
                json_response = json.loads(generated_text)

                # Ensure response is a dictionary
                if not isinstance(json_response, dict):
                    logger.error(
                        'JSON response is not a dictionary: got %s',
                        type(json_response).__name__,
                        extra={'response_text': generated_text[:200]},
                    )
                    raise LLMProviderError(f'Expected JSON object/dictionary, got {type(json_response).__name__}')

                logger.debug(
                    'JSON generation successful: completion_tokens=%s, prompt_tokens=%s, keys=%s',
                    completion_tokens,
                    prompt_tokens,
                    list(json_response.keys()),
                )
            except json.JSONDecodeError as e:
                logger.error('Failed to parse JSON response: %s', e, extra={'response_text': generated_text[:200]})
                raise LLMProviderError(f'Invalid JSON response: {e}') from e

            return json_response, completion_tokens, prompt_tokens

        except LLMProviderError:
            raise
        except Exception as e:
            logger.error(
                'Azure OpenAI JSON generation failed: %s',
                e,
                exc_info=True,
                extra={'model': self._model_name, 'prompt_length': len(prompt)},
            )
            raise LLMProviderError(f'LLM JSON generation failed: {e}') from e
