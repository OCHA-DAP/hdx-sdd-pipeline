"""Azure OpenAI LLM provider."""

import json
import logging
from typing import Any, Tuple

from openai import AzureOpenAI

from .llm_provider import ILLMProvider
from ...domain.exceptions import LLMProviderError

logger = logging.getLogger(__name__)

DETERMINISTIC_SEED = 42


class AzureOpenAIProvider(ILLMProvider):
    """Azure OpenAI implementation of ILLMProvider."""

    def __init__(
        self,
        model_name: str,
        azure_endpoint: str,
        api_key: str,
        api_version: str = '2024-02-15-preview',
    ):
        self._model_name = model_name
        logger.info(
            'Initializing AzureOpenAIProvider: model=%s, endpoint=%s, api_version=%s',
            model_name,
            azure_endpoint,
            api_version,
        )
        try:
            self.client = AzureOpenAI(api_key=api_key, api_version=api_version, azure_endpoint=azure_endpoint)
        except Exception as e:
            raise LLMProviderError(f'Azure OpenAI initialization failed: {e}') from e

    @property
    def model_name(self) -> str:
        return self._model_name

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_reasoning_model(self) -> bool:
        return 'gpt-5' in self._model_name.lower()

    def _call(self, messages, max_tokens, temperature, seed, response_format=None, **kwargs):
        extra = {'response_format': response_format} if response_format else {}
        if self._is_reasoning_model():
            return self.client.chat.completions.create(
                model=self._model_name,
                messages=messages,
                max_completion_tokens=max_tokens + (1024 if response_format else 512),
                reasoning_effort='minimal',
                seed=seed,
                **extra,
            )
        return self.client.chat.completions.create(
            model=self._model_name,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            seed=seed,
            **extra,
            **kwargs,
        )

    @staticmethod
    def _token_counts(response) -> Tuple[int, int]:
        if not hasattr(response, 'usage') or response.usage is None:
            return 0, 0
        return response.usage.completion_tokens, response.usage.prompt_tokens

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def generate(
        self,
        prompt: str,
        system: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 256,
        seed: int = DETERMINISTIC_SEED,
        **kwargs,
    ) -> Tuple[str, int, int]:
        messages = []
        if system:
            messages.append({'role': 'system', 'content': system})
        messages.append({'role': 'user', 'content': prompt})

        try:
            response = self._call(messages, max_tokens, temperature, seed, **kwargs)
        except Exception as e:
            raise LLMProviderError(f'LLM generation failed: {e}') from e

        text = response.choices[0].message.content
        completion_tokens, prompt_tokens = self._token_counts(response)
        return text, completion_tokens, prompt_tokens

    def generate_json(
        self,
        prompt: str,
        system: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 512,
        seed: int = DETERMINISTIC_SEED,
        **kwargs,
    ) -> Tuple[Any, int, int]:
        messages = []
        if system:
            messages.append({'role': 'system', 'content': system})
        messages.append({'role': 'user', 'content': prompt})

        try:
            response = self._call(
                messages,
                max_tokens,
                temperature,
                seed,
                response_format={'type': 'json_object'},
                **kwargs,
            )
        except Exception as e:
            raise LLMProviderError(f'LLM JSON generation failed: {e}') from e

        text = response.choices[0].message.content
        completion_tokens, prompt_tokens = self._token_counts(response)

        try:
            parsed = json.loads(text)
            if not isinstance(parsed, dict):
                raise LLMProviderError(f'Expected JSON object, got {type(parsed).__name__}')
        except json.JSONDecodeError as e:
            raise LLMProviderError(f'Invalid JSON response: {e}') from e

        return parsed, completion_tokens, prompt_tokens
