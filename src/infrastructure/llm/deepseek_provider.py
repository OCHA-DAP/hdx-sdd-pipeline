"""DeepSeek V4 Flash LLM provider."""

import json
import logging
from typing import Any, Tuple

from openai import OpenAI

from .llm_provider import ILLMProvider

logger = logging.getLogger(__name__)


class DeepSeekProvider(ILLMProvider):
    """DeepSeek V4 Flash via an OpenAI-compatible endpoint."""

    _MODEL = 'DeepSeek-V4-Flash'

    def __init__(self, endpoint: str, api_key: str, model_name: str = _MODEL):
        self._model = model_name
        self.client = OpenAI(base_url=endpoint, api_key=api_key)
        logger.info('Initialized DeepSeekProvider: model=%s, endpoint=%s', self._model, endpoint)

    @property
    def model_name(self) -> str:
        return self._model

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _call(
        self,
        messages: list[dict],
        temperature: float,
        max_tokens: int,
        **kwargs,
    ):
        return self.client.chat.completions.create(
            model=self._model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs,
        )

    @staticmethod
    def _token_counts(completion) -> Tuple[int, int]:
        if completion.usage:
            return completion.usage.completion_tokens, completion.usage.prompt_tokens
        return 0, 0

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def generate(
        self,
        prompt: str,
        system: str | None = None,
        temperature: float = 0.7,
        max_tokens: int = 24,
        **kwargs,
    ) -> Tuple[str, int, int]:
        messages = []
        if system:
            messages.append({'role': 'system', 'content': system})
        messages.append({'role': 'user', 'content': prompt})

        completion = self._call(messages, temperature, max_tokens, **kwargs)
        text = completion.choices[0].message.content
        completion_tokens, prompt_tokens = self._token_counts(completion)

        logger.debug(
            'generate: completion_tokens=%s, prompt_tokens=%s',
            completion_tokens,
            prompt_tokens,
        )
        return text, completion_tokens, prompt_tokens

    def generate_json(
        self,
        prompt: str,
        system: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 2048,
        **kwargs,
    ) -> Tuple[Any, int, int]:
        _json_instruction = 'Always respond with valid JSON only — no markdown fences, no explanation, no extra text.'
        if system:
            effective_system = f'{system}\n\n{_json_instruction}'
        else:
            effective_system = _json_instruction

        raw, completion_tokens, prompt_tokens = self.generate(
            prompt=prompt,
            system=effective_system,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs,
        )

        cleaned = raw.strip()
        if cleaned.startswith('```'):
            cleaned = cleaned.split('\n', 1)[-1]
            cleaned = cleaned.rsplit('```', 1)[0]

        return json.loads(cleaned), completion_tokens, prompt_tokens
