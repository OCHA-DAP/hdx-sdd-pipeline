"""OpenAI LLM provider."""

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Tuple

from openai import OpenAI

# Add project root to sys.path if running directly as a script
ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from src.domain.exceptions import LLMProviderError  # noqa: E402

logger = logging.getLogger(__name__)


class OpenAIProvider:
    """OpenAI or OpenAI-compatible endpoint LLM provider."""

    def __init__(
        self,
        model_name: str,
        endpoint: str | None = None,
        api_key: str | None = None,
    ):
        self._model = model_name

        logger.info(
            'Initializing OpenAIProvider: model=%s, base_url=%s',
            self._model,
            endpoint,
        )

        try:
            self.client = OpenAI(
                base_url=endpoint,
                api_key=api_key,
            )
        except Exception as e:
            raise LLMProviderError(f'OpenAI initialization failed: {e}') from e

    @property
    def model_name(self) -> str:
        return self._model

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_reasoning_model(self) -> bool:
        return 'gpt-5' in self._model.lower()

    def _call(
        self,
        messages: list[dict],
        temperature: float,
        max_tokens: int,
        response_format: dict | None = None,
        **kwargs,
    ):
        extra = {'response_format': response_format} if response_format else {}

        try:
            if self._is_reasoning_model():
                reasoning_effort = kwargs.pop('reasoning_effort', 'minimal')
                api_params = {
                    'model': self._model,
                    'messages': messages,
                    **extra,
                }
                if reasoning_effort == 'none':
                    api_params['temperature'] = temperature
                    api_params['max_tokens'] = max_tokens
                    api_params.update(kwargs)
                else:
                    kwargs.pop('temperature', None)
                    kwargs.pop('top_p', None)
                    api_params['reasoning_effort'] = reasoning_effort
                    api_params['max_completion_tokens'] = max_tokens + 8192
                    api_params.update(kwargs)

                raw_response = self.client.chat.completions.with_raw_response.create(**api_params)
            else:
                raw_response = self.client.chat.completions.with_raw_response.create(
                    model=self._model,
                    messages=messages,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    **extra,
                    **kwargs,
                )

            return raw_response.parse()

        except Exception as e:
            logger.error('OpenAI API call failed: %s', str(e), exc_info=True)

            if hasattr(e, 'response'):
                status = getattr(e.response, 'status_code', None)
                body = getattr(e.response, 'text', None)
                headers = getattr(e.response, 'headers', None)

                logger.error(
                    'Error response: status=%s, body=%s, headers=%s',
                    status,
                    body,
                    headers,
                )

            raise

    @staticmethod
    def _token_counts(completion) -> Tuple[int, int]:
        if hasattr(completion, 'usage') and completion.usage:
            return (
                getattr(completion.usage, 'completion_tokens', 0),
                getattr(completion.usage, 'prompt_tokens', 0),
            )

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

        try:
            completion = self._call(
                messages,
                temperature,
                max_tokens,
                **kwargs,
            )
        except Exception as e:
            logger.error('OpenAI generate failed: %s', str(e))
            raise LLMProviderError(f'LLM generation failed: {e}') from e

        if not hasattr(completion, 'choices') or not completion.choices:
            logger.error(
                'OpenAI generate returned no choices. completion=%s',
                completion,
            )
            raise LLMProviderError('LLM generation returned no choices')

        choice = completion.choices[0]

        if not hasattr(choice, 'message') or not choice.message:
            logger.error(
                'OpenAI generate choice has no message. choice=%s',
                choice,
            )
            raise LLMProviderError('LLM generation choice has no message')

        text = choice.message.content

        if text is None:
            logger.error(
                'OpenAI generate message content is None. message=%s',
                choice.message,
            )
            raise LLMProviderError('LLM generation message content is None')

        completion_tokens, prompt_tokens = self._token_counts(completion)

        logger.debug(
            'generate: completion_tokens=%s, prompt_tokens=%s, text_preview=%s...',
            completion_tokens,
            prompt_tokens,
            text[:50] if text else 'None',
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
        messages = []

        if system:
            messages.append({'role': 'system', 'content': system})

        messages.append({'role': 'user', 'content': prompt})

        try:
            completion = self._call(
                messages,
                temperature,
                max_tokens,
                response_format={'type': 'json_object'},
                **kwargs,
            )
        except Exception as e:
            logger.error('OpenAI generate_json failed: %s', str(e))
            raise LLMProviderError(f'LLM JSON generation failed: {e}') from e

        if not hasattr(completion, 'choices') or not completion.choices:
            logger.error(
                'OpenAI generate_json returned no choices. completion=%s',
                completion,
            )
            raise LLMProviderError('LLM JSON generation returned no choices')

        choice = completion.choices[0]

        if not hasattr(choice, 'message') or not choice.message:
            logger.error(
                'OpenAI generate_json choice has no message. choice=%s',
                choice,
            )
            raise LLMProviderError('LLM JSON generation choice has no message')

        raw = choice.message.content

        if raw is None:
            logger.error(
                'OpenAI generate_json message content is None. message=%s',
                choice.message,
            )
            raise LLMProviderError('LLM JSON generation message content is None')

        cleaned = raw.strip()

        if cleaned.startswith('```'):
            cleaned = cleaned.split('\n', 1)[-1]
            cleaned = cleaned.rsplit('```', 1)[0]

        try:
            parsed = json.loads(cleaned)

            if not isinstance(parsed, dict):
                raise LLMProviderError(f'Expected JSON object, got {type(parsed).__name__}')

        except json.JSONDecodeError as e:
            logger.error(
                'OpenAI JSON decode error: %s on content: %s',
                e,
                cleaned,
            )
            raise LLMProviderError(f'Invalid JSON response: {e}') from e

        completion_tokens, prompt_tokens = self._token_counts(completion)

        return parsed, completion_tokens, prompt_tokens


if __name__ == '__main__':
    from dotenv import load_dotenv

    load_dotenv()

    api_key = os.getenv('OPENAI_API_KEY')
    endpoint = os.getenv('OPENAI_ENDPOINT')

    if api_key:
        provider = OpenAIProvider(
            model_name='gpt-4.1-nano',
            api_key=api_key,
            endpoint=endpoint,
        )

        print('Successfully initialized OpenAIProvider.')

        completion, completion_tokens, prompt_tokens = provider.generate(
            'Hello, how are you?',
            temperature=0.7,
            max_tokens=24,
        )

        print(completion)

    else:
        print('OPENAI_API_KEY is not set. Please set it in your .env file or environment.')
