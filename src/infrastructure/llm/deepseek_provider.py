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
        try:
            raw_response = self.client.chat.completions.with_raw_response.create(
                model=self._model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                **kwargs,
            )
            headers = dict(raw_response.http_response.headers)

            completion = raw_response.parse()

            choices_data = []
            if hasattr(completion, 'choices') and completion.choices:
                for c in completion.choices:
                    msg = getattr(c, 'message', None)
                    choices_data.append(
                        {
                            'finish_reason': getattr(c, 'finish_reason', None),
                            'message_role': getattr(msg, 'role', None) if msg else None,
                            'message_content': getattr(msg, 'content', None) if msg else None,
                        }
                    )

            return completion

        except Exception as e:
            logger.error('DeepSeek API call failed: %s', str(e), exc_info=True)
            if hasattr(e, 'response'):
                status = getattr(e.response, 'status_code', None)
                body = getattr(e.response, 'text', None)
                headers = getattr(e.response, 'headers', None)

                # Detect Azure Responsible AI filter trigger
                rai_invoked = False
                if headers:
                    # Headers are dict-like but key lookup could be case-sensitive depending on the type
                    rai_header = next((v for k, v in headers.items() if k.lower() == 'x-ms-rai-invoked'), None)
                    if rai_header == 'true':
                        rai_invoked = True

                if rai_invoked or (status == 404 and body and 'Not Found' in body):
                    logger.error(
                        '⚠️ [AZURE CONTENT SAFETY TRIGGERED] The request triggered Azure\'s Responsible AI (RAI) safety '  # noqa: S106
                        'filters. Azure MaaS endpoints bizarrely return 404 Not Found (or filter responses) when RAI '  # noqa: S106
                        'is invoked. Status: %s, x-ms-rai-invoked: %s',
                        status,
                        rai_header,
                    )

                logger.error('Error response: status=%s, body=%s, headers=%s', status, body, headers)
            raise

    @staticmethod
    def _token_counts(completion) -> Tuple[int, int]:
        if hasattr(completion, 'usage') and completion.usage:
            return getattr(completion.usage, 'completion_tokens', 0), getattr(completion.usage, 'prompt_tokens', 0)
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
            completion = self._call(messages, temperature, max_tokens, **kwargs)
        except Exception as e:
            logger.error('DeepSeek generate failed: %s', str(e))
            return 'UNDETERMINED', 0, 0

        if not hasattr(completion, 'choices') or not completion.choices:
            logger.error('DeepSeek generate returned no choices. completion=%s', completion)
            return 'UNDETERMINED', 0, 0

        choice = completion.choices[0]
        if not hasattr(choice, 'message') or not choice.message:
            logger.error('DeepSeek generate choice has no message. choice=%s', choice)
            return 'UNDETERMINED', 0, 0

        text = choice.message.content
        if text is None:
            logger.error('DeepSeek generate message content is None. message=%s', choice.message)
            # Sometimes models return content in other fields or it's just empty
            text = 'UNDETERMINED'

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
        _json_instruction = 'Always respond with valid JSON only — no markdown fences, no explanation, no extra text.'
        if system:
            effective_system = f'{system}\n\n{_json_instruction}'
        else:
            effective_system = _json_instruction

        try:
            raw, completion_tokens, prompt_tokens = self.generate(
                prompt=prompt,
                system=effective_system,
                temperature=temperature,
                max_tokens=max_tokens,
                **kwargs,
            )
        except Exception as e:
            logger.error('DeepSeek generate_json failed: %s', str(e))
            return {'error': 'UNDETERMINED'}, 0, 0

        logger.info('DeepSeekProvider.generate_json: raw=%s', raw)

        if raw == 'UNDETERMINED':
            return {'error': 'UNDETERMINED'}, completion_tokens, prompt_tokens

        cleaned = raw.strip()
        if cleaned.startswith('```'):
            cleaned = cleaned.split('\n', 1)[-1]
            cleaned = cleaned.rsplit('```', 1)[0]

        try:
            return json.loads(cleaned), completion_tokens, prompt_tokens
        except json.JSONDecodeError as e:
            logger.error('DeepSeek JSON decode error: %s on content: %s', e, cleaned)
            return {'error': 'UNDETERMINED'}, completion_tokens, prompt_tokens
