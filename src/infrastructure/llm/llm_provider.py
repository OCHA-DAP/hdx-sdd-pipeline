"""Abstract base class / interface for all LLM providers."""

from abc import ABC, abstractmethod
from typing import Any, Tuple


class ILLMProvider(ABC):
    """Common interface every LLM provider must implement."""

    @property
    @abstractmethod
    def model_name(self) -> str:
        """Return the model identifier in use."""

    @abstractmethod
    def generate(
        self,
        prompt: str,
        system: str | None = None,
        temperature: float = 0.7,
        max_tokens: int = 2048,
        **kwargs,
    ) -> Tuple[str, int, int]:
        """Generate a plain-text response.

        Returns:
            (generated_text, completion_tokens, prompt_tokens)
        """

    @abstractmethod
    def generate_json(
        self,
        prompt: str,
        system: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 2048,
        **kwargs,
    ) -> Tuple[Any, int, int]:
        """Generate a JSON-parsed response.

        Returns:
            (parsed_object, completion_tokens, prompt_tokens)

        Raises:
            json.JSONDecodeError: if the response is not valid JSON.
            LLMProviderError: on API or validation failures.
        """
