"""LLM Provider interface."""

from abc import ABC, abstractmethod
from typing import Tuple, Any, Dict


class ILLMProvider(ABC):
    """
    Interface for LLM providers.

    This abstraction allows us to swap different LLM implementations
    (Azure OpenAI, OpenAI, local models, etc.) without changing business logic.
    """

    @abstractmethod
    def generate(self, prompt: str, max_tokens: int = 256, temperature: float = 0.0, **kwargs) -> Tuple[str, int, int]:
        """
        Generate text completion from prompt.

        Args:
            prompt: Input prompt
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature
            **kwargs: Additional provider-specific parameters

        Returns:
            Tuple of (generated_text, completion_tokens, prompt_tokens)
        """
        pass

    @abstractmethod
    def generate_json(
        self, prompt: str, max_tokens: int = 256, temperature: float = 0.0, **kwargs
    ) -> Tuple[Dict[str, Any], int, int]:
        """
        Generate JSON response from prompt.

        Args:
            prompt: Input prompt
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature
            **kwargs: Additional provider-specific parameters

        Returns:
            Tuple of (json_response, completion_tokens, prompt_tokens)
        """
        pass

    @property
    @abstractmethod
    def model_name(self) -> str:
        """Get the model name being used."""
        pass
