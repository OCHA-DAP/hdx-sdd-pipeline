from abc import ABC, abstractmethod
from typing import Optional, Any, Dict
import logging

# import torch


class BaseLLMModel(ABC):
    """
    Abstract base class for LLM model strategies.
    Defines the interface that all model implementations must follow.
    """

    def __init__(self, model_name: str, device: Optional[str] = None, **kwargs):
        self.model_name = model_name
        # self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.device = "cpu"
        self.model = None
        self.tokenizer = None
        self.client = None
        self.model_type = self._get_model_type()
        self._setup_model(**kwargs)

    @abstractmethod
    def _get_model_type(self) -> str:
        """Return the model type identifier."""
        pass

    @abstractmethod
    def _setup_model(self, **kwargs) -> None:
        """Initialize the model, tokenizer, and client as needed."""
        pass

    @abstractmethod
    def generate(self, prompt: str, temperature: float = 0.3, max_new_tokens: int = 8, **kwargs) -> str:
        """Generate text from the given prompt."""
        pass

    def get_model_components(self) -> tuple:
        """Return model components (model, tokenizer, client, model_type)."""
        return self.model, self.tokenizer, self.client, self.model_type

    def is_ready(self) -> bool:
        """Check if the model is ready for inference."""
        if self.model_type in ["openai", "azure", "deepseek"]:
            return self.client is not None
        else:
            return self.model is not None and self.tokenizer is not None

    def _setup_logging(self, logger_name: str) -> None:
        """Setup logging for the model."""
        logging.getLogger(logger_name).setLevel(logging.WARNING)
        logging.getLogger("httpx").setLevel(logging.WARNING)
