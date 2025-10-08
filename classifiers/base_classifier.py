# src/classifiers/base_classifier.py
import logging
from typing import Any, Dict, Optional

from llm_model import AzureOpenAIStrategy
from utilities.prompt_manager import PromptManager

logger = logging.getLogger(__name__)


class BaseClassifier:
    """
    Base class that provides common functionality for all classifiers.
    - Prompt rendering
    - Model generation
    - Output standardization
    - Sensitivity mapping
    """

    _SENSITIVITY_KEYWORDS = {
        "non_sensitive": "NON_SENSITIVE",
        "medium_sensitive": "MEDIUM_SENSITIVE",
        "moderate_sensitive": "MODERATE_SENSITIVE",
        "high_sensitive": "HIGH_SENSITIVE",
        "severe_sensitive": "SEVERE_SENSITIVE",
    }

    def __init__(self, model_name: str):
        self.model_name = model_name
        self.model_instance = AzureOpenAIStrategy(self.model_name)
        (
            self.model,
            self.tokenizer,
            self.client,
            self.model_type,
        ) = self.model_instance.get_model_components()

        self.generate = self.model_instance.generate
        self.prompt_manager = PromptManager()

    # ---------------------------------------------------------------------
    # 🧰 Helper Methods
    # ---------------------------------------------------------------------

    @staticmethod
    def _standardize_output(
        classification_type: str,
        value: str,
        raw_model_output: Any,
        success: bool = True,
    ) -> Dict[str, Any]:
        """Return standardized classification output."""
        return {
            "classification_type": classification_type,
            "value": value,
            "raw_model_output": (raw_model_output.strip() if isinstance(raw_model_output, str) else raw_model_output),
            "success": success,
        }

    def _run_prompt(
        self,
        prompt_name: str,
        context: Dict[str, Any],
        version: str = "v0",
        max_new_tokens: int = 256,
    ) -> str:
        """Render a Jinja prompt and run the model."""
        prompt = self.prompt_manager.get_prompt(prompt_name=prompt_name, version=version, context=context)
        return self.generate(prompt, max_new_tokens=max_new_tokens).strip()

    def _map_sensitivity(self, prediction: str) -> str:
        """Map model output text to standardized sensitivity levels."""
        pred_lower = prediction.lower()
        for keyword, level in self._SENSITIVITY_KEYWORDS.items():
            if keyword in pred_lower:
                return level
        return "UNDETERMINED"

    @staticmethod
    def _has_alphanumeric(values: list) -> bool:
        """Check if any value contains at least one letter or digit."""
        return any(any(char.isalpha() or char.isdigit() for char in str(value)) for value in values)
