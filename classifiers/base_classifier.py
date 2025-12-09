# src/classifiers/base_classifier.py
import logging
from typing import Any, Dict

from llm_model import AzureOpenAIStrategy
from utils.prompt_manager import PromptManager
from utils.exception_handler import handle_exception_wrap

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
        'non_sensitive': 'NON_SENSITIVE',
        'medium_sensitive': 'MEDIUM_SENSITIVE',
        'moderate_sensitive': 'MODERATE_SENSITIVE',
        'high_sensitive': 'HIGH_SENSITIVE',
        'severe_sensitive': 'SEVERE_SENSITIVE',
    }

    def __init__(self, model: AzureOpenAIStrategy):
        self.model = model
        self.model_name = model.model_name
        self.prompt_manager = PromptManager()

    @staticmethod
    @handle_exception_wrap()
    def _standardize_output(
        classification_type: str,
        value: str,
        raw_model_output: Any,
    ) -> Dict[str, Any]:
        """Return standardized classification output."""
        return {
            'classification_type': classification_type,
            'value': value,
            'raw_model_output': (raw_model_output.strip() if isinstance(raw_model_output, str) else raw_model_output),
            # 'success': success,
        }

    @handle_exception_wrap()
    def _run_prompt(
        self,
        prompt_name: str,
        context: Dict[str, Any],
        version: str = 'v0',
        max_new_tokens: int = 256,
        json_response_format: bool = False,
    ) -> tuple[Any, int, int]:
        """
        Render a Jinja prompt and run the model.

        Raises:
            RuntimeError: If prompt rendering fails (with ERROR_SOURCE_PROMPT_RENDERING context)
            RuntimeError: If Azure generation fails (with ERROR_SOURCE_AZURE_GENERATION or
            ERROR_SOURCE_AZURE_JSON_GENERATION context)
        """
        # Prompt rendering
        prompt = self.prompt_manager.get_prompt(prompt_name=prompt_name, version=version, context=context)

        if json_response_format:
            prediction, completion_tokens, prompt_tokens = self.model.generate_json(
                prompt, max_new_tokens=max_new_tokens
            )
        else:
            prediction, completion_tokens, prompt_tokens = self.model.generate(prompt, max_new_tokens=max_new_tokens)
        return prediction, completion_tokens, prompt_tokens

    @handle_exception_wrap()
    def _map_sensitivity(self, prediction: str) -> str:
        """Map model output text to standardized sensitivity levels."""
        pred_lower = prediction.lower()
        for keyword, level in self._SENSITIVITY_KEYWORDS.items():
            if keyword in pred_lower:
                return level
        return 'UNDETERMINED'
