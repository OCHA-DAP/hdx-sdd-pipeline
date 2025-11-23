# src/classifiers/base_classifier.py
import logging
from typing import Any, Dict

from llm_model import AzureOpenAIStrategy
from utils.prompt_manager import PromptManager
from utils.main_config import DEBUG

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

    def __init__(self, model_name: str, azure_endpoint: str, api_key: str):
        self.model_name = model_name
        self.prompt_manager = PromptManager()
        self.model = AzureOpenAIStrategy(model_name=model_name, azure_endpoint=azure_endpoint, api_key=api_key)

    # ---------------------------------------------------------------------
    # 🧰 Helper Methods
    # ---------------------------------------------------------------------

    @staticmethod
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
        try:
            prompt = self.prompt_manager.get_prompt(prompt_name=prompt_name, version=version, context=context)
        except Exception as e:
            error_msg = f'Prompt rendering failed for {prompt_name} (v{version}): {str(e)}'
            logger.exception(error_msg)
            raise RuntimeError(error_msg) from e

        if DEBUG:
            return 'DEBUG_MODE', 0, 0

        try:
            if json_response_format:
                prediction, completion_tokens, prompt_tokens = self.model.generate_json(
                    prompt, max_new_tokens=max_new_tokens
                )
            else:
                prediction, completion_tokens, prompt_tokens = self.model.generate(
                    prompt, max_new_tokens=max_new_tokens
                )
            return prediction, completion_tokens, prompt_tokens
        except RuntimeError:
            # Re-raise RuntimeError from Azure (already has context)
            raise
        except Exception as e:
            error_msg = f'Azure generation failed for {prompt_name}: {str(e)}'
            logger.exception(error_msg)
            raise RuntimeError(error_msg) from e

    def _map_sensitivity(self, prediction: str) -> str:
        """Map model output text to standardized sensitivity levels."""
        pred_lower = prediction.lower()
        for keyword, level in self._SENSITIVITY_KEYWORDS.items():
            if keyword in pred_lower:
                return level
        return 'UNDETERMINED'

    @staticmethod
    def _has_alphanumeric(values: list) -> bool:
        """Check if any value contains at least one letter or digit."""
        return any(any(char.isalpha() or char.isdigit() for char in str(value)) for value in values)
