# src/classifiers/pii_sensitivity_classifier.py
import logging
from typing import Any
from .base_classifier import BaseClassifier
from llm_model.azure_strategy import AzureOpenAIStrategy
from utils.exception_handler import handle_exception_wrap

logger = logging.getLogger(__name__)


class ReadMeScanClassifier(BaseClassifier):
    """
    Classify the sensitivity level of a README file.
    """

    def __init__(self, model: AzureOpenAIStrategy):
        super().__init__(model)

    @handle_exception_wrap()
    def classify(self, readme_string: str) -> tuple[dict[str, Any], int, int]:
        """Classify the sensitivity level of detected PII entities."""
        context = {'readme_string': readme_string}
        prediction, completion_tokens, prompt_tokens = self._run_prompt(
            'readme_scan', context, version='v0', max_new_tokens=256, json_response_format=True
        )
        return prediction, completion_tokens, prompt_tokens
