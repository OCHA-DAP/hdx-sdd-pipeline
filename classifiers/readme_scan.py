# src/classifiers/pii_sensitivity_classifier.py
import logging
from typing import Any
from .base_classifier import BaseClassifier
from utils.error_constants import ERROR_SOURCE_README_SCAN

logger = logging.getLogger(__name__)


class ReadMeScanClassifier(BaseClassifier):
    """
    Classify the sensitivity level of a README file.
    """

    def classify_readme(self, readme_string: str) -> dict[str, Any]:
        """Classify the sensitivity level of detected PII entities."""
        context = {'readme_string': readme_string}
        try:
            prediction, completion_tokens, prompt_tokens = self._run_prompt(
                'readme_scan', context, version='v0', max_new_tokens=256, json_response_format=True
            )

            # Check for error indicators
            if isinstance(prediction, dict) and 'error' in prediction:
                error_msg = f'ReadMe scan failed: {prediction.get("error", "Unknown error")}'
                logger.error(error_msg)
                raise RuntimeError(error_msg)

            return prediction, completion_tokens, prompt_tokens
        except Exception as e:
            error_msg = f'ReadMe scan classification failed: {str(e)}'
            logger.exception(error_msg)
            raise RuntimeError(error_msg) from e
