# src/classifiers/pii_sensitivity_classifier.py
import logging
from typing import Any
from .base_classifier import BaseClassifier

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
            return prediction, completion_tokens, prompt_tokens
        except Exception as e:
            logger.exception('ReadMe scan classification failed: %s', str(e))
            return False, 0, 0
