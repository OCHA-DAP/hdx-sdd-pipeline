# src/classifiers/pii_sensitivity_classifier.py
import logging
from typing import Any, Dict

from .base_classifier import BaseClassifier

logger = logging.getLogger(__name__)


class PIIReflectionClassifier(BaseClassifier):
    """
    Classify the sensitivity level of detected PII entities.
    """

    def classify(
        self,
        column_name: str,
        context: str,
        pii_entity: str,
        max_new_tokens: int = 128,
        version: str = "v0",
    ) -> Dict[str, Any]:
        if pii_entity == "None":
            return self._standardize_output(
                "PII_SENSITIVITY",
                "NON_SENSITIVE",
                "PII Entity = None",
            )

        jinja_context = {
            "column_name": column_name,
            "context": context,
            "pii_entity": pii_entity,
        }

        try:
            prediction = self._run_prompt(
                "pii_reflection", jinja_context, version, max_new_tokens
            )
            sensitivity_level = self._map_sensitivity(prediction)
            success = sensitivity_level != "UNDETERMINED"
            return self._standardize_output(
                "PII_SENSITIVITY", sensitivity_level, prediction, success
            )
        except Exception as e:
            logger.exception("PII sensitivity classification failed")
            return self._standardize_output(
                "PII_SENSITIVITY", "ERROR_GENERATION", str(e), success=False
            )
