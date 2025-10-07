# src/classifiers/non_pii_classifier.py
import logging
from typing import Any, Dict, Optional

from .base_classifier import BaseClassifier

logger = logging.getLogger(__name__)


class NonPIIClassifier(BaseClassifier):
    """
    Classify sensitivity level for non-PII tables.
    """

    def classify(
        self,
        table_context: str,
        isp: Optional[Dict[str, Any]] = None,
        max_new_tokens: int = 512,
        version: str = "v0",
    ) -> Dict[str, Any]:
        context = {"table_context": table_context, "isp": isp or {}}

        try:
            prediction = self._run_prompt(
                "non_pii_detection", context, version, max_new_tokens
            )
            sensitivity_level = self._map_sensitivity(prediction)
            success = sensitivity_level != "UNDETERMINED"
            return self._standardize_output(
                "NON_PII_SENSITIVITY", sensitivity_level, prediction, success
            )
        except Exception as e:
            logger.exception("Non-PII table sensitivity classification failed")
            return self._standardize_output(
                "NON_PII_SENSITIVITY", "ERROR_GENERATION", str(e), success=False
            )
