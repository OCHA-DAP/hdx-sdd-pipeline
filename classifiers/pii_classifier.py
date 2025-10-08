# src/classifiers/pii_classifier.py
import logging
from typing import Any, Dict, List

from utilities.prompt_register import PII_ENTITIES_LIST
from .base_classifier import BaseClassifier

logger = logging.getLogger(__name__)


class PIIClassifier(BaseClassifier):
    """
    Handles detection of PII entities from column names and sample values.
    """

    def classify(
        self,
        column_name: str,
        sample_values: List[Any],
        k: int = 5,
        version: str = 'v0',
    ) -> Dict[str, Any]:
        """Detect PII entity type in a column."""

        if not self._has_alphanumeric(sample_values):
            return self._standardize_output('PII', 'None', 'No alphanumeric content')

        context = {'column_name': column_name, 'sample_values': sample_values[:k]}

        try:
            prediction = self._run_prompt('pii_detection', context, version, max_new_tokens=8)
        except Exception as e:
            logger.exception('PII classification failed')
            return self._standardize_output('PII', 'ERROR_GENERATION', str(e), success=False)

        prediction_lower = prediction.lower()
        if 'none' in prediction_lower:
            return self._standardize_output('PII', 'None', prediction)

        # Prioritize AGE entity last
        if 'AGE' in PII_ENTITIES_LIST:
            PII_ENTITIES_LIST.remove('AGE')
            PII_ENTITIES_LIST.append('AGE')

        for entity in PII_ENTITIES_LIST:
            if entity.lower() in prediction_lower:
                return self._standardize_output('PII', entity, prediction)

        return self._standardize_output('PII', 'UNDETERMINED', prediction, success=False)
