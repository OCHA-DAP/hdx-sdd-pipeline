"""classifiers/pii_classifier.py: Handles detection of PII entities."""

import logging
from typing import Any, Dict, List
import pandas as pd

from utils.main_config import PII_ENTITIES_LIST
from .base_classifier import BaseClassifier

logger = logging.getLogger(__name__)


class PIIClassifier(BaseClassifier):
    """
    Handles detection of PII entities from column names and sample values.
    """

    def _prepare_context(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Prepare context for PII classification."""
        return df.to_dict(orient='records')

    def _classify_column(
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

    def classify_df(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Classify a DataFrame."""
        # context = self._prepare_context(df)
        for column in df.columns:
            pred = self._classify_column(column, df[column].tolist())
            if pred['entity_type'] != 'None':
                return pred
        return self._standardize_output('PII', 'None', 'No PII detected', success=True)
