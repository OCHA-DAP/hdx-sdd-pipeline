# src/classifiers/pii_sensitivity_classifier.py
import logging
from typing import Any, Dict

from models.sdd_report import SDDReport
from .base_classifier import BaseClassifier
from tqdm import tqdm

logger = logging.getLogger(__name__)


class PIIReflectionClassifier(BaseClassifier):
    """
    Classify the sensitivity level of detected PII entities.
    """

    def classify_column(
        self,
        column_name: str,
        table_markdown: str,
        column_entity: str,
        max_new_tokens: int = 12,
        version: str = 'v0',
    ) -> Dict[str, Any]:
        if column_entity == 'None':
            return self._standardize_output(
                'PII_SENSITIVITY',
                'NON_SENSITIVE',
                'PII Entity = None',
            )

        jinja_context = {
            'column_name': column_name,
            'table_markdown': table_markdown,
            'column_entity': column_entity,
        }

        try:
            prediction, completion_tokens, prompt_tokens = self._run_prompt(
                'pii_reflection', jinja_context, version, max_new_tokens
            )
            # sensitivity_level = self._map_sensitivity(prediction)

            return prediction, completion_tokens, prompt_tokens
        except Exception as e:
            logger.exception('PII reflection classification failed', e)
            return False, 0, 0

    def classify_df(self, table_markdown: str, report: SDDReport) -> Dict[str, Any]:
        """Classify the sensitivity level of detected PII entities in a DataFrame."""
        for column in tqdm(report.columns, desc='Classifying columns'):
            # Skip if no PII entity type is detected
            if column.pii.get('sensitive') is not None:
                continue
            # Skip if PII entity type is error
            if column.pii.get('entity_type') == 'ERROR' or column.pii.get('entity_type') == 'None':
                pred = False
            else:
                pred, completion_tokens, prompt_tokens = self.classify_column(
                    column_name=column.column_name,
                    table_markdown=table_markdown,
                    column_entity=column.pii.get('entity_type'),
                )
                report.completion_tokens += completion_tokens
                report.prompt_tokens += prompt_tokens
                if pred == 'SENSITIVE':
                    pred = True
                elif pred == 'NON_SENSITIVE':
                    pred = False

            report.update_pii_column(
                column_name=column.column_name, entity_type=column.pii.get('entity_type'), sensitive=pred
            )
            report.pii_reflection_model = self.model_name
        return report
