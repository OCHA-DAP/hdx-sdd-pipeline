# src/classifiers/non_pii_classifier.py
import logging
from typing import Any, Dict, Optional
from models.sdd_report import SDDReport, NonPIIReport
from .base_classifier import BaseClassifier

logger = logging.getLogger(__name__)


class NonPIIClassifier(BaseClassifier):
    """Classify the sensitivity level of non-PII sensitive data."""

    def format_prediction(self, prediction: str) -> str:
        """Format the prediction of the non-PII classifier."""
        prediction = prediction.split('\n')[0]  # First line of the prediction
        if 'high_sensitive' in prediction.lower():
            return 'HIGH_SENSITIVE'
        elif 'moderate_sensitive' in prediction.lower():
            return 'MODERATE_SENSITIVE'
        elif 'non_sensitive' in prediction.lower():
            return 'NON_SENSITIVE'
        else:
            return 'UNDETERMINED'

    def classify(
        self,
        table_markdown: str,
        report: SDDReport,
        isp: Optional[Dict[str, Any]] = None,
        max_new_tokens: int = 512,
        version: str = 'v0',
    ) -> Dict[str, Any]:
        """Classify the sensitivity level of non-PII sensitive data."""
        context = {'table_markdown': table_markdown, 'isp': isp['default'] or {}}

        try:
            if report.non_pii is not None:
                return report
            prediction, completion_tokens, prompt_tokens = self._run_prompt(
                'non_pii_detection', context, version, max_new_tokens
            )
            report.completion_tokens += completion_tokens
            report.prompt_tokens += prompt_tokens
            pred_level = self.format_prediction(prediction)
            report.add_non_pii_report(
                NonPIIReport(
                    sensitivity=pred_level,
                    explanation=prediction,
                )
            )
            report.non_pii_classifier_model = self.model_name
            return report
        except Exception as e:
            logger.exception('Non-PII table sensitivity classification failed: %s', str(e))
            return report
