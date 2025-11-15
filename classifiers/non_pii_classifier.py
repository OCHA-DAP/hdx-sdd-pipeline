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
        version: str = 'v1',
    ) -> SDDReport:
        """Classify the sensitivity level of non-PII sensitive data."""
        if isp is None:
            raise ValueError('ISP is required')
        isp_name = list(isp.keys())[0]
        context = {'table_markdown': table_markdown, 'isp': isp[isp_name]}

        try:
            if report.non_pii is not None:
                return report
            prediction, completion_tokens, prompt_tokens = self._run_prompt(
                'non_pii_detection',
                context,
                version,
                max_new_tokens,
                json_response_format=True,
            )
            if isinstance(completion_tokens, int):
                report.completion_tokens += completion_tokens
            if isinstance(prompt_tokens, int):
                report.prompt_tokens += prompt_tokens
            report.add_non_pii_report(
                NonPIIReport(
                    model_name=self.model_name,
                    isp_used=isp_name,
                    sensitivity=prediction['sensitivity'],
                    sensitive_columns=prediction['sensitive_columns'],
                    cited_isp_rules=prediction['cited_isp_rules'],
                    explanation=prediction['explanation'],
                )
            )
            return report
        except Exception as e:
            logger.exception('Non-PII table sensitivity classification failed: %s', str(e))
            return report
