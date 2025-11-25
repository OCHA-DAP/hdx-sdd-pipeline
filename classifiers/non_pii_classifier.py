# src/classifiers/non_pii_classifier.py
import logging
from typing import Any, Dict, Optional
from models.sdd_report import SDDReport, NonPIIReport
from .base_classifier import BaseClassifier
from utils.error_constants import ERROR_SOURCE_NON_PII_CLASSIFICATION

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
            logger.error('ISP is required')
            return report
        isp_name = list(isp.keys())[0]
        context = {'table_markdown': table_markdown, 'isp': isp[isp_name]}

        # Stop processing if there's already an error
        if not report.processing_success:
            logger.error('Processing failed')
            return report

        try:
            if report.non_pii is not None:
                logger.warning('Non-PII report already exists')
                return report
            prediction, completion_tokens, prompt_tokens = self._run_prompt(
                'non_pii_detection',
                context,
                version,
                max_new_tokens,
                json_response_format=True,
            )

            # Check for error indicators in prediction
            if isinstance(prediction, dict) and 'error' in prediction:
                error_msg = f'Non-PII classification failed: {prediction.get("error", "Unknown error")}'
                logger.error(error_msg)
                report.set_error(ERROR_SOURCE_NON_PII_CLASSIFICATION, error_msg)
                return report

            if isinstance(completion_tokens, int):
                report.completion_tokens += completion_tokens
            if isinstance(prompt_tokens, int):
                report.prompt_tokens += prompt_tokens
            report.add_non_pii_report(
                NonPIIReport(
                    model_name=self.model_name,
                    isp_used=isp_name,
                    sensitivity=prediction.get('sensitivity', 'UNDETERMINED'),
                    sensitive_columns=prediction.get('sensitive_columns', []),
                    cited_isp_rules=prediction.get('cited_isp_rules', []),
                    explanation=prediction.get('explanation', ''),
                )
            )
            logger.info('Non-PII classification successful')
            return report
        except Exception as e:
            error_msg = f'Non-PII table sensitivity classification failed: {str(e)}'
            logger.exception(error_msg)
            report.set_error(ERROR_SOURCE_NON_PII_CLASSIFICATION, error_msg)
            return report
