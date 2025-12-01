# src/classifiers/non_pii_classifier.py
import logging
from models.sdd_report import SDDReport, NonPIIReport
from .base_classifier import BaseClassifier
from llm_model.azure_strategy import AzureOpenAIStrategy
from utils.exception_handler import handle_exception_wrap

logger = logging.getLogger(__name__)


class NonPIIClassifier(BaseClassifier):
    """Classify the sensitivity level of non-PII sensitive data."""

    def __init__(self, model: AzureOpenAIStrategy):
        super().__init__(model)
        self.model = model

    @handle_exception_wrap
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

    @handle_exception_wrap
    def classify(
        self,
        table_markdown: str,
        report: SDDReport,
        isp,
        max_new_tokens: int = 512,
        version: str = 'v1',
    ) -> SDDReport:
        """Classify the sensitivity level of non-PII sensitive data."""
        isp_name = list(isp.keys())[0]
        context = {'table_markdown': table_markdown, 'isp': isp[isp_name]}
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

        return NonPIIReport(
            model_name=self.model.model_name,
            isp_used=isp_name,
            sensitivity=prediction.get('sensitivity', 'UNDETERMINED'),
            sensitive_columns=prediction.get('sensitive_columns', []),
            cited_isp_rules=prediction.get('cited_isp_rules', []),
            explanation=prediction.get('explanation', ''),
        )
