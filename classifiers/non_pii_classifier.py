# src/classifiers/non_pii_classifier.py
import logging
from typing import Dict, Any
from .base_classifier import BaseClassifier
from llm_model.azure_strategy import AzureOpenAIStrategy
from utils.exception_handler import handle_exception_wrap
from utils.utils import table_markdown

logger = logging.getLogger(__name__)


class NonPIIClassifier(BaseClassifier):
    """Classify the sensitivity level of non-PII sensitive data."""

    def __init__(self, model: AzureOpenAIStrategy):
        super().__init__(model)
        self.model = model

    @handle_exception_wrap()
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

    @handle_exception_wrap()
    def classify(
        self,
        sdd_report: Dict[str, Any],
        isp,
        max_new_tokens: int = 512,
        version: str = 'v1',
    ) -> Dict[str, Any]:
        """Classify the sensitivity level of non-PII sensitive data."""
        context = {'table_markdown': table_markdown(sdd_report), 'isp': isp}

        prediction, completion_tokens, prompt_tokens = self._run_prompt(
            'non_pii_detection',
            context,
            version,
            max_new_tokens,
            json_response_format=True,
        )

        if isinstance(completion_tokens, int):
            sdd_report['completion_tokens'] += completion_tokens
        if isinstance(prompt_tokens, int):
            sdd_report['prompt_tokens'] += prompt_tokens

        sdd_report['non_pii_model'] = self.model.model_name
        sdd_report['non_pii'] = {}
        sdd_report['non_pii']['sensitivity'] = prediction.get('sensitivity', 'UNDETERMINED')
        sdd_report['non_pii']['sensitive_columns'] = prediction.get('sensitive_columns', [])
        sdd_report['non_pii']['cited_isp_rules'] = prediction.get('cited_isp_rules', [])
        sdd_report['non_pii']['explanation'] = prediction.get('explanation', '')

        return sdd_report


if __name__ == '__main__':
    from utils.processing import create_report
    import os

    sdd_report = create_report('research/data/panama.xlsx')

    non_pii_classifier = NonPIIClassifier(
        AzureOpenAIStrategy(
            model_name='gpt-4.1-nano',
            azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT'),
            api_key=os.getenv('AZURE_OPENAI_API_KEY'),
        )
    )
    import json

    with open('/Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/data/isps.json', 'r') as f:
        isp = json.load(f)

    print(isp['default'])

    for sheet in sdd_report:
        # Only use the two first columns
        sheet['columns'] = sheet['columns'][:2]
        sdd_report = non_pii_classifier.classify(sheet, isp)
        print(sdd_report)
