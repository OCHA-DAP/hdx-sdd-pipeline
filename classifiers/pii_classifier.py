import logging
from typing import Any, List, Tuple
import pandas as pd
from tqdm import tqdm

from .base_classifier import BaseClassifier
from models.sdd_report import PIIColumnReport, SDDReport
from utils.main_config import PII_ENTITIES_LIST
from llm_model.azure_strategy import AzureOpenAIStrategy

logger = logging.getLogger(__name__)

DEBUG = True


class PIIClassifier(BaseClassifier):
    """
    Returns only PIIColumnReport objects and model tokens.
    """

    def __init__(self, model: AzureOpenAIStrategy):
        super().__init__(model)
        self.model = model

    @staticmethod
    def _normalize_prediction(pred: str) -> str:
        if not isinstance(pred, str):
            return 'UNDETERMINED'

        pred = pred.lower()

        if 'none' in pred:
            return 'None'

        # AGE lowest priority
        entity_order = [e for e in PII_ENTITIES_LIST if e != 'AGE'] + ['AGE']
        for e in entity_order:
            if e.lower() in pred:
                return e

        return 'UNDETERMINED'

    def _classify_column(
        self,
        column_name: str,
        sample_values: List[Any],
        k: int = 5,
        version: str = 'v0',
    ) -> Tuple[PIIColumnReport, int, int]:
        sample_values = [str(v) for v in sample_values[:k]]

        if not sample_values or any(v == '' for v in sample_values) or sample_values == []:
            return (
                {'column_name': column_name, 'sample_values': sample_values, 'pii': {'entity_type': 'None'}},
                0,
                0,
            )

        context = {'column_name': column_name, 'sample_values': sample_values}

        prediction, completion_tokens, prompt_tokens = self._run_prompt(
            'pii_detection',
            context,
            version,
            max_new_tokens=8,
        )

        entity = self._normalize_prediction(prediction)

        return (
            {'column_name': column_name, 'sample_values': sample_values, 'pii': {'entity_type': entity}},
            completion_tokens,
            prompt_tokens,
        )

    def classify_df(
        self,
        sdd_report: SDDReport,
    ) -> SDDReport:
        """
        Returns:
          - list of PIIColumnReport objects
          - total completion tokens
          - total prompt tokens
          - model name
        """
        sdd_report['pii_classifier_model'] = self.model.model_name

        for column in tqdm(sdd_report['columns'], desc='Classifying PII'):
            col_report, comp, prompt = self._classify_column(column['column_name'], column['sample_values'])
            # Replace the column in the sdd_report with the new column
            sdd_report['columns'][sdd_report['columns'].index(column)] = col_report
            sdd_report['completion_tokens'] += comp
            sdd_report['prompt_tokens'] += prompt

        return sdd_report


if __name__ == '__main__':
    from utils.processing import create_report
    import os

    sdd_report = create_report('research/data/panama.xlsx')
    print(sdd_report)
    pii_classifier = PIIClassifier(
        AzureOpenAIStrategy(
            model_name='gpt-4.1-nano',
            azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT'),
            api_key=os.getenv('AZURE_OPENAI_API_KEY'),
        )
    )

    for sheet in sdd_report:
        # Only use the two first columns
        sheet['columns'] = sheet['columns'][:2]
        sdd_report = pii_classifier.classify_df(sheet)
        print(sdd_report)
