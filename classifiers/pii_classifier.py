import logging
from typing import Any, List, Tuple
import pandas as pd
from tqdm import tqdm

from .base_classifier import BaseClassifier
from models.sdd_report import PIIColumnReport
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
                PIIColumnReport(
                    column_name=column_name,
                    sample_values=sample_values,
                    pii={'entity_type': 'None'},
                ),
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
            PIIColumnReport(
                column_name=column_name,
                sample_values=sample_values,
                pii={'entity_type': entity},
            ),
            completion_tokens,
            prompt_tokens,
        )

    def classify_df(
        self,
        df: pd.DataFrame,
    ) -> Tuple[List[PIIColumnReport], int, int, str]:
        """
        Returns:
          - list of PIIColumnReport objects
          - total completion tokens
          - total prompt tokens
          - model name
        """

        pii_columns: List[PIIColumnReport] = []
        total_completion = 0
        total_prompt = 0

        for column in tqdm(df.columns, desc='Classifying PII'):
            values = df[column].dropna().astype(str).tolist()

            col_report, comp, prompt = self._classify_column(column, values)
            pii_columns.append(col_report)
            total_completion += comp
            total_prompt += prompt

        return pii_columns, total_completion, total_prompt, self.model.model_name
