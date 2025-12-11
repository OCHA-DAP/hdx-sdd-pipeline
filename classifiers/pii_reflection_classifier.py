import logging
from typing import Any, Dict, List, Tuple
from tqdm import tqdm

from .base_classifier import BaseClassifier
from llm_model.azure_strategy import AzureOpenAIStrategy
from utils.exception_handler import handle_exception_wrap
from models.sdd_report import PIIColumnReport

logger = logging.getLogger(__name__)


class PIIReflectionClassifier(BaseClassifier):
    """
    Classifies whether detected PII columns are sensitive or not.

    Returns only:
      - list of reflection results
      - completion tokens
      - prompt tokens
      - model name
    """

    def __init__(self, model: AzureOpenAIStrategy):
        super().__init__(model)
        self.model = model

    @handle_exception_wrap()
    def classify_column(
        self,
        column_name: str,
        table_markdown: str,
        entity_type: str,
        max_new_tokens: int = 12,
        version: str = 'v0',
    ) -> Tuple[str, int, int]:
        if entity_type == 'None':
            return 'NON_SENSITIVE', 0, 0

        ctx = {
            'column_name': column_name,
            'table_markdown': table_markdown,
            'column_entity': entity_type,
        }

        prediction, completion_tokens, prompt_tokens = self._run_prompt(
            'pii_reflection',
            ctx,
            version=version,
            max_new_tokens=max_new_tokens,
            json_response_format=False,
        )

        return prediction, completion_tokens, prompt_tokens

    def classify_df(
        self,
        table_markdown: str,
        pii_columns: List,
    ) -> Tuple[List[Dict[str, Any]], int, int, str]:  # reflections  # completion tokens  # prompt tokens  # model name
        """
        Args:
            table_markdown: the markdown table used for reasoning
            pii_columns: list of PIIColumnReport from PIIClassifier

        Returns:
            (
              [
                {
                  "column_name": ...,
                  "entity_type": ...,
                  "sensitive": bool
                },
                ...
              ],
              total_completion_tokens,
              total_prompt_tokens,
              model_name
            )
        """

        reflections: List[PIIColumnReport] = []
        total_completion = 0
        total_prompt = 0

        for col in tqdm(pii_columns, desc='Reflecting on PII sensitivity'):
            entity = col.pii.get('entity_type')

            if entity == 'None':
                reflections.append(
                    PIIColumnReport(
                        column_name=col.column_name,
                        sample_values=col.sample_values,
                        pii={'entity_type': entity, 'sensitive': False},
                    )
                )
                continue

            pred, comp, prompt = self.classify_column(
                column_name=col.column_name,
                table_markdown=table_markdown,
                entity_type=entity,
            )

            # token aggregation
            total_completion += comp
            total_prompt += prompt

            sensitive = False
            if pred == 'SENSITIVE':
                sensitive = True

            reflections.append(
                PIIColumnReport(
                    column_name=col.column_name,
                    sample_values=col.sample_values,
                    pii={'entity_type': entity, 'sensitive': sensitive},
                )
            )

        return reflections, total_completion, total_prompt, self.model.model_name
