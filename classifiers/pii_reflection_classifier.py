import logging
from typing import Any, Dict, Tuple
from tqdm import tqdm

from .base_classifier import BaseClassifier
from llm_model.azure_strategy import AzureOpenAIStrategy
from utils.exception_handler import handle_exception_wrap
from utils.utils import table_markdown

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
        if entity_type == 'None' or entity_type == 'TODO' or entity_type == 'UNDETERMINED':
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
        sdd_report: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Args:
            sdd_report: the dictionary to classify
        """

        sdd_report['pii_reflection_model'] = self.model.model_name

        for col in tqdm(sdd_report['columns'], desc='Reflecting on PII sensitivity'):
            entity = col['pii']['entity_type']

            pred, comp, prompt = self.classify_column(
                column_name=col['column_name'],
                table_markdown=table_markdown(sdd_report),
                entity_type=entity,
            )

            # token aggregation
            sdd_report['completion_tokens'] += comp
            sdd_report['prompt_tokens'] += prompt

            sensitive = False
            if pred == 'SENSITIVE':
                sensitive = True
                sdd_report['personal_data_sensitive'] = True

            col['pii']['sensitive'] = sensitive

        return sdd_report


if __name__ == '__main__':
    from utils.processing import create_report
    import os

    sdd_report = create_report('research/data/panama.xlsx')
    pii_reflection_classifier = PIIReflectionClassifier(
        AzureOpenAIStrategy(
            model_name='gpt-4.1-nano',
            azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT'),
            api_key=os.getenv('AZURE_OPENAI_API_KEY'),
        )
    )

    for sheet in sdd_report:
        # Only use the two first columns
        sheet['columns'] = sheet['columns'][:2]
        sdd_report = pii_reflection_classifier.classify_df(sheet)
        print(sdd_report)
