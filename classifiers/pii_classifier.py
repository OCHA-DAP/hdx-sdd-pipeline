"""classifiers/pii_classifier.py: Handles detection of PII entities."""

import logging
from typing import Any, List
import pandas as pd

from .base_classifier import BaseClassifier
from models.sdd_report import SDDReport, PIIColumnReport
from utils.main_config import PII_ENTITIES_LIST

logger = logging.getLogger(__name__)

DEBUG = True


class PIIClassifier(BaseClassifier):
    """
    Handles detection of PII entities from column names and sample values.
    """

    def _prepare_context(self, df: pd.DataFrame) -> dict:
        """Prepare context for PII classification."""
        return df.to_dict(orient='records')

    def _classify_column(
        self,
        column_name: str,
        sample_values: List[Any],
        k: int = 5,
        version: str = 'v0',
        report: SDDReport = None,
    ) -> None:
        """
        Detect PII entity type in a column and add a PIIColumnReport to the report.
        Updates report.completion_tokens and report.prompt_tokens.
        """
        if report is None:
            raise ValueError('SDDReport instance must be provided.')

        # Limit to first k non-null values
        sample_values = [str(v) for v in sample_values[:k]]

        # Handle empty or non-alphanumeric columns
        if not self._has_alphanumeric(sample_values):
            report.add_pii_column(
                PIIColumnReport(
                    column_name=column_name,
                    sample_values=sample_values,
                    pii={
                        'entity_type': 'None',
                    },
                )
            )
            return

        context = {'column_name': column_name, 'sample_values': sample_values}

        try:
            # Run your GPT/LLM model via Azure or other strategy
            prediction, completion_tokens, prompt_tokens = self._run_prompt(
                'pii_detection', context, version, max_new_tokens=8
            )

            # Update token counts
            report.completion_tokens += completion_tokens
            report.prompt_tokens += prompt_tokens

        except Exception as e:
            logger.exception('PII classification failed for column %s: %s', column_name, str(e))
            report.add_pii_column(
                PIIColumnReport(
                    column_name=column_name,
                    sample_values=sample_values[:k],
                    pii={
                        'entity_type': 'ERROR',
                    },
                )
            )
            return report

        # Normalize prediction
        prediction_lower = prediction.lower() if isinstance(prediction, str) else ''
        if 'none' in prediction_lower:
            entity_type = 'None'
        else:
            # Prioritize AGE last
            entity_list = [e for e in PII_ENTITIES_LIST if e != 'AGE'] + ['AGE']
            entity_type = 'UNDETERMINED'
            for entity in entity_list:
                if entity.lower() in prediction_lower:
                    entity_type = entity

        # Add PII column to report
        report.add_pii_column(
            PIIColumnReport(
                column_name=column_name,
                sample_values=sample_values,
                pii={
                    'entity_type': entity_type,
                },
            )
        )

    def classify_df(self, df: pd.DataFrame, report: SDDReport) -> SDDReport:
        """Classify each column in a DataFrame and populate the SDD report."""

        for column in df.columns:
            # TODO: Check if the column is already classified
            sample_values = df[column].dropna().astype(str).tolist()
            self._classify_column(column_name=column, sample_values=sample_values, report=report)
            report.add_pii_classifier_model(model_name=self.model_name)
        return report
