"""pipeline/orchestrator.py: Orchestrates the three classification stages."""

import logging
from typing import Any, Dict

from classifiers.pii_classifier import PIIClassifier
from classifiers.pii_reflection_classifier import PIIReflectionClassifier
from classifiers.non_pii_classifier import NonPIIClassifier
from utils.main_config import PII_DETECT_MODEL, PII_REFLECT_MODEL, NON_PII_DETECT_MODEL, ISP_DEFAULT

logger = logging.getLogger(__name__)


class SSDOrchestrator:
    """
    Orchestrates the three classification stages:
    1. PII Detection
    2. PII Reflection (sensitivity)
    3. Non-PII Classification
    """

    def __init__(self, pii_model: str = None, pii_reflect_model: str = None, non_pii_model: str = None):
        """
        Initialize the orchestrator with model configurations.

        Args:
            pii_model: Model name for PII detection
            pii_reflect_model: Model name for PII reflection
            non_pii_model: Model name for non-PII classification
        """
        self.pii_model = pii_model or PII_DETECT_MODEL
        self.pii_reflect_model = pii_reflect_model or PII_REFLECT_MODEL
        self.non_pii_model = non_pii_model or NON_PII_DETECT_MODEL

        # Initialize classifiers
        self.pii_classifier = PIIClassifier(model_name=self.pii_model)
        self.pii_reflection_classifier = PIIReflectionClassifier(model_name=self.pii_reflect_model)
        self.non_pii_classifier = NonPIIClassifier(model_name=self.non_pii_model)

        # Load ISP data once at startup
        self.isp_data = ISP_DEFAULT

        logger.info(
            f'Initialized SSDOrchestrator with models: PII={self.pii_model}, '
            f'PII_Reflect={self.pii_reflect_model}, Non_PII={self.non_pii_model}'
        )

    def process_table(self, table_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a table through all three classification stages.

        Args:
            table_data: Preprocessed table data with columns and metadata

        Returns:
            dict: Complete results with all classification outputs
        """
        try:
            logger.info('Starting table processing pipeline')

            # Phase 1: PII Detection
            logger.info('Phase 1: PII Detection')
            table_data = self._detect_pii_entities(table_data)

            # Phase 2: PII Reflection (sensitivity)
            logger.info('Phase 2: PII Reflection')
            table_data = self._reflect_pii_sensitivity(table_data)

            # Phase 3: Non-PII Classification
            logger.info('Phase 3: Non-PII Classification')
            table_data = self._classify_non_pii_sensitivity(table_data)

            logger.info('Table processing pipeline completed successfully')
            return table_data

        except Exception as e:
            logger.error(f'Error in table processing pipeline: {e}')
            # Add error information to the results
            table_data['metadata']['processing_error'] = str(e)
            table_data['metadata']['processing_success'] = False
            raise

    def _detect_pii_entities(self, table_data: Dict[str, Any]) -> Dict[str, Any]:
        """Phase 1: Detect PII entities in each column."""
        columns = table_data.get('columns', {})

        for col_name, col_data in columns.items():
            try:
                logger.info(f'Detecting PII in column: {col_name}')

                sample_values = col_data.get('sample_values', [])
                if not sample_values:
                    # No sample values, mark as no PII
                    col_data['pii_entity'] = {'entity_type': 'None', 'confidence': 'No sample values', 'success': True}
                    continue

                # Run PII classification
                pii_result = self.pii_classifier.classify(column_name=col_name, sample_values=sample_values)

                # Store result
                col_data['pii_entity'] = pii_result

                logger.info(f'PII detection result for {col_name}: {pii_result.get('entity_type', 'Unknown')}')

            except Exception as e:
                logger.error(f'Error detecting PII in column {col_name}: {e}')
                col_data['pii_entity'] = {'entity_type': 'ERROR', 'confidence': str(e), 'success': False}

        return table_data

    def _reflect_pii_sensitivity(self, table_data: Dict[str, Any]) -> Dict[str, Any]:
        """Phase 2: Reflect on PII sensitivity for columns with detected PII."""
        columns = table_data.get('columns', {})
        table_context = table_data.get('table_context', '')

        for col_name, col_data in columns.items():
            try:
                pii_entity = col_data.get('pii_entity', {})
                entity_type = pii_entity.get('entity_type', 'None')

                # Skip if no PII detected or error occurred
                if entity_type in ['None', 'ERROR']:
                    col_data['pii_sensitivity'] = {
                        'sensitivity_level': 'NON_SENSITIVE',
                        'confidence': 'No PII detected',
                        'success': True,
                    }
                    continue

                logger.info(f'Reflecting PII sensitivity for column: {col_name} (entity: {entity_type})')

                # Run PII reflection classification
                reflection_result = self.pii_reflection_classifier.classify(
                    column_name=col_name, context=table_context, pii_entity=entity_type
                )

                # Store result
                col_data['pii_sensitivity'] = reflection_result

                logger.info(
                    f'PII sensitivity result for {col_name}: {reflection_result.get('sensitivity_level', 'Unknown')}'
                )

            except Exception as e:
                logger.error(f'Error reflecting PII sensitivity in column {col_name}: {e}')
                col_data['pii_sensitivity'] = {'sensitivity_level': 'ERROR', 'confidence': str(e), 'success': False}

        return table_data

    def _classify_non_pii_sensitivity(self, table_data: Dict[str, Any]) -> Dict[str, Any]:
        """Phase 3: Classify overall table sensitivity for non-PII aspects."""
        try:
            table_context = table_data.get('table_context', '')

            logger.info('Classifying non-PII table sensitivity')

            # Run non-PII classification
            non_pii_result = self.non_pii_classifier.classify(table_context=table_context, isp=self.isp_data)

            # Store result in metadata
            table_data['metadata']['non_pii_sensitivity'] = non_pii_result

            logger.info(f'Non-PII classification result: {non_pii_result.get('sensitivity_level', 'Unknown')}')

        except Exception as e:
            logger.error(f'Error in non-PII classification: {e}')
            table_data['metadata']['non_pii_sensitivity'] = {
                'sensitivity_level': 'ERROR',
                'confidence': str(e),
                'success': False,
            }

        return table_data

    def get_processing_summary(self, table_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a summary of processing results.

        Args:
            table_data: Processed table data

        Returns:
            dict: Summary of classification results
        """
        columns = table_data.get('columns', {})
        metadata = table_data.get('metadata', {})

        # Count PII entities
        pii_columns = []
        sensitivity_levels = {}

        for col_name, col_data in columns.items():
            pii_entity = col_data.get('pii_entity', {})
            pii_sensitivity = col_data.get('pii_sensitivity', {})

            entity_type = pii_entity.get('entity_type', 'None')
            sensitivity_level = pii_sensitivity.get('sensitivity_level', 'NON_SENSITIVE')

            if entity_type != 'None':
                pii_columns.append(
                    {'column': col_name, 'entity_type': entity_type, 'sensitivity_level': sensitivity_level}
                )

            # Count sensitivity levels
            sensitivity_levels[sensitivity_level] = sensitivity_levels.get(sensitivity_level, 0) + 1

        # Get non-PII sensitivity
        non_pii_sensitivity = metadata.get('non_pii_sensitivity', {})
        overall_sensitivity = non_pii_sensitivity.get('sensitivity_level', 'UNKNOWN')

        summary = {
            'total_columns': len(columns),
            'pii_columns_count': len(pii_columns),
            'pii_columns': pii_columns,
            'sensitivity_distribution': sensitivity_levels,
            'overall_non_pii_sensitivity': overall_sensitivity,
            'processing_success': metadata.get('processing_success', True),
        }

        return summary
