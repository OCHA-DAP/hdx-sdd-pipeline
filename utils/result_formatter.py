"""utils/result_formatter.py: Formats results for Redis response."""

import logging
from typing import Any, Dict
from datetime import datetime

logger = logging.getLogger(__name__)


def format_results_for_redis(table_data: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format classification results for Redis response.

    Args:
        table_data: Processed table data with classification results
        event: Original event data

    Returns:
        dict: Formatted response for Redis
    """
    try:
        # Extract basic information
        metadata = table_data.get('metadata', {})
        columns = table_data.get('columns', {})

        # Process PII columns
        pii_columns = []
        sensitivity_summary = {}

        for col_name, col_data in columns.items():
            pii_entity = col_data.get('pii_entity', {})
            pii_sensitivity = col_data.get('pii_sensitivity', {})

            entity_type = pii_entity.get('entity_type', 'None')
            sensitivity_level = pii_sensitivity.get('sensitivity_level', 'NON_SENSITIVE')

            if entity_type != 'None':
                pii_columns.append(
                    {
                        'column_name': col_name,
                        'entity_type': entity_type,
                        'sensitivity_level': sensitivity_level,
                        'confidence': pii_entity.get('confidence', ''),
                        'success': pii_entity.get('success', False),
                    }
                )

            # Count sensitivity levels
            sensitivity_summary[sensitivity_level] = sensitivity_summary.get(sensitivity_level, 0) + 1

        # Get non-PII sensitivity
        non_pii_sensitivity = metadata.get('non_pii_sensitivity', {})
        overall_sensitivity = non_pii_sensitivity.get('sensitivity_level', 'UNKNOWN')

        # Create response structure
        response = {
            # Original event metadata
            'event_metadata': {
                'dataset_id': event.get('dataset_id', ''),
                'resource_id': event.get('resource_id', ''),
                'resource_name': event.get('resource_name', ''),
                'event_type': event.get('event_type', ''),
                'timestamp': event.get('timestamp', ''),
            },
            # Processing metadata
            'processing_metadata': {
                'file_name': metadata.get('file_name', ''),
                'file_path': metadata.get('file_path', ''),
                'num_rows': metadata.get('num_rows', 0),
                'num_columns': metadata.get('num_columns', 0),
                'processing_timestamp': metadata.get('processing_timestamp', datetime.now().isoformat()),
                'processing_success': metadata.get('processing_success', True),
                'processing_error': metadata.get('processing_error', ''),
            },
            # Classification results summary
            'classification_summary': {
                'total_columns': len(columns),
                'pii_columns_count': len(pii_columns),
                'sensitivity_distribution': sensitivity_summary,
                'overall_non_pii_sensitivity': overall_sensitivity,
            },
            # Detailed PII results
            'pii_detection_results': pii_columns,
            # Non-PII classification result
            'non_pii_classification': {
                'sensitivity_level': overall_sensitivity,
                'confidence': non_pii_sensitivity.get('confidence', ''),
                'success': non_pii_sensitivity.get('success', False),
            },
            # Overall status
            'status': 'success' if metadata.get('processing_success', True) else 'error',
            'message': (
                'Processing completed successfully'
                if metadata.get('processing_success', True)
                else f"Processing failed: {metadata.get('processing_error', 'Unknown error')}"
            ),
        }

        logger.info(
            'Formatted results for Redis: %s PII columns detected, overall sensitivity: %s',
            len(pii_columns),
            overall_sensitivity
        )

        return response

    except Exception as e:
        logger.error('Error formatting results for Redis: %s', e)
        # Return error response
        return {
            'event_metadata': event,
            'processing_metadata': {
                'processing_success': False,
                'processing_error': str(e),
                'processing_timestamp': datetime.now().isoformat(),
            },
            'classification_summary': {
                'total_columns': 0,
                'pii_columns_count': 0,
                'sensitivity_distribution': {},
                'overall_non_pii_sensitivity': 'ERROR',
            },
            'pii_detection_results': [],
            'non_pii_classification': {'sensitivity_level': 'ERROR', 'confidence': str(e), 'success': False},
            'status': 'error',
            'message': f'Result formatting failed: {e}',
        }


def format_error_response(event: Dict[str, Any], error_message: str) -> Dict[str, Any]:
    """
    Format an error response for Redis.

    Args:
        event: Original event data
        error_message: Error message to include

    Returns:
        dict: Formatted error response
    """
    return {
        'event_metadata': event,
        'processing_metadata': {
            'processing_success': False,
            'processing_error': error_message,
            'processing_timestamp': datetime.now().isoformat(),
        },
        'classification_summary': {
            'total_columns': 0,
            'pii_columns_count': 0,
            'sensitivity_distribution': {},
            'overall_non_pii_sensitivity': 'ERROR',
        },
        'pii_detection_results': [],
        'non_pii_classification': {'sensitivity_level': 'ERROR', 'confidence': error_message, 'success': False},
        'status': 'error',
        'message': f'Processing failed: {error_message}',
    }


def extract_key_metrics(response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract key metrics from a formatted response.

    Args:
        response: Formatted response data

    Returns:
        dict: Key metrics summary
    """
    try:
        summary = response.get('classification_summary', {})
        pii_results = response.get('pii_detection_results', [])

        # Count high/very high sensitivity columns
        high_sensitivity_count = 0
        severe_sensitivity_count = 0

        for pii_result in pii_results:
            sensitivity = pii_result.get('sensitivity_level', 'NON_SENSITIVE')
            if sensitivity in ['HIGH_SENSITIVE', 'SEVERE_SENSITIVE']:
                high_sensitivity_count += 1
            if sensitivity == 'SEVERE_SENSITIVE':
                severe_sensitivity_count += 1

        metrics = {
            'total_columns': summary.get('total_columns', 0),
            'pii_columns': summary.get('pii_columns_count', 0),
            'high_sensitivity_columns': high_sensitivity_count,
            'severe_sensitivity_columns': severe_sensitivity_count,
            'overall_sensitivity': summary.get('overall_non_pii_sensitivity', 'UNKNOWN'),
            'processing_success': response.get('status') == 'success',
        }

        return metrics

    except Exception as e:
        logger.error('Error extracting key metrics: %s', e)
        return {
            'total_columns': 0,
            'pii_columns': 0,
            'high_sensitivity_columns': 0,
            'severe_sensitivity_columns': 0,
            'overall_sensitivity': 'ERROR',
            'processing_success': False,
        }
