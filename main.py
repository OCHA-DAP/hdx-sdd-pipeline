"""main.py: Main entry point for the HDX SSD Pipeline."""

import logging
import logging.config
import os
import dotenv
from hdx_redis_lib import connect_to_hdx_event_bus, RedisConfig

from preprocessing.preprocessor import TablePreprocessor
from pipeline.orchestrator import SSDOrchestrator
from utils.hdx_downloader import download_resource
from utils.main_config import INPUT_STREAM
from utils.result_formatter import format_results_for_redis

logging.config.fileConfig('logging.conf')
dotenv.load_dotenv()

logger = logging.getLogger(__name__)

# Redis configuration
STREAM_NAME = INPUT_STREAM
GROUP_NAME = 'default_group'
CONSUMER_NAME = 'consumer-1'
REDIS_STREAM_HOST = os.getenv('REDIS_STREAM_HOST', 'localhost')
REDIS_STREAM_PORT = int(os.getenv('REDIS_STREAM_PORT', '6379'))
REDIS_STREAM_DB = int(os.getenv('REDIS_STREAM_DB', '0'))  # Changed to 0 as per plan

event_bus = connect_to_hdx_event_bus(
    STREAM_NAME,
    GROUP_NAME,
    CONSUMER_NAME,
    RedisConfig(host=REDIS_STREAM_HOST, db=REDIS_STREAM_DB, port=REDIS_STREAM_PORT),
)

# Initialize components
preprocessor = TablePreprocessor()
orchestrator = SSDOrchestrator()


def event_processor(event):
    """
    Process an HDX event through the complete SSD pipeline.

    Args:
        event: HDX event containing resource information

    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        logger.info('Processing event: %s', event)

        # Extract event data
        resource_id = event.get('resource_id')

        if not resource_id:
            error_msg = 'No resource_id found in event'
            logger.error(error_msg)
            return False, error_msg

        # Step 1: Download CSV from HDX
        logger.info('Downloading resource %s from HDX', resource_id)
        try:
            file_path = download_resource(resource_id)
            logger.info('Successfully downloaded file: %s', file_path)
        except Exception as e:
            error_msg = 'Failed to download resource %s: %s', resource_id, e
            logger.error(error_msg)
            return False, error_msg

        # Step 2: Preprocess file
        logger.info('Preprocessing file: %s', file_path)
        try:
            table_data = preprocessor.process_file(file_path)
            logger.info('File preprocessing completed successfully')
        except Exception as e:
            error_msg = 'Failed to preprocess file %s: %s', file_path, e
            logger.error(error_msg)
            return False, error_msg

        # Step 3: Run through orchestrator
        logger.info('Running classification pipeline')
        try:
            results = orchestrator.process_table(table_data)
            logger.info('Classification pipeline completed successfully')
        except Exception as e:
            error_msg = 'Classification pipeline failed: %s', e
            logger.error(error_msg)
            return False, error_msg

        # Step 4: Format results for Redis
        logger.info('Formatting results for Redis')
        try:
            formatted_results = format_results_for_redis(results, event)
            logger.info('Results formatted successfully')
        except Exception as e:
            error_msg = 'Failed to format results: %s', e
            logger.error(error_msg)
            return False, error_msg

        # Step 5: Send results back to Redis (placeholder for now)
        # TODO: Implement Redis response mechanism
        logger.info('Results ready for Redis response')

        # Log summary
        summary = formatted_results.get('classification_summary', {})
        pii_count = summary.get('pii_columns_count', 0)
        overall_sensitivity = summary.get('overall_non_pii_sensitivity', 'UNKNOWN')

        success_msg = (
            f'Processing completed: {pii_count} PII columns detected,' f'overall sensitivity: {overall_sensitivity}'
        )
        logger.info(success_msg)

        return True, success_msg

    except Exception as e:
        error_msg = 'Unexpected error in event processing: %s', e
        logger.error(error_msg, exc_info=True)
        return False, error_msg


if __name__ == '__main__':
    event_bus.hdx_listen(event_processor, allowed_event_types={'resource-data-changed'}, max_iterations=10_000)
