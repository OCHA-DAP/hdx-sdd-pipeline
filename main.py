"""
Main entry point for HDX SSD Pipeline (Refactored).

This is the new main entry point using clean architecture.
It processes HDX resource events from Redis streams.
"""

import logging
from time import sleep
from dotenv import load_dotenv

from hdx_redis_lib import connect_to_hdx_event_bus, RedisConfig
from src.event_processor import EventProcessor
from config import get_config
from src.shared.utils.logging_conf import configure_logging

# Load environment
load_dotenv()

logger = logging.getLogger(__name__)

main_config = get_config()


def main():
    """
    Main entry point for the HDX SSD Pipeline.

    This connects to the Redis event bus and processes events
    using the clean architecture event processor.
    """
    configure_logging()
    logger.info('=' * 60)
    logger.info('HDX SSD Pipeline Starting (Clean Architecture)')
    logger.info('=' * 60)

    # Check if worker is enabled
    worker_enabled = main_config.WORKER_ENABLED

    if not worker_enabled:
        logger.info('WORKER_ENABLED is false. Sleeping indefinitely...')
        while True:
            sleep(3600)
        return

    # Initialize event processor
    logger.info('Initializing event processor...')
    processor = EventProcessor()

    # Connect to Redis event bus
    logger.info('Connecting to Redis event bus...')

    event_bus = connect_to_hdx_event_bus(
        main_config.REDIS_STREAM_STREAM_NAME,
        main_config.REDIS_STREAM_GROUP_NAME,
        main_config.REDIS_STREAM_CONSUMER_NAME,
        RedisConfig(
            host=main_config.REDIS_STREAM_HOST,
            db=main_config.REDIS_STREAM_DB,
            port=main_config.REDIS_STREAM_PORT,
        ),
    )

    logger.info('Connected to Redis event bus')
    logger.info('Listening for events...')

    # Start listening for events
    event_bus.hdx_listen(
        processor.process_event,
        allowed_event_types={'resource-created', 'resource-data-changed'},
        max_iterations=10_000,
    )

    logger.info('Event processing completed')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info('\nShutting down gracefully...')
    except Exception as e:
        logger.error(f'Fatal error: {e}', exc_info=True)
        raise
