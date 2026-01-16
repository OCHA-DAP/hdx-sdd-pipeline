"""
Main entry point for HDX SSD Pipeline (Refactored).

This is the new main entry point using clean architecture.
It processes HDX resource events from Redis streams.
"""

import logging
import logging.config
import os
from time import sleep
from dotenv import load_dotenv

from hdx_redis_lib import connect_to_hdx_event_bus, RedisConfig
from event_processor import EventProcessor

# Load environment
load_dotenv()

# Setup logging
logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)


def main():
    """
    Main entry point for the HDX SSD Pipeline.
    
    This connects to the Redis event bus and processes events
    using the clean architecture event processor.
    """
    logger.info("="*60)
    logger.info("HDX SSD Pipeline Starting (Clean Architecture)")
    logger.info("="*60)
    
    # Check if worker is enabled
    worker_enabled = os.getenv('WORKER_ENABLED', 'true').lower() == 'true'
    
    if not worker_enabled:
        logger.info("WORKER_ENABLED is false. Sleeping indefinitely...")
        while True:
            sleep(3600)
        return
    
    # Initialize event processor
    logger.info("Initializing event processor...")
    processor = EventProcessor()
    
    # Connect to Redis event bus
    logger.info("Connecting to Redis event bus...")
    
    event_bus = connect_to_hdx_event_bus(
        os.getenv('REDIS_STREAM_STREAM_NAME', 'hdx_event_stream'),
        os.getenv('REDIS_STREAM_GROUP_NAME', 'hdx_sdd_group'),
        os.getenv('REDIS_STREAM_CONSUMER_NAME', 'hdx_sdd_consumer_1'),
        RedisConfig(
            host=os.getenv('REDIS_STREAM_HOST', 'redis'),
            db=int(os.getenv('REDIS_STREAM_DB', '7')),
            port=int(os.getenv('REDIS_STREAM_PORT', '6379')),
        ),
    )
    
    logger.info("Connected to Redis event bus")
    logger.info("Listening for events...")
    
    # Start listening for events
    event_bus.hdx_listen(
        processor.process_event,
        allowed_event_types={'resource-created', 'resource-data-changed'},
        max_iterations=10_000,
    )
    
    logger.info("Event processing completed")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\nShutting down gracefully...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise
