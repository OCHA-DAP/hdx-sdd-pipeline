"""
Main entry point for HDX SSD Pipeline (Refactored).

This is the new main entry point using clean architecture.
It processes HDX resource events from Redis streams.
"""

import logging
import src.shared.utils.logging_conf  # noqa # this needs to be at the top to configure logging
from time import sleep

from hdx_redis_lib import connect_to_hdx_event_bus, RedisConfig
from src.event_processor import EventProcessor
from config import get_config

logger = logging.getLogger(__name__)

main_config = get_config()

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

# redis-py 8.0 introduced a default socket_timeout of 5s which causes xreadgroup(block=120s)
# to timeout prematurely. Set it above the block duration so only genuine hangs trigger it.
event_bus.redis_conn.connection_pool.connection_kwargs['socket_timeout'] = 3 * 60


if __name__ == '__main__':
    # Initialize event processor
    logger.info('Initializing event processor...')
    processor = EventProcessor()

    if not main_config.WORKER_ENABLED:
        logger.info('WORKER_ENABLED is false. Sleeping indefinitely...')

        while True:
            sleep(3600)
    else:
        event_bus.hdx_listen(
            processor.process_event,
            allowed_event_types={'sdd-resource-created', 'sdd-resource-data-changed'},
            max_iterations=10_000,
        )
