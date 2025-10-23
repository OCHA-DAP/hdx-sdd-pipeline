import logging
import logging.config
import os
from hdx_redis_lib import connect_to_hdx_event_bus, RedisConfig

logging.config.fileConfig('logging.conf')

logger = logging.getLogger(__name__)

stream_name = 'hdx_event_stream'
group_name = 'default_group'
consumer_name = 'consumer-1'
redis_stream_host = 'localhost'
redis_stream_port = os.getenv('REDIS_STREAM_PORT', 6379)
redis_stream_db = os.getenv('REDIS_STREAM_DB', 7)

event_bus = connect_to_hdx_event_bus(
    stream_name,
    group_name,
    consumer_name,
    RedisConfig(host=redis_stream_host, db=redis_stream_db, port=redis_stream_port),
)


def event_processor(event):
    # Process the event (this is just a placeholder)
    logger.info(f'Handling event: {event}')
    return True, 'Success'


if __name__ == '__main__':
    event_bus.hdx_listen(event_processor, allowed_event_types={'resource-data-changed'}, max_iterations=10_000)