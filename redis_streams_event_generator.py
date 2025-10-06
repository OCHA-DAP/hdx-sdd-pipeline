import os
import json
import logging
from typing import List

from hdx_redis_lib import connect_to_hdx_write_only_event_bus, RedisConfig

# Configure logging to print to console
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
)

event_bus = None
log = logging.getLogger(__name__)


def stream_events_to_redis(event_list: List[dict], stream_name: str = 'hdx_event_stream'):
    global event_bus
    if event_bus is None:
        redis_stream_host = 'localhost'
        redis_stream_port = os.getenv('REDIS_STREAM_PORT', 6379)
        redis_stream_db = os.getenv('REDIS_STREAM_DB', 7)

        event_bus = connect_to_hdx_write_only_event_bus(
            stream_name, RedisConfig(host=redis_stream_host, port=redis_stream_port, db=redis_stream_db)
        )
    for event in event_list:
        # Add the event to the Redis stream
        log.info('Processing event type {}'.format(event['event_type']))
        event_bus.push_hdx_event(event)
        log.info('Finished processing event type {}'.format(event['event_type']))


# read event list from events.json and stream to redis
if __name__ == '__main__':
    with open('events.json', 'r') as f:
        events = json.load(f)
    stream_events_to_redis(events)
