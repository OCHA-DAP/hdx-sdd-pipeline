import os
import time
import logging
import pytest
from hdx_redis_lib import connect_to_hdx_event_bus, RedisConfig
from redis_streams_event_generator import stream_events_to_redis

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


@pytest.fixture
def redis_config():
    """Fixture to provide Redis configuration."""
    return RedisConfig(host='localhost', port=os.getenv('REDIS_STREAM_PORT', 6379), db=os.getenv('REDIS_STREAM_DB', 7))


@pytest.fixture
def stream_config():
    """Fixture to provide stream configuration."""
    return {'stream_name': 'hdx_test_event_stream', 'group_name': 'test_group', 'consumer_name': 'test_consumer'}


@pytest.fixture
def event_bus_listener(redis_config, stream_config):
    """Fixture to provide an event bus listener."""
    bus = connect_to_hdx_event_bus(
        stream_config['stream_name'], stream_config['group_name'], stream_config['consumer_name'], redis_config
    )
    yield bus
    # Cleanup
    try:
        bus.redis_conn.delete(stream_config['stream_name'])
    except Exception as e:
        logger.warning(f'Failed to cleanup stream: {e}')


def test_push_and_listen_events(event_bus_listener):
    """Test pushing events to Redis stream and listening for them."""
    # Test event data
    test_event = {
        'event_type': 'resource-data-changed',
        'event_time': '2025-10-06T10:00:00.000000',
        'event_source': 'ckan',
        'dataset_name': 'test-dataset',
        'dataset_title': 'Test Dataset',
        'dataset_id': 'test-dataset-id-123',
        'resource_name': 'test_resource.csv',
        'resource_id': 'test-resource-id-456',
    }

    # Push event to Redis stream using the stream_events_to_redis function
    logger.info('Pushing test event to Redis stream')
    stream_events_to_redis([test_event], stream_name='hdx_test_event_stream')
    logger.info('Test event pushed successfully')

    # Give Redis a moment to process
    time.sleep(0.1)

    # Track if event was received
    received_events = []

    def event_processor(event):
        """Process received events."""
        logger.info(f'Received event: {event}')
        received_events.append(event)
        return True, 'Success'

    # Listen for the event (max 1 iteration since we only sent 1 event)
    logger.info('Starting to listen for events')
    event_bus_listener.hdx_listen(event_processor, allowed_event_types={'resource-data-changed'}, max_iterations=1)

    # Verify the event was received
    assert len(received_events) == 1, f'Expected 1 event, but received {len(received_events)}'

    received_event = received_events[0]
    assert received_event['event_type'] == test_event['event_type']
    assert received_event['dataset_name'] == test_event['dataset_name']
    assert received_event['resource_id'] == test_event['resource_id']

    logger.info('Test completed successfully')

    # delete the test stream
    try:
        event_bus_listener.redis_client.delete('hdx_test_event_stream')
        logger.info('Test stream deleted successfully')
    except Exception as e:
        logger.warning(f'Failed to delete test stream: {e}')
