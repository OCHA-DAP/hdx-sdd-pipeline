"""
Main entry point for HDX SSD Pipeline (Refactored).

This is the new main entry point using clean architecture.
It processes HDX resource events from Redis streams.
"""

import logging
import json
import sys

from src.event_processor import EventProcessor
from config import get_config


# Configure logging to console only for local development
def configure_console_logging():
    """Configure logging to output only to console."""
    logging.basicConfig(
        level=logging.INFO,
        format='[%(process)d - %(thread)d] %(asctime)s %(levelname)-5.5s [%(name)s:%(lineno)d] %(message)s',
        stream=sys.stdout,
        force=True,  # Override any existing configuration
    )


# Configure console logging
configure_console_logging()

logger = logging.getLogger(__name__)

main_config = get_config()
event_processor = EventProcessor(config=main_config)


def load_events_from_file(file_path: str) -> list:
    """Load events from a JSON file."""
    try:
        with open(file_path, 'r') as f:
            events = json.load(f)
        logger.info(f'Loaded {len(events)} events from {file_path}')
        return events
    except FileNotFoundError:
        logger.error(f'Events file not found: {file_path}')
        return []
    except json.JSONDecodeError as e:
        logger.error(f'Error parsing JSON file {file_path}: {e}')
        return []


if __name__ == '__main__':
    # Use events.json and process
    events_file = 'events.json'

    logger.info('Starting local event processor...')
    logger.info(f'Loading events from {events_file}')

    events = load_events_from_file(events_file)

    if not events:
        logger.warning('No events to process. Exiting.')
        exit(1)

    logger.info(f'Processing {len(events)} events...')

    for i, event in enumerate(events, 1):
        try:
            logger.info(f'Processing event {i}/{len(events)}')
            event_processor.process_event(event)
        except Exception as e:
            logger.error(f'Error processing event {i}: {e}')
            continue

    logger.info('Finished processing all events.')
