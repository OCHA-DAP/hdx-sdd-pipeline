"""
Main entry point for HDX SSD Pipeline (Refactored).

This is the new main entry point using clean architecture.
It processes HDX resource events from Redis streams.
uv run python main_test.py
"""

import json
from src.event_processor import EventProcessor


def main():
    # Load events.json
    with open('events.json', 'r') as f:
        events = json.load(f)

    # Initialize event processor
    processor = EventProcessor()

    # Process events
    for event in events:
        processor.process_event(event)


if __name__ == '__main__':
    main()
