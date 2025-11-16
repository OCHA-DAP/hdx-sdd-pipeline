from main import event_processor

event_processor(
    {
        'resource_id': '1234567890',
        'event_type': 'resource-data-changed',
        'event_time': '2021-01-01T00:00:00Z',
        'event_data': {
            'resource_id': '1234567890',
            'resource_name': 'test_resource',
            'resource_url': 'https://example.com/test_resource',
        },
    }
)
