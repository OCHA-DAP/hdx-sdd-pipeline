"""Unit tests for GoogleSheetsPIIPromptStrategy and PromptManager integration."""

from unittest.mock import MagicMock, patch
from src.infrastructure.external.pii_prompt_strategy import GoogleSheetsPIIPromptStrategy
from src.shared.utils.prompt_manager import PromptManager


def test_pii_prompt_strategy_rendering():
    mock_worksheet = MagicMock()
    mock_worksheet.get_all_values.return_value = [
        ['section_id', 'type', 'content'],
        ['1_role', 'instruction', 'You are a PII classification system.'],
        ['5_input_name', 'variable_template', 'Column name: {{ column_name }}'],
        [
            '5_input_vals',
            'variable_template',
            '{% if sample_values %}Samples: {{ sample_values }}{% endif %}',
        ],
    ]

    mock_spreadsheet = MagicMock()
    mock_spreadsheet.worksheet.return_value = mock_worksheet

    mock_gsheets_client = MagicMock()
    mock_gsheets_client.open_by_url.return_value = mock_spreadsheet

    with patch('src.infrastructure.external.google_sheets_client.get_gsheets', return_value=mock_gsheets_client):
        strategy = GoogleSheetsPIIPromptStrategy(spreadsheet_url='http://example.com', worksheet_name='PII detection')

        rendered = strategy.render({'column_name': 'Area Code', 'sample_values': ['206', '1']})

        assert rendered is not None
        assert 'You are a PII classification system.' in rendered
        assert 'Column name: Area Code' in rendered
        assert "Samples: ['206', '1']" in rendered or 'Samples: ["206", "1"]' in rendered or "['206', '1']" in rendered


def test_prompt_manager_fallback_on_error():
    pm = PromptManager(prompts_dir='src/prompts')

    with patch('src.infrastructure.external.google_sheets_client.get_gsheets', side_effect=RuntimeError('Auth failed')):
        rendered = pm.get_prompt('pii_detection', context={'column_name': 'test_col', 'sample_values': []})

        assert rendered is not None
        assert 'PII' in rendered or 'column' in rendered.lower()


def test_pii_prompt_strategy_redis_caching():
    mock_store = MagicMock()
    mock_store.get_object.return_value = 'Cached PII Template: {{ column_name }}'

    strategy = GoogleSheetsPIIPromptStrategy(store=mock_store, cache_key='pii_detection_prompt_cache')
    rendered = strategy.render({'column_name': 'RedisCol'})

    assert rendered == 'Cached PII Template: RedisCol'
    mock_store.get_object.assert_called_once_with('pii_detection_prompt_cache')
