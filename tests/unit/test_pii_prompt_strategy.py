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


def test_pii_prompt_strategy_section_id_sorting():
    from src.infrastructure.external.pii_prompt_strategy import GoogleSheetsPIIReflectionPromptStrategy

    mock_worksheet = MagicMock()
    # Intentionally pass rows out of order (section 10, then section 2, then section 1)
    mock_worksheet.get_all_values.return_value = [
        ['section_id', 'type', 'content'],
        ['10', 'footer', 'PART_3_FOOTER'],
        ['2', 'body', 'PART_2_BODY'],
        ['1', 'header', 'PART_1_HEADER'],
    ]

    mock_spreadsheet = MagicMock()
    mock_spreadsheet.worksheet.return_value = mock_worksheet

    mock_gsheets_client = MagicMock()
    mock_gsheets_client.open_by_url.return_value = mock_spreadsheet

    with patch('src.infrastructure.external.google_sheets_client.get_gsheets', return_value=mock_gsheets_client):
        strategy = GoogleSheetsPIIPromptStrategy(spreadsheet_url='http://example.com', worksheet_name='PII detection')
        template_str = strategy.load_template_string(force_refresh=True)

        assert template_str == 'PART_1_HEADER\n\nPART_2_BODY\n\nPART_3_FOOTER'

        refl_strategy = GoogleSheetsPIIReflectionPromptStrategy(
            spreadsheet_url='http://example.com', worksheet_name='PII reflection'
        )
        refl_template_str = refl_strategy.load_template_string(force_refresh=True)

        assert refl_template_str == 'PART_1_HEADER\n\nPART_2_BODY\n\nPART_3_FOOTER'


def test_spreadsheet_prompt_strategy_local_excel_all_categories():
    from src.infrastructure.external.pii_prompt_strategy import SpreadsheetPromptStrategy

    categories = [
        'personal_data_detection',
        'personal_data_reflection',
        'non_personal_data_classificatio',
        'non_personal_data_default_class',
        'readme',
    ]

    for cat in categories:
        strategy = SpreadsheetPromptStrategy(
            worksheet_name=cat,
            spreadsheet_url='http://invalid-url-to-force-fallback',
            excel_path='src/prompts/prompts_dev.xlsx',
        )
        rules = strategy.load_rules(force_refresh=True)
        assert len(rules) > 0, f'Expected active rules for category {cat}'

        # Verify section_id sorting
        section_ids = [float(r['section_id']) for r in rules if 'section_id' in r and r['section_id'] is not None]
        assert section_ids == sorted(section_ids), f'Section IDs for {cat} are not strictly sorted: {section_ids}'
