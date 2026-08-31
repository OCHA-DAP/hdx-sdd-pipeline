import pytest
import json
from unittest.mock import patch, mock_open
from src.shared.utils.isp_retrieval import ISPRetriever


@pytest.fixture
def mock_ckan_client():
    """Mock CKAN client fixture."""
    with patch('src.shared.utils.ckan.CKANClient') as mock:
        yield mock


def test_isp_retriever_default():
    """Test ISP retriever returns default rules when no match found."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default_rule'}}

    with patch('builtins.open', mock_open(read_data=json.dumps(mock_isps))):
        rules = retriever.get_isp_rules(None)

    assert rules == {'rule': 'default_rule'}


def test_isp_retriever_country_match(mock_ckan_client):
    """Test ISP retriever matches ISO3 from CKAN package groups."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default'}, 'some_isp': {'country': 'tst', 'rule': 'custom'}}

    mock_ckan_client.return_value.package_show.return_value = {'groups': [{'name': 'TST'}]}

    with patch('builtins.open', mock_open(read_data=json.dumps(mock_isps))):
        rules = retriever.get_isp_rules('pkg123', ckan_client=mock_ckan_client.return_value)

    assert rules == {'country': 'tst', 'rule': 'custom'}


def test_isp_retriever_country_match_group_name_field(mock_ckan_client):
    """Test ISP retriever matches using the group name field from CKAN."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default'}, 'some_isp': {'country': 'tst', 'rule': 'custom'}}

    mock_ckan_client.return_value.package_show.return_value = {'groups': [{'name': 'tst'}]}

    with patch('builtins.open', mock_open(read_data=json.dumps(mock_isps))):
        rules = retriever.get_isp_rules('pkg123', ckan_client=mock_ckan_client.return_value)

    assert rules == {'country': 'tst', 'rule': 'custom'}


def test_isp_retriever_group_id_field_falls_back_to_default(mock_ckan_client):
    """Test ISP retriever ignores the CKAN group id field and falls back to default."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default'}, 'drc_isp': {'country': 'cod', 'rule': 'custom'}}

    mock_ckan_client.return_value.package_show.return_value = {'groups': [{'name': 'country-group', 'id': 'cod'}]}

    with patch('builtins.open', mock_open(read_data=json.dumps(mock_isps))):
        rules = retriever.get_isp_rules('pkg123', ckan_client=mock_ckan_client.return_value)

    assert rules == {'rule': 'default'}


def test_isp_retriever_no_country_match(mock_ckan_client):
    """Test ISP retriever falls back to default when no country match."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default'}, 'some_isp': {'country': 'tst', 'rule': 'custom'}}

    mock_ckan_client.return_value.package_show.return_value = {'groups': [{'name': 'other'}]}

    with patch('builtins.open', mock_open(read_data=json.dumps(mock_isps))):
        rules = retriever.get_isp_rules('pkg123', ckan_client=mock_ckan_client.return_value)

    assert rules == {'rule': 'default'}


def test_isp_retriever_ckan_disabled():
    """Test ISP retriever works when CKAN is disabled."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default_rule'}}

    with patch('builtins.open', mock_open(read_data=json.dumps(mock_isps))):
        rules = retriever.get_isp_rules('pkg123')

    assert rules == {'rule': 'default_rule'}


def test_isp_retriever_file_error():
    """Test ISP retriever handles file loading errors."""
    retriever = ISPRetriever()

    with patch('builtins.open', side_effect=Exception('File not found')):
        rules = retriever.get_isp_rules(None)

    assert rules == {}


def test_isp_retriever_default_exception():
    """Test ISP retriever handles general exceptions."""
    retriever = ISPRetriever()

    with patch('builtins.open', side_effect=Exception('File error')):
        rules = retriever.get_isp_rules(None)

    assert rules == {}


def test_isp_retriever_ckan_disabled_exception():
    """Test ISP retriever handles exceptions when CKAN is disabled."""
    retriever = ISPRetriever()

    with patch('builtins.open', side_effect=Exception('File error')):
        rules = retriever.get_isp_rules(None)

    assert rules == {}


def test_isp_retriever_general_exception(mock_ckan_client):
    """Test ISP retriever handles CKAN exceptions gracefully."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default'}}

    # Mock CKAN to raise exception
    mock_ckan_client.return_value.package_show.side_effect = Exception('CKAN Error')

    with patch('builtins.open', mock_open(read_data=json.dumps(mock_isps))):
        rules = retriever.get_isp_rules('pkg123', ckan_client=mock_ckan_client.return_value)

    assert rules == {'rule': 'default'}


def test_isp_retriever_no_resource_name_fallback_on_hdx(mock_ckan_client):
    """Test ISP retriever does not fall back to resource name matching when package metadata is available."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default'}, 'some_isp': {'country': 'tst', 'rule': 'custom'}}

    # CKAN returns no group info
    mock_ckan_client.return_value.package_show.return_value = {'groups': []}

    with patch('builtins.open', mock_open(read_data=json.dumps(mock_isps))):
        # Even if resource name matches ISO3, it should not fall back to it because package_id is provided
        rules = retriever.get_isp_rules('pkg123', resource_name='TST.csv', ckan_client=mock_ckan_client.return_value)

    assert rules == {'rule': 'default'}


def test_isp_retriever_ckan_disabled_fallback():
    """Test ISP retriever uses resource name when CKAN is disabled."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default'}, 'some_isp': {'country': 'tst', 'rule': 'custom'}}

    with patch('builtins.open', mock_open(read_data=json.dumps(mock_isps))):
        rules = retriever.get_isp_rules('pkg123', 'tst.csv')

    assert rules == {'country': 'tst', 'rule': 'custom'}


def test_isp_retriever_non_iso3_resource_name_falls_back_to_default():
    """Test ISP retriever falls back when resource stem is not a valid ISO3 match."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default'}, 'some_isp': {'country': 'afg', 'rule': 'custom'}}

    with patch('builtins.open', mock_open(read_data=json.dumps(mock_isps))):
        rules = retriever.get_isp_rules(None, 'dataset_xyz_data.csv')

    assert rules == {'rule': 'default'}


def test_isp_retriever_caching():
    """Test ISP retriever caches loaded rules and mappings."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default_rule'}}

    mock_file = mock_open(read_data=json.dumps(mock_isps))
    with patch('builtins.open', mock_file) as mock_file_obj:
        # First call should open file
        rules1 = retriever.get_isp_rules(None)

        # Second call should use cache (no additional file opens)
        rules2 = retriever.get_isp_rules(None)

        # File should only be opened once
        assert mock_file_obj.call_count == 1
        assert rules1 == rules2 == {'rule': 'default_rule'}


def test_match_country_direct():
    """Test direct ISO3 matching in match_country method."""
    retriever = ISPRetriever()
    isps = {'test_isp': {'country': 'tst', 'rule': 'custom'}, 'default': {'rule': 'default'}}

    result = retriever.match_country('TST', isps)
    assert result == {'country': 'tst', 'rule': 'custom'}


def test_match_country_requires_exact_iso3():
    """Test non-exact strings do not match in match_country method."""
    retriever = ISPRetriever()
    isps = {'test_isp': {'country': 'tst', 'rule': 'custom'}, 'default': {'rule': 'default'}}

    result = retriever.match_country('tst_region', isps)
    assert result is None


def test_match_country_no_match():
    """Test no match case in match_country method."""
    retriever = ISPRetriever()
    isps = {'test_isp': {'country': 'tst', 'rule': 'custom'}, 'default': {'rule': 'default'}}

    result = retriever.match_country('unknown', isps)
    assert result is None


def test_match_country_empty_text():
    """Test empty text case in match_country method."""
    retriever = ISPRetriever()
    isps = {'default': {'rule': 'default'}}

    result = retriever.match_country('', isps)
    assert result is None

    result = retriever.match_country(None, isps)
    assert result is None


def test_match_country_case_insensitive():
    """Test ISO3 matching is case-insensitive."""
    retriever = ISPRetriever()
    isps = {'afghanistan': {'country': 'AFG', 'rule': 'custom'}, 'default': {'rule': 'default'}}

    result = retriever.match_country('afg', isps)
    assert result == {'country': 'AFG', 'rule': 'custom'}


def test_google_sheets_isp_strategy():
    """Test GoogleSheetsISPStrategy correctly parses Google Sheet rows."""
    from src.infrastructure.external.isp_strategies import GoogleSheetsISPStrategy
    from unittest.mock import MagicMock

    strategy = GoogleSheetsISPStrategy()

    # Mocking get_gsheets and components
    mock_client = MagicMock()
    mock_spreadsheet = MagicMock()
    mock_worksheet = MagicMock()
    mock_readme_worksheet = MagicMock()

    mock_client.open_by_url.return_value = mock_spreadsheet

    mock_readme_worksheet.get_all_values.return_value = [
        ['Acronyms and abbreviations used in Data / Information types', ''],
        ['AAP', 'Accountability to Affected Populations'],
        ['SEA', 'Sexual Exploitation and Abuse'],
        ['GBV', 'Gender Based Violence'],
        ['HNO', 'Humanitarian Needs Overview'],
    ]

    def get_worksheet(name):
        if name == 'ReadMe':
            return mock_readme_worksheet
        return mock_worksheet

    mock_spreadsheet.worksheet.side_effect = get_worksheet

    mock_worksheet.get_all_values.return_value = [
        ['Country', 'Sensitivity Level', 'Data / Information Type', 'Category', 'Lowest Disaggregation'],
        ['Afghanistan', 'low/no sensitivity', 'HNO data', '3W', 'Admin 1'],
        ['default', 'severe sensitivity', 'SEA/GBV data', 'AAP', 'Community'],
    ]

    with patch('src.infrastructure.external.google_sheets_client.get_gsheets', return_value=mock_client):
        isps = strategy.get_isps()

    assert 'Afghanistan' in isps
    assert isps['Afghanistan']['ISO_CODE'] == 'AFG'
    # Category 3W expanded to "Who does What Where (3W)"
    assert (
        isps['Afghanistan']['low_no_sensitivity']
        == '- HNO data (Category: Who does What Where (3W), Lowest Disaggregation Level: Admin 1)\n'
        '-- Definitions: HNO = Humanitarian Needs Overview\n'
    )
    assert isps['Afghanistan']['sensitivity_rules']['LOW/NON_SENSITIVE']['data and information type'] == [
        '- HNO data (Category: Who does What Where (3W), Lowest Disaggregation Level: Admin 1)\n'
        '-- Definitions: HNO = Humanitarian Needs Overview\n'
    ]

    # Default is parsed from the sheet
    assert 'default' in isps
    assert isps['default']['is_default'] is True
    assert isps['default']['sensitivity_rules']['SEVERE_SENSITIVE']['data and information type'] == [
        '- SEA/GBV data (Category: Accountability to Affected Populations (AAP), '
        'Lowest Disaggregation Level: Community)\n'
        '-- Definitions: AAP = Accountability to Affected Populations, SEA = Sexual Exploitation and Abuse, '
        'GBV = Gender Based Violence\n'
    ]


def test_google_sheets_isp_strategy_filters_disabled_and_inactive():
    """Test GoogleSheetsISPStrategy excludes disabled or inactive rows."""
    from src.infrastructure.external.isp_strategies import GoogleSheetsISPStrategy
    from unittest.mock import MagicMock

    strategy = GoogleSheetsISPStrategy()
    mock_client = MagicMock()
    mock_spreadsheet = MagicMock()
    mock_worksheet = MagicMock()
    mock_readme_worksheet = MagicMock()

    mock_client.open_by_url.return_value = mock_spreadsheet
    mock_readme_worksheet.get_all_values.return_value = []
    mock_spreadsheet.worksheet.side_effect = lambda name: mock_readme_worksheet if name == 'ReadMe' else mock_worksheet

    mock_worksheet.get_all_values.return_value = [
        ['Country', 'Sensitivity Level', 'Data / Information Type', 'Enabled', 'ISP Status'],
        ['Afghanistan', 'low/no sensitivity', 'Active Rule 1', 'Yes', 'Active'],
        ['Afghanistan', 'low/no sensitivity', 'Disabled Rule', 'No', 'Active'],
        ['Sudan', 'high sensitivity', 'Dev Rule', 'Yes', 'Under development'],
        ['Sudan', 'severe sensitivity', 'Unused Rule', 'Yes', 'Not used'],
        ['Sudan', 'medium sensitivity', 'Active Rule 2', 'Yes', 'Approved'],
    ]

    with patch('src.infrastructure.external.google_sheets_client.get_gsheets', return_value=mock_client):
        isps = strategy.get_isps()

    assert 'Afghanistan' in isps
    assert 'Active Rule 1' in isps['Afghanistan']['low_no_sensitivity']
    assert 'Disabled Rule' not in isps['Afghanistan']['low_no_sensitivity']

    assert 'Sudan' in isps
    assert 'Active Rule 2' in isps['Sudan']['medium_sensitivity']
    assert 'Dev Rule' not in isps['Sudan']['high_sensitivity']
    assert 'Unused Rule' not in isps['Sudan']['severe_sensitivity']


def test_isp_retriever_redis_caching():
    """Test ISPRetriever retrieves from and sets to Redis KV store."""
    from unittest.mock import MagicMock

    mock_strategy = MagicMock()
    mock_store = MagicMock()

    mock_isps = {'default': {'rule': 'from_strategy'}}
    mock_strategy.get_isps.return_value = mock_isps

    # 1. Cache hit case
    mock_store.get_object.return_value = {'default': {'rule': 'cached'}}
    retriever = ISPRetriever(strategy=mock_strategy, store=mock_store)

    rules = retriever.get_isp_rules(None)
    assert rules == {'rule': 'cached'}
    mock_strategy.get_isps.assert_not_called()

    # 2. Cache miss case
    mock_store.get_object.return_value = None
    retriever = ISPRetriever(strategy=mock_strategy, store=mock_store)

    rules = retriever.get_isp_rules(None)
    assert rules == {'rule': 'from_strategy'}
    mock_strategy.get_isps.assert_called_once()
    mock_store.set_object.assert_called_once_with('isp_rules_cache', mock_isps, expire_in_seconds=60 * 60 * 12)
