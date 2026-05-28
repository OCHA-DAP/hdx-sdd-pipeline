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


def test_isp_retriever_resource_name_fallback(mock_ckan_client):
    """Test ISP retriever falls back to resource name matching."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default'}, 'some_isp': {'country': 'tst', 'rule': 'custom'}}

    # CKAN returns no group info
    mock_ckan_client.return_value.package_show.return_value = {'groups': []}

    with patch('builtins.open', mock_open(read_data=json.dumps(mock_isps))):
        # Resource stem should be ISO3 and matched case-insensitively
        rules = retriever.get_isp_rules('pkg123', 'TST.csv', mock_ckan_client.return_value)

    assert rules == {'country': 'tst', 'rule': 'custom'}


def test_isp_retriever_ckan_disabled_fallback():
    """Test ISP retriever uses resource name when CKAN is disabled."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default'}, 'some_isp': {'country': 'tst', 'rule': 'custom'}}

    with patch('builtins.open', mock_open(read_data=json.dumps(mock_isps))):
        rules = retriever.get_isp_rules(None, 'tst.csv')

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
