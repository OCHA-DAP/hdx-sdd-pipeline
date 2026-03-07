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
    """Test ISP retriever matches country from CKAN package data."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default'}, 'some_isp': {'country': 'testland', 'rule': 'custom'}}

    mock_ckan_client.return_value.package_show.return_value = {
        'solr_additions': json.dumps({'countries': ['testland']})
    }

    with patch('builtins.open', mock_open(read_data=json.dumps(mock_isps))):
        rules = retriever.get_isp_rules('pkg123', ckan_client=mock_ckan_client.return_value)

    assert rules == {'country': 'testland', 'rule': 'custom'}


def test_isp_retriever_country_match_string(mock_ckan_client):
    """Test ISP retriever handles string country from CKAN."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default'}, 'some_isp': {'country': 'testland', 'rule': 'custom'}}

    mock_ckan_client.return_value.package_show.return_value = {'solr_additions': json.dumps({'countries': 'testland'})}

    with patch('builtins.open', mock_open(read_data=json.dumps(mock_isps))):
        rules = retriever.get_isp_rules('pkg123', ckan_client=mock_ckan_client.return_value)

    assert rules == {'country': 'testland', 'rule': 'custom'}


def test_isp_retriever_no_country_match(mock_ckan_client):
    """Test ISP retriever falls back to default when no country match."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default'}, 'some_isp': {'country': 'testland', 'rule': 'custom'}}

    mock_ckan_client.return_value.package_show.return_value = {
        'solr_additions': json.dumps({'countries': ['otherland']})
    }

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
    mock_isps = {'default': {'rule': 'default'}, 'some_isp': {'country': 'testland', 'rule': 'custom'}}

    # CKAN returns no country info
    mock_ckan_client.return_value.package_show.return_value = {'solr_additions': json.dumps({'countries': []})}

    with patch('builtins.open', mock_open(read_data=json.dumps(mock_isps))):
        # Pass resource name that contains 'testland'
        rules = retriever.get_isp_rules('pkg123', 'dataset_testland_2023.csv', mock_ckan_client.return_value)

    assert rules == {'country': 'testland', 'rule': 'custom'}


def test_isp_retriever_ckan_disabled_fallback():
    """Test ISP retriever uses resource name when CKAN is disabled."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default'}, 'some_isp': {'country': 'testland', 'rule': 'custom'}}

    with patch('builtins.open', mock_open(read_data=json.dumps(mock_isps))):
        rules = retriever.get_isp_rules(None, 'dataset_testland.csv')

    assert rules == {'country': 'testland', 'rule': 'custom'}


def test_isp_retriever_partial_country_matching():
    """Test ISP retriever matches partial country names."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default'}, 'some_isp': {'country': 'afghanistan', 'rule': 'custom'}}

    with patch('builtins.open', mock_open(read_data=json.dumps(mock_isps))):
        # Test with partial match 'afg' (first 3 chars)
        rules = retriever.get_isp_rules(None, 'dataset_afg_data.csv')

    assert rules == {'country': 'afghanistan', 'rule': 'custom'}


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


def test_isp_retriever_clear_cache():
    """Test ISP retriever cache clearing functionality."""
    retriever = ISPRetriever()
    mock_isps = {'default': {'rule': 'default_rule'}}

    mock_file = mock_open(read_data=json.dumps(mock_isps))
    with patch('builtins.open', mock_file) as mock_file_obj:
        # First call
        retriever.get_isp_rules(None)

        # Clear cache
        retriever.clear_cache()

        # Second call should open file again
        retriever.get_isp_rules(None)

        # File should be opened twice
        assert mock_file_obj.call_count == 2


def test_match_country_direct():
    """Test direct country matching in match_country method."""
    retriever = ISPRetriever()
    isps = {'test_isp': {'country': 'testland', 'rule': 'custom'}, 'default': {'rule': 'default'}}
    country_mapping = {}

    result = retriever.match_country('This is data from testland', isps, country_mapping)
    assert result == {'country': 'testland', 'rule': 'custom'}


def test_match_country_partial():
    """Test partial country matching in match_country method."""
    retriever = ISPRetriever()
    isps = {'test_isp': {'country': 'testland', 'rule': 'custom'}, 'default': {'rule': 'default'}}
    country_mapping = {'tes': 'testland'}

    result = retriever.match_country('This is data from tes_region', isps, country_mapping)
    assert result == {'country': 'testland', 'rule': 'custom'}


def test_match_country_no_match():
    """Test no match case in match_country method."""
    retriever = ISPRetriever()
    isps = {'test_isp': {'country': 'testland', 'rule': 'custom'}, 'default': {'rule': 'default'}}
    country_mapping = {}

    result = retriever.match_country('This is data from unknown', isps, country_mapping)
    assert result is None


def test_match_country_empty_text():
    """Test empty text case in match_country method."""
    retriever = ISPRetriever()
    isps = {'default': {'rule': 'default'}}
    country_mapping = {}

    result = retriever.match_country('', isps, country_mapping)
    assert result is None

    result = retriever.match_country(None, isps, country_mapping)
    assert result is None


# Test whether isp afghanistan is found when afghanistan only in source title
def test_match_country_afghanistan_only_in_title():
    """Test that ISP Afghanistan is found when 'Afghanistan' only appears in source title."""
    retriever = ISPRetriever()
    isps = {'afghanistan': {'country': 'Afghanistan', 'rule': 'custom'}, 'default': {'rule': 'default'}}
    country_mapping = {}

    # Simulate text that contains 'Afghanistan' only in the source title
    text_with_afghanistan = "This is a data source about Afghanistan"

    result = retriever.match_country(text_with_afghanistan, isps, country_mapping)
    assert result == {'country': 'Afghanistan', 'rule': 'custom'}
