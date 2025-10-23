"""test/unit/test_ckan_utils.py: Unit tests for utils/ckan.py."""

from unittest.mock import patch
import pytest
from utils.ckan import update_resource_fields


def test_invalid_resource_id_type():
    """Test update_resource_fields raises ValueError when resource_id is not a string."""
    with pytest.raises(ValueError, match='must be a dictionary'):
        update_resource_fields('some-id', 'not-a-dict')


@patch('utils.ckan.requests.post')
def test_update_resource_fields_invalid_resource_id(mock_post):
    """Test handling of invalid resource ID when CKAN returns an error."""
    # Mock the HTTP response from CKAN
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {'success': False, 'error': {'message': 'Resource not found'}}

    result = update_resource_fields('invalid-id', {'sensitive': True})

    # The function should return None if update fails
    assert result is None

    # Verify the API call was made correctly
    mock_post.assert_called_once()
    _, kwargs = mock_post.call_args
    assert kwargs['json']['id'] == 'invalid-id'


@patch('utils.ckan.requests.post')
def test_update_resource_fields_success(mock_post):
    """Test successful update of resource fields when CKAN returns a success response."""
    # Mock the HTTP response from CKAN
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {'success': True, 'result': {'name': 'test-resource'}}

    result = update_resource_fields('test-id', {'sensitive': True})
    assert result is not None
    assert result['name'] == 'test-resource'

    # Verify the API call was made correctly
    mock_post.assert_called_once()
    _, kwargs = mock_post.call_args
    assert kwargs['json']['id'] == 'test-id'
    assert kwargs['json']['sensitive'] is True
