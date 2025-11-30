import pytest
from unittest.mock import patch
import requests
from pathlib import Path

# Fixtures imported from conftest.py: mock_ckan_client, create_mock_response
# CKANClient imported via conftest.py

# --- Test Data ---
PACKAGE_ID = 'test-dataset'
RESOURCE_ID = 'test-resource'
BASE_URL = 'http://mock-ckan.org'

# Standard payloads for CKAN success/failure
PACKAGE_SHOW_SUCCESS = {
    'success': True,
    'result': {'id': PACKAGE_ID, 'title': 'Test Dataset Title', 'resources': [{'id': 'r1'}]},
}
RESOURCE_SHOW_SUCCESS = {
    'success': True,
    'result': {'id': RESOURCE_ID, 'name': 'Resource 1', 'download_url': f'{BASE_URL}/data/{RESOURCE_ID}.csv'},
}
CKAN_ERROR_RESPONSE = {'success': False, 'error': {'message': 'Not authorized', '__type': 'Authorization Error'}}
RESOURCE_PATCH_SUCCESS = {
    'success': True,
    'result': {'id': RESOURCE_ID, 'name': 'Resource 1', 'custom_field': 'new_value'},
}

# --- Core Request Tests (_request helper) ---


# Test successful GET request
@patch('requests.get')
def test_request_success_get(mock_get, mock_ckan_client, create_mock_response):
    mock_resp = create_mock_response(200, json_data=PACKAGE_SHOW_SUCCESS)
    mock_get.return_value = mock_resp

    result = mock_ckan_client.package_show(PACKAGE_ID)
    assert result


# Test non-200 status code that does NOT raise an HTTPError (hits L42)
@patch('requests.get')
def test_request_non_200_print(mock_get, mock_ckan_client, create_mock_response):
    """Test hitting the 'CKAN request failed: %s' print for non-200, non-raising status (L42)."""
    # Mock a non-200 status code (e.g., 301) but configure raise_for_status not to raise.
    # We must ensure success=True in the JSON payload so it doesn't return None later.
    mock_resp = create_mock_response(301, json_data=PACKAGE_SHOW_SUCCESS, raise_for_status_exc=None)
    mock_get.return_value = mock_resp

    result = mock_ckan_client._request('package_show')
    assert result == PACKAGE_SHOW_SUCCESS['result']


# Test CKAN API failure (HTTP 200 but success: False)
@patch('requests.get')
def test_request_ckan_api_error(mock_get, mock_ckan_client, create_mock_response):
    mock_resp = create_mock_response(200, json_data=CKAN_ERROR_RESPONSE)
    mock_get.return_value = mock_resp

    result = mock_ckan_client._request('package_show')
    assert result is None


# --- API Method Tests (package_show, resource_show) ---


@patch('utils.ckan.CKANClient._request')
def test_package_show_valid(mock_request, mock_ckan_client):
    mock_request.return_value = PACKAGE_SHOW_SUCCESS['result']
    result = mock_ckan_client.package_show(PACKAGE_ID)

    mock_request.assert_called_once_with('package_show', params={'id': PACKAGE_ID})
    assert result == PACKAGE_SHOW_SUCCESS['result']


@patch('utils.ckan.CKANClient._request')
def test_resource_show_valid(mock_request, mock_ckan_client):
    mock_request.return_value = RESOURCE_SHOW_SUCCESS['result']
    result = mock_ckan_client.resource_show(RESOURCE_ID)

    mock_request.assert_called_once_with('resource_show', params={'id': RESOURCE_ID})
    assert result == RESOURCE_SHOW_SUCCESS['result']


# --- Update/Patch Methods Tests ---


@patch('utils.ckan.CKANClient._request')
def test_update_resource_fields_success(mock_request, mock_ckan_client):
    mock_request.return_value = RESOURCE_PATCH_SUCCESS['result']
    fields = {'custom_field': 'new_value', 'description': 'updated'}

    result = mock_ckan_client.update_resource_fields(RESOURCE_ID, fields)

    expected_payload = {'id': RESOURCE_ID, **fields}
    mock_request.assert_called_once_with('resource_patch', method='POST', json=expected_payload)
    assert result == RESOURCE_PATCH_SUCCESS['result']


@patch('utils.ckan.CKANClient._request')
def test_update_resource_fields_failure(mock_request, mock_ckan_client):
    """Test update_resource_fields when _request returns None (hits L65)."""
    mock_request.return_value = None

    result = mock_ckan_client.update_resource_fields(RESOURCE_ID, {'field': 'value'})

    assert result is None


@patch('utils.ckan.CKANClient._request')
def test_remove_resource_field_success(mock_request, mock_ckan_client):
    mock_request.return_value = RESOURCE_PATCH_SUCCESS['result']
    field_name = 'old_field'

    result = mock_ckan_client.remove_resource_field(RESOURCE_ID, field_name)

    expected_payload = {'id': RESOURCE_ID, field_name: None}
    mock_request.assert_called_once_with('resource_patch', method='POST', json=expected_payload)

    assert result == RESOURCE_PATCH_SUCCESS['result']


# --- Download Link Tests ---


@patch('utils.ckan.CKANClient.resource_show')
def test_get_download_link_not_found(mock_resource_show, mock_ckan_client):
    """Test _get_download_link when no URL is found (hits L86, L87)."""
    # Case 1: resource_show returns a dict without the URL
    mock_resource_show.return_value = {'id': RESOURCE_ID, 'name': 'No Link'}
    assert mock_ckan_client._get_download_link(RESOURCE_ID) is None

    # Case 2: resource_show fails and returns None
    mock_resource_show.return_value = None
    assert mock_ckan_client._get_download_link(RESOURCE_ID) is None


# --- Download Methods Tests ---


@patch('utils.ckan.CKANClient.resource_show')
@patch('pathlib.Path.write_bytes')
@patch('pathlib.Path.mkdir')  # Mock Path.mkdir to prevent file system creation
@patch('requests.get')
def test_download_resource_success(
    mock_get_download, mock_mkdir, mock_write_bytes, mock_resource_show, mock_ckan_client, create_mock_response
):
    """Test successful download (hits L96)."""

    # 1. Mock the API call to get the download URL
    mock_resource_show.return_value = RESOURCE_SHOW_SUCCESS['result']

    # 2. Mock the actual file download HTTP request
    file_content = b'mock,csv,data\n1,2,3'
    # The success path ensures L96 is hit by calling write_bytes
    mock_download_resp = create_mock_response(200, content=file_content)
    mock_get_download.return_value = mock_download_resp

    filename = 'custom-file.csv'
    output_dir = Path('/tmp/test-out')

    # 3. Call the method
    result_path = mock_ckan_client.download_resource(RESOURCE_ID, filename=filename, output_dir=output_dir)

    # 4. Assertions
    mock_resource_show.assert_called_once_with(RESOURCE_ID)

    # Check the file download was requested correctly
    download_url = RESOURCE_SHOW_SUCCESS['result']['download_url']
    mock_get_download.assert_called_once_with(download_url, timeout=30)

    # Check that the file content was written (covers L96)
    mock_write_bytes.assert_called_once_with(file_content)

    # Check the returned Path object
    expected_path = output_dir / filename
    assert result_path == expected_path


@patch('utils.ckan.CKANClient.resource_show')
def test_download_resource_no_url(mock_resource_show, mock_ckan_client):
    # Mock API response to show no download_url
    mock_resource_show.return_value = {'id': RESOURCE_ID, 'name': 'No Link'}

    with pytest.raises(TypeError):
        mock_ckan_client.download_resource(RESOURCE_ID)


@patch('utils.ckan.CKANClient.resource_show')
@patch('pathlib.Path.mkdir')
@patch('requests.get')
def test_download_resource_file_download_failure(
    mock_get_download, mock_mkdir, mock_resource_show, mock_ckan_client, create_mock_response
):
    # 1. Mock the API call to return a valid URL
    mock_resource_show.return_value = RESOURCE_SHOW_SUCCESS['result']

    # 2. Mock the download request to return a response that raises an HTTPError at raise_for_status() (hits L94)
    mock_download_resp = create_mock_response(404, raise_for_status_exc=requests.exceptions.HTTPError('File Not Found'))
    mock_get_download.return_value = mock_download_resp

    # 3. Call the method and assert RuntimeError is raised
    with pytest.raises(requests.exceptions.HTTPError):
        mock_ckan_client.download_resource(RESOURCE_ID)
