"""Tests for CKAN client."""

import pytest
from unittest.mock import Mock, patch
import requests
from src.shared.utils.ckan import CKANClient
from src.shared.utils.exception_handler import ContextualError
import logging


class TestCKANClient:
    """Test suite for CKANClient."""

    @pytest.fixture
    def client(self):
        """Create a CKAN client instance."""
        return CKANClient(base_url='https://test.hdx.org', api_token='test-token-123')

    @pytest.fixture
    def client_no_token(self):
        """Create a CKAN client without API token."""
        return CKANClient(base_url='https://test.hdx.org')

    def test_initialization_with_token(self, client):
        """Test client initialization with API token."""
        assert client.base_url == 'https://test.hdx.org'
        assert client.api_token == 'test-token-123'
        assert 'Authorization' in client.headers
        assert client.headers['Authorization'] == 'test-token-123'

    def test_initialization_without_token(self, client_no_token):
        """Test client initialization without API token."""
        assert client_no_token.base_url == 'https://test.hdx.org'
        assert client_no_token.api_token is None
        assert client_no_token.headers == {}

    @patch('src.shared.utils.ckan.requests.get')
    def test_request_get_success(self, mock_get, client):
        """Test successful GET request."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'success': True, 'result': {'id': 'test-123', 'name': 'Test Resource'}}
        mock_get.return_value = mock_response

        result = client._request('resource_show', method='GET', params={'id': 'test-123'})

        assert result == {'id': 'test-123', 'name': 'Test Resource'}
        mock_get.assert_called_once()

    @patch('src.shared.utils.ckan.requests.post')
    def test_request_post_success(self, mock_post, client):
        """Test successful POST request."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'success': True, 'result': {'id': 'test-123', 'updated': True}}
        mock_post.return_value = mock_response

        result = client._request('resource_patch', method='POST', json={'id': 'test-123'})

        assert result == {'id': 'test-123', 'updated': True}
        mock_post.assert_called_once()

    @patch('src.shared.utils.ckan.requests.get')
    def test_request_http_error_401(self, mock_get, client, caplog):
        """Test handling of 401 authentication error."""

        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        mock_get.return_value = mock_response

        with pytest.raises(ContextualError) as excinfo:
            client._request('resource_show')

        # Verify wrapper captured the 401
        assert isinstance(excinfo.value.original_exc, requests.exceptions.HTTPError)
        assert excinfo.value.original_exc.response.status_code == 401
        assert 'Authentication failed' in caplog.text

    @patch('src.shared.utils.ckan.requests.get')
    def test_request_http_error_403(self, mock_get, client, caplog):
        """Test handling of 403 permission error."""

        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        mock_get.return_value = mock_response

        with pytest.raises(ContextualError) as excinfo:
            client._request('resource_patch')

        # Verify wrapper captured the 403
        assert isinstance(excinfo.value.original_exc, requests.exceptions.HTTPError)
        assert excinfo.value.original_exc.response.status_code == 403
        assert 'Permission denied' in caplog.text

    @patch('src.shared.utils.ckan.requests.get')
    def test_request_api_error(self, mock_get, client, caplog):
        """Test handling of API error response."""

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'success': False, 'error': {'message': 'Resource not found'}}
        mock_get.return_value = mock_response

        with caplog.at_level(logging.ERROR):
            result = client._request('resource_show')

        assert result is None
        assert 'CKAN API returned error' in caplog.text

    @patch.object(CKANClient, '_request')
    def test_package_show(self, mock_request, client):
        """Test package_show method."""
        mock_request.return_value = {'id': 'pkg-123', 'name': 'Test Package'}

        result = client.package_show('pkg-123')

        assert result == {'id': 'pkg-123', 'name': 'Test Package'}
        mock_request.assert_called_once_with('package_show', params={'id': 'pkg-123'})

    @patch.object(CKANClient, '_request')
    def test_resource_show(self, mock_request, client):
        """Test resource_show method."""
        mock_request.return_value = {'id': 'res-123', 'name': 'Test Resource'}

        result = client.resource_show('res-123')

        assert result == {'id': 'res-123', 'name': 'Test Resource'}
        mock_request.assert_called_once_with('resource_show', params={'id': 'res-123'})

    @patch.object(CKANClient, '_request')
    def test_update_resource_fields_success(self, mock_request, client):
        """Test successful resource field update."""
        mock_request.return_value = {'id': 'res-123', 'sdd_report': 'updated'}

        result = client.update_resource_fields('res-123', {'sdd_report': 'updated'})

        assert result == {'id': 'res-123', 'sdd_report': 'updated'}
        mock_request.assert_called_once_with(
            'resource_patch', method='POST', json={'id': 'res-123', 'sdd_report': 'updated'}
        )

    def test_update_resource_fields_no_token(self, client_no_token):
        """Test update_resource_fields raises error without API token."""

        with pytest.raises(ContextualError) as excinfo:
            client_no_token.update_resource_fields('res-123', {'field': 'value'})

        # Verify the original exception was EnvironmentError
        assert isinstance(excinfo.value.original_exc, EnvironmentError)
        assert 'HDX_API_TOKEN is required' in str(excinfo.value.original_exc)

    @patch.object(CKANClient, '_request')
    def test_update_resource_fields_failure(self, mock_request, client, caplog):
        """Test resource field update failure."""
        mock_request.return_value = None

        with caplog.at_level(logging.ERROR):
            result = client.update_resource_fields('res-123', {'field': 'value'})

        assert result is None
        assert 'Failed to update resource' in caplog.text

    @patch.object(CKANClient, '_request')
    def test_remove_resource_field(self, mock_request, client):
        """Test removing a resource field."""
        mock_request.return_value = {'id': 'res-123', 'field': None}

        client.remove_resource_field('res-123', 'sdd_report')

        mock_request.assert_called_once_with(
            'resource_patch', method='POST', json={'id': 'res-123', 'sdd_report': None}
        )

    def test_remove_resource_field_no_token(self, client_no_token):
        """Test remove_resource_field raises error without API token."""
        with pytest.raises(ContextualError) as excinfo:
            client_no_token.remove_resource_field('res-123', 'field')

        assert isinstance(excinfo.value.original_exc, EnvironmentError)
        assert 'HDX_API_TOKEN is required' in str(excinfo.value.original_exc)

    @patch.object(CKANClient, 'resource_show')
    def test_get_download_link_success(self, mock_resource_show, client):
        """Test getting download link from resource."""
        mock_resource_show.return_value = {'id': 'res-123', 'download_url': 'https://test.hdx.org/download/file.csv'}

        result = client._get_download_link('res-123')

        assert result == 'https://test.hdx.org/download/file.csv'

    @patch.object(CKANClient, 'resource_show')
    def test_get_download_link_no_url(self, mock_resource_show, client, caplog):
        """Test getting download link when URL is missing."""

        mock_resource_show.return_value = {'id': 'res-123'}

        with caplog.at_level(logging.INFO):
            result = client._get_download_link('res-123')

        assert result is None
        assert 'No download URL found' in caplog.text

    @patch('src.shared.utils.ckan.requests.get')
    def test_download_file_success(self, mock_get, client, tmp_path):
        """Test successful file download."""
        mock_response = Mock()
        mock_response.content = b'test file content'
        mock_get.return_value = mock_response

        output_dir = tmp_path / 'downloads'
        result = client._download_file('https://test.hdx.org/file.csv', 'file.csv', output_dir)

        assert result.exists()
        assert result.read_bytes() == b'test file content'
        assert result.name == 'file.csv'

    @patch.object(CKANClient, '_get_download_link')
    @patch.object(CKANClient, '_download_file')
    def test_download_resource_success(self, mock_download, mock_get_link, client, tmp_path):
        """Test successful resource download."""
        mock_get_link.return_value = 'https://test.hdx.org/file.csv'
        mock_download.return_value = tmp_path / 'file.csv'

        result = client.download_resource('res-123', output_dir=tmp_path)

        assert result == tmp_path / 'file.csv'
        mock_get_link.assert_called_once_with('res-123')
        mock_download.assert_called_once()

    @patch.object(CKANClient, '_get_download_link')
    def test_download_resource_no_url(self, mock_get_link, client):
        """Test download_resource raises error when no URL found."""
        mock_get_link.return_value = None

        with pytest.raises(ContextualError) as excinfo:
            client.download_resource('res-123')

        assert isinstance(excinfo.value.original_exc, ValueError)
        assert 'No download URL found' in str(excinfo.value.original_exc)

    @patch('src.shared.utils.ckan.requests.get')
    def test_request_timeout(self, mock_get, client):
        """Test request timeout handling."""
        mock_get.side_effect = requests.exceptions.Timeout()

        with pytest.raises(ContextualError) as excinfo:
            client._request('resource_show')

        assert isinstance(excinfo.value.original_exc, requests.exceptions.Timeout)

    @patch('src.shared.utils.ckan.requests.get')
    def test_request_connection_error(self, mock_get, client):
        """Test connection error handling."""
        mock_get.side_effect = requests.exceptions.ConnectionError()

        with pytest.raises(ContextualError) as excinfo:
            client._request('resource_show')

        assert isinstance(excinfo.value.original_exc, requests.exceptions.ConnectionError)
