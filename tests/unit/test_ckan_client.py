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

    def test_initialization_with_user_agent(self):
        """Test client initialization includes configured user-agent header."""
        client = CKANClient(base_url='https://test.hdx.org', api_token='test-token-123', user_agent='TestUA/1.0.0')

        assert client.headers['Authorization'] == 'test-token-123'
        assert client.headers['User-Agent'] == 'TestUA/1.0.0'

    def test_initialization_without_user_agent(self):
        """Test client initialization does not include user-agent when omitted."""
        client = CKANClient(base_url='https://test.hdx.org', api_token='test-token-123')

        assert 'User-Agent' not in client.headers

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
        assert 'Error in _request' in caplog.text

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
        assert 'Error in _request' in caplog.text

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
        assert 'CKAN_API_TOKEN is required' in str(excinfo.value.original_exc)

    @patch.object(CKANClient, '_request')
    def test_update_resource_fields_failure(self, mock_request, client, caplog):
        """Test resource field update failure."""
        mock_request.return_value = None

        with caplog.at_level(logging.ERROR):
            result = client.update_resource_fields('res-123', {'field': 'value'})

        assert result is None
        assert 'Failed to update resource' in caplog.text

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
