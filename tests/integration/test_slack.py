"""Integration tests for Slack notification system."""

from unittest.mock import Mock, patch
from config.config import SlackClientWrapper
import slack_sdk.errors as slack_errors


class TestSlackClientWrapper:
    """Test suite for Slack client wrapper."""

    def setup_method(self):
        """Set up test environment before each test."""
        self.test_channel = 'test-channel'
        self.test_message = 'Test message'

    @patch('config.config.get_config')
    def test_init_with_token(self, mock_get_config):
        """Test SlackClientWrapper initialization with valid token."""
        mock_config = Mock()
        mock_config.HDX_SDD_SLACK_CHANNEL = self.test_channel
        mock_config.HDX_SDD_SLACK_ACCESS_TOKEN = 'test-token'
        mock_get_config.return_value = mock_config

        with patch('slack_sdk.WebClient') as mock_web_client:
            slack_wrapper = SlackClientWrapper()

            # Verify WebClient was initialized with token
            mock_web_client.assert_called_once_with(token='test-token')
            assert slack_wrapper.slack_client == mock_web_client.return_value
            assert slack_wrapper.slack_channel == self.test_channel

    @patch('config.config.get_config')
    def test_init_without_token(self, mock_get_config):
        """Test SlackClientWrapper initialization without token."""
        mock_config = Mock()
        mock_config.HDX_SDD_SLACK_CHANNEL = self.test_channel
        mock_config.HDX_SDD_SLACK_ACCESS_TOKEN = None
        mock_get_config.return_value = mock_config

        slack_wrapper = SlackClientWrapper()

        # Verify client is None when no token
        assert slack_wrapper.slack_client is None
        assert slack_wrapper.slack_channel == self.test_channel

    @patch('config.config.get_config')
    def test_post_to_slack_channel_success(self, mock_get_config):
        """Test successful message posting to Slack."""
        mock_config = Mock()
        mock_config.HDX_SDD_SLACK_CHANNEL = self.test_channel
        mock_config.HDX_SDD_SLACK_ACCESS_TOKEN = 'test-token'
        mock_get_config.return_value = mock_config

        slack_wrapper = SlackClientWrapper()

        # Mock successful post
        slack_wrapper.slack_client.chat_postMessage = Mock()

        # Test posting message
        slack_wrapper.post_to_slack_channel(self.test_message)

        # Verify the message was posted with correct format
        slack_wrapper.slack_client.chat_postMessage.assert_called_once_with(
            channel=self.test_channel, text=f'[SDD Pipeline] {self.test_message}'
        )

    @patch('config.config.get_config')
    def test_post_to_slack_channel_without_client(self, mock_get_config):
        """Test message posting when Slack client is not initialized."""
        mock_config = Mock()
        mock_config.HDX_SDD_SLACK_CHANNEL = self.test_channel
        mock_config.HDX_SDD_SLACK_ACCESS_TOKEN = None
        mock_get_config.return_value = mock_config

        slack_wrapper = SlackClientWrapper()

        # Test posting message without client
        slack_wrapper.post_to_slack_channel(self.test_message)

        # Should not raise any exceptions
        assert True

    @patch('config.config.get_config')
    def test_post_to_slack_channel_api_error(self, mock_get_config):
        """Test handling of Slack API errors."""
        mock_config = Mock()
        mock_config.HDX_SDD_SLACK_CHANNEL = self.test_channel
        mock_config.HDX_SDD_SLACK_ACCESS_TOKEN = 'test-token'
        mock_get_config.return_value = mock_config

        slack_wrapper = SlackClientWrapper()

        # Mock Slack API error
        error_response = {'error': 'Invalid channel'}
        api_error = slack_errors.SlackApiError(message='Invalid channel', response=error_response)
        slack_wrapper.slack_client.chat_postMessage = Mock(side_effect=api_error)

        # Should not raise exception
        slack_wrapper.post_to_slack_channel(self.test_message)

        # Verify the error was handled
        slack_wrapper.slack_client.chat_postMessage.assert_called_once()

    @patch('config.config.get_config')
    def test_post_to_slack_channel_general_exception(self, mock_get_config):
        """Test handling of general exceptions during Slack posting."""
        mock_config = Mock()
        mock_config.HDX_SDD_SLACK_CHANNEL = self.test_channel
        mock_config.HDX_SDD_SLACK_ACCESS_TOKEN = 'test-token'
        mock_get_config.return_value = mock_config

        slack_wrapper = SlackClientWrapper()

        # Mock general exception
        slack_wrapper.slack_client.chat_postMessage = Mock(side_effect=Exception('Network error'))

        # Should not raise exception
        slack_wrapper.post_to_slack_channel(self.test_message)

        # Verify the method was called despite exception
        slack_wrapper.slack_client.chat_postMessage.assert_called_once()
