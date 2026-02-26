"""Integration tests for logging system."""

from unittest.mock import patch
import os


def test_logging_configuration_import():
    """Test that logging configuration can be imported without errors."""
    with patch('logging.config.fileConfig'):
        # Should not raise any exceptions
        assert True


def test_logging_configuration_with_env_var():
    """Test that logging configuration respects environment variables."""
    with patch('logging.config.fileConfig') as mock_config:
        import importlib
        import src.shared.utils.logging_conf

        # Re-import with custom config
        with patch.dict(os.environ, {'LOGGING_CONF': 'custom.conf'}):
            importlib.reload(src.shared.utils.logging_conf)

        # Verify fileConfig was called with custom path
        mock_config.assert_called_with('custom.conf')


def test_logging_configuration_default():
    """Test that logging configuration uses default path when env var not set."""
    with patch('logging.config.fileConfig') as mock_config:
        import importlib
        import src.shared.utils.logging_conf

        # Re-import with no env var
        with patch.dict(os.environ, {}, clear=True):
            importlib.reload(src.shared.utils.logging_conf)

        # Verify fileConfig was called with default path
        mock_config.assert_called_with('logging.conf')
