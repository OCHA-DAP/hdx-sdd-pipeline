"""Unit tests for Config."""

import os
from unittest.mock import patch
from config.config import Config


def test_google_sheet_url_default():
    """Test GOOGLE_SHEET_URL has expected default when unset."""
    with patch.dict(os.environ, {}, clear=True):
        cfg = Config()
        assert (
            cfg.GOOGLE_SHEET_URL
            == 'https://docs.google.com/spreadsheets/d/1vbn0d3tqZB0dGJTUdBPfn-oRU9m7xPeIwjXH4HW0eYI/edit'
        )


def test_google_sheet_url_custom_env():
    """Test GOOGLE_SHEET_URL can be overridden via environment variable."""
    custom_url = 'https://docs.google.com/spreadsheets/d/custom-sheet-id/edit'
    with patch.dict(os.environ, {'GOOGLE_SHEET_URL': custom_url}, clear=True):
        cfg = Config()
        assert cfg.GOOGLE_SHEET_URL == custom_url
