"""Unit tests for Config."""

import os
from unittest.mock import patch
from config.config import Config


def test_pii_prompt_strategy_defaults_to_google_sheets_with_fallback_urls():
    """Test PII prompt strategies fallback to default Google Sheet URLs when env vars are unset."""
    with patch.dict(os.environ, {}, clear=True):
        cfg = Config()
        assert cfg.PII_DETECTION_GOOGLE_SHEET_URL == 'https://docs.google.com/spreadsheets/d/1vbn0d3tqZB0dGJTUdBPfn-oRU9m7xPeIwjXH4HW0eYI/edit?gid=0#gid=0'
        assert cfg.PII_PROMPT_STRATEGY == 'google_sheets'
        assert cfg.PII_DETECTION_WORKSHEET_NAME == 'PII detection'
        assert cfg.PII_REFLECTION_GOOGLE_SHEET_URL == 'https://docs.google.com/spreadsheets/d/1vbn0d3tqZB0dGJTUdBPfn-oRU9m7xPeIwjXH4HW0eYI/edit?gid=0#gid=0'
        assert cfg.PII_REFLECTION_PROMPT_STRATEGY == 'google_sheets'
        assert cfg.PII_REFLECTION_WORKSHEET_NAME == 'PII reflection'


def test_pii_prompt_strategy_defaults_to_google_sheets_when_sheet_url_set():
    """Test PII prompt strategies resolve to google_sheets when Google Sheet URLs are provided."""
    env = {
        'PII_DETECTION_GOOGLE_SHEET_URL': 'https://docs.google.com/spreadsheets/d/test_det/edit',
        'PII_REFLECTION_GOOGLE_SHEET_URL': 'https://docs.google.com/spreadsheets/d/test_refl/edit',
    }
    with patch.dict(os.environ, env, clear=True):
        cfg = Config()
        assert cfg.PII_DETECTION_GOOGLE_SHEET_URL == 'https://docs.google.com/spreadsheets/d/test_det/edit'
        assert cfg.PII_PROMPT_STRATEGY == 'google_sheets'
        assert cfg.PII_REFLECTION_GOOGLE_SHEET_URL == 'https://docs.google.com/spreadsheets/d/test_refl/edit'
        assert cfg.PII_REFLECTION_PROMPT_STRATEGY == 'google_sheets'
