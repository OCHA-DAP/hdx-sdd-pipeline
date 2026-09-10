"""Unit tests for Config."""

import os
from unittest.mock import patch
from config.config import Config


def test_pii_prompt_strategy_defaults_to_local_when_sheet_url_unset():
    """Test PII prompt strategies default to local when Google Sheet URLs are unset."""
    with patch.dict(os.environ, {}, clear=True):
        cfg = Config()
        assert cfg.PII_DETECTION_GOOGLE_SHEET_URL == ''
        assert cfg.PII_PROMPT_STRATEGY == 'local'
        assert cfg.PII_REFLECTION_GOOGLE_SHEET_URL == ''
        assert cfg.PII_REFLECTION_PROMPT_STRATEGY == 'local'


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
