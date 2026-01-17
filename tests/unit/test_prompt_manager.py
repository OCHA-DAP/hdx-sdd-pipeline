"""Unit tests for PromptManager."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from src.shared.utils.prompt_manager import PromptManager


class TestPromptManager:
    """Test suite for PromptManager."""
    
    def test_initialization_with_existing_dir(self):
        """Test initialization with existing prompts directory."""
        with patch('pathlib.Path.exists', return_value=True):
            manager = PromptManager(prompts_dir="prompts")
            assert manager.prompts_dir == Path("prompts")
            assert manager.env is not None
    
    def test_initialization_with_nonexistent_dir(self):
        """Test initialization with non-existent directory."""
        with patch('pathlib.Path.exists', return_value=False):
            manager = PromptManager(prompts_dir="nonexistent")
            assert manager.env is None
    
    def test_get_prompt_fallback_pii_detection(self):
        """Test fallback prompt for PII detection."""
        with patch('pathlib.Path.exists', return_value=False):
            manager = PromptManager()
            
            context = {
                'column_name': 'email',
                'sample_values': ['test@example.com', 'user@test.com']
            }
            
            prompt = manager.get_prompt('pii_detection', context=context)
            
            assert 'email' in prompt
            assert 'test@example.com' in prompt
            assert 'EMAIL_ADDRESS' in prompt
    
    def test_get_prompt_fallback_pii_reflection(self):
        """Test fallback prompt for PII reflection."""
        with patch('pathlib.Path.exists', return_value=False):
            manager = PromptManager()
            
            context = {
                'column_name': 'email',
                'entity_type': 'EMAIL_ADDRESS',
                'sample_values': ['test@example.com'],
                'table_context': 'UserData'
            }
            
            prompt = manager.get_prompt('pii_reflection', context=context)
            
            assert 'email' in prompt
            assert 'EMAIL_ADDRESS' in prompt
            assert 'sensitive' in prompt.lower()
    
    def test_get_prompt_fallback_non_pii_classification(self):
        """Test fallback prompt for non-PII classification."""
        with patch('pathlib.Path.exists', return_value=False):
            manager = PromptManager()
            
            context = {
                'table_summary': 'Table: TestData\nRows: 100',
                'isp_rules': {'country': 'Ukraine'}
            }
            
            prompt = manager.get_prompt('non_pii_classification', context=context)
            
            assert 'TestData' in prompt
            assert 'Ukraine' in prompt
            assert 'SENSITIVE' in prompt
    
    def test_get_prompt_fallback_unknown(self):
        """Test fallback for unknown prompt."""
        with patch('pathlib.Path.exists', return_value=False):
            manager = PromptManager()
            
            prompt = manager.get_prompt('unknown_prompt', context={})
            
            assert 'Prompt not found' in prompt
            assert 'unknown_prompt' in prompt
    
    def test_get_prompt_with_template(self):
        """Test getting prompt with Jinja2 template."""
        with patch('pathlib.Path.exists', return_value=True):
            manager = PromptManager()
            
            # Mock template
            mock_template = Mock()
            mock_template.render.return_value = "Rendered prompt"
            
            manager.env = Mock()
            manager.env.get_template.return_value = mock_template
            
            context = {'column_name': 'test'}
            prompt = manager.get_prompt('pii_detection', context=context)
            
            assert prompt == "Rendered prompt"
            mock_template.render.assert_called_once_with(**context)
    
    def test_get_prompt_template_error_fallback(self):
        """Test that template errors fall back to default prompts."""
        with patch('pathlib.Path.exists', return_value=True):
            manager = PromptManager()
            
            manager.env = Mock()
            manager.env.get_template.side_effect = Exception("Template not found")
            
            context = {'column_name': 'email', 'sample_values': ['test@example.com']}
            prompt = manager.get_prompt('pii_detection', context=context)
            
            # Should fall back to default prompt
            assert 'email' in prompt
            assert 'test@example.com' in prompt
    
    def test_get_prompt_with_version(self):
        """Test getting prompt with specific version."""
        with patch('pathlib.Path.exists', return_value=True):
            manager = PromptManager()
            
            mock_template = Mock()
            mock_template.render.return_value = "V1 prompt"
            
            manager.env = Mock()
            manager.env.get_template.return_value = mock_template
            
            prompt = manager.get_prompt('pii_detection', version='v1', context={})
            
            # Should request v1 template
            manager.env.get_template.assert_called_with('src/prompts/pii_detection/v1.jinja2')
    
    def test_get_prompt_empty_context(self):
        """Test getting prompt with empty context."""
        with patch('pathlib.Path.exists', return_value=False):
            manager = PromptManager()
            
            # Should not raise error with empty context
            prompt = manager.get_prompt('pii_detection', context={})
            
            assert isinstance(prompt, str)
            assert len(prompt) > 0
    
    def test_get_prompt_none_context(self):
        """Test getting prompt with None context."""
        with patch('pathlib.Path.exists', return_value=False):
            manager = PromptManager()
            
            # Should handle None context
            prompt = manager.get_prompt('pii_detection', context=None)
            
            assert isinstance(prompt, str)
            assert len(prompt) > 0
