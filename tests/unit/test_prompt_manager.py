"""Unit tests for PromptManager."""

from unittest.mock import Mock, patch
from pathlib import Path
import pytest
from jinja2 import TemplateNotFound

from src.shared.utils.prompt_manager import PromptManager


class TestPromptManager:
    """Test suite for PromptManager."""

    def test_initialization_with_existing_dir(self):
        """Test initialization with existing prompts directory."""
        with patch('pathlib.Path.exists', return_value=True):
            manager = PromptManager(prompts_dir='prompts')
            assert manager.prompts_dir == Path('prompts')
            assert manager.env is not None

    def test_initialization_with_nonexistent_dir(self):
        """Test initialization with non-existent directory."""
        with patch('src.shared.utils.prompt_manager.Path.exists', return_value=False):
            with pytest.raises(FileNotFoundError, match='Prompts directory not found'):
                PromptManager(prompts_dir='nonexistent')

    @patch('src.shared.utils.prompt_manager.Path.exists')
    @patch('src.shared.utils.prompt_manager.Path.glob')
    def test_get_latest_version(self, mock_glob, mock_exists):
        """Test getting latest version of prompt."""
        mock_exists.return_value = True

        # Mock glob to return some version files
        file1 = Mock()
        file1.name = 'v1.jinja'
        file1.stem = 'v1'

        file2 = Mock()
        file2.name = 'v2.jinja'
        file2.stem = 'v2'

        mock_glob.return_value = [file1, file2]

        with patch('src.shared.utils.prompt_manager.Environment'):
            manager = PromptManager()
            version = manager.get_latest_version('pii_detection')
            assert version == 'v2'

    @patch('src.shared.utils.prompt_manager.Path.exists')
    def test_get_latest_version_no_dir(self, mock_exists):
        """Test getting latest version when prompt dir missing."""
        # First exists call is for init (True), second for get_latest_version (False)
        mock_exists.side_effect = [True, False]

        with patch('src.shared.utils.prompt_manager.Environment'):
            manager = PromptManager()
            version = manager.get_latest_version('pii_detection')
            assert version is None

    @patch('src.shared.utils.prompt_manager.Path.exists')
    @patch('src.shared.utils.prompt_manager.Path.glob')
    def test_get_latest_version_no_files(self, mock_glob, mock_exists):
        """Test getting latest version when no files found."""
        mock_exists.return_value = True
        mock_glob.return_value = []

        with patch('src.shared.utils.prompt_manager.Environment'):
            manager = PromptManager()
            version = manager.get_latest_version('pii_detection')
            assert version is None

    @patch('src.shared.utils.prompt_manager.Path.exists', return_value=True)
    def test_get_prompt_success(self, mock_exists):
        """Test successfully getting a prompt."""
        with patch('src.shared.utils.prompt_manager.Environment') as MockEnv:
            mock_template = Mock()
            mock_template.render.return_value = 'Rendered Prompt'

            mock_env_instance = MockEnv.return_value
            mock_env_instance.get_template.return_value = mock_template

            manager = PromptManager()

            # Mock get_latest_version to return v1
            with patch.object(manager, 'get_latest_version', return_value='v1'):
                prompt = manager.get_prompt('pii_detection', context={'key': 'value'})

                assert prompt == 'Rendered Prompt'
                mock_env_instance.get_template.assert_called_with('pii_detection/v1.jinja')
                mock_template.render.assert_called_with(key='value')

    @patch('src.shared.utils.prompt_manager.Path.exists', return_value=True)
    def test_get_prompt_with_explicit_version(self, mock_exists):
        """Test getting a prompt with explicit version."""
        with patch('src.shared.utils.prompt_manager.Environment') as MockEnv:
            mock_template = Mock()
            mock_template.render.return_value = 'V2 Prompt'

            mock_env_instance = MockEnv.return_value
            mock_env_instance.get_template.return_value = mock_template

            manager = PromptManager()

            prompt = manager.get_prompt('pii_detection', version='v2')

            assert prompt == 'V2 Prompt'
            mock_env_instance.get_template.assert_called_with('pii_detection/v2.jinja')

    @patch('src.shared.utils.prompt_manager.Path.exists', return_value=True)
    def test_get_prompt_not_found(self, mock_exists):
        """Test getting a prompt that doesn't exist."""
        with patch('src.shared.utils.prompt_manager.Environment') as MockEnv:
            mock_env_instance = MockEnv.return_value
            mock_env_instance.get_template.side_effect = TemplateNotFound('template_name')

            manager = PromptManager()

            with patch.object(manager, 'get_latest_version', return_value='v1'):
                with pytest.raises(FileNotFoundError, match='Template not found'):
                    manager.get_prompt('pii_detection')

    @patch('src.shared.utils.prompt_manager.Path.exists', return_value=True)
    def test_get_prompt_render_error(self, mock_exists):
        """Test error during rendering propagates."""
        with patch('src.shared.utils.prompt_manager.Environment') as MockEnv:
            mock_template = Mock()
            mock_template.render.side_effect = ValueError('Render error')

            mock_env_instance = MockEnv.return_value
            mock_env_instance.get_template.return_value = mock_template

            manager = PromptManager()

            with patch.object(manager, 'get_latest_version', return_value='v1'):
                with pytest.raises(ValueError, match='Render error'):
                    manager.get_prompt('pii_detection')

    @patch('src.shared.utils.prompt_manager.Path.exists', return_value=True)
    def test_get_prompt_no_versions(self, mock_exists):
        """Test getting a prompt when no versions exist."""
        with patch('src.shared.utils.prompt_manager.Environment'):
            manager = PromptManager()

            with patch.object(manager, 'get_latest_version', return_value=None):
                with pytest.raises(FileNotFoundError, match='No versions found'):
                    manager.get_prompt('pii_detection')
