"""Prompt manager for rendering Jinja2 templates."""

import logging
from pathlib import Path
from typing import Dict, Any, Optional
from jinja2 import Environment, FileSystemLoader, TemplateNotFound
import re

logger = logging.getLogger(__name__)


class PromptManager:
    """
    Manages prompt templates using Jinja2.

    Loads templates from the src/prompts/ directory and renders them
    with provided context. Automatically detects the latest version
    of each prompt category.
    """

    def __init__(self, prompts_dir: str = 'src/prompts', store: Optional[Any] = None):
        """
        Initialize prompt manager.

        Args:
            prompts_dir: Directory containing prompt templates
            store: RedisKeyValueStore or cache store instance for prompt caching
        """
        self.prompts_dir = Path(prompts_dir)

        if not self.prompts_dir.exists():
            raise FileNotFoundError(f'Prompts directory not found: {self.prompts_dir}')

        self.env = Environment(loader=FileSystemLoader(str(self.prompts_dir)), trim_blocks=True, lstrip_blocks=True)
        self.store = store

        # Cache of SpreadsheetPromptStrategy per category
        self._spreadsheet_strategies: Dict[str, Any] = {}

    def _get_spreadsheet_strategy(self, prompt_name: str):
        """Lazy load SpreadsheetPromptStrategy for any prompt category."""
        if prompt_name not in self._spreadsheet_strategies:
            from src.infrastructure.external.pii_prompt_strategy import (
                SpreadsheetPromptStrategy,
            )

            ws_name = prompt_name
            # Map category names to worksheet names
            category_mapping = {
                'pii_detection': 'personal_data_detection',
                'pii_reflection': 'personal_data_reflection',
                'non_pii_classification': 'non_personal_data_classificatio',
                'non_pii_default': 'non_personal_data_default_class',
                'non_pii_classification/default': 'non_personal_data_default_class',
                'readme_scan': 'readme',
            }
            ws_name = category_mapping.get(prompt_name, prompt_name)

            try:
                self._spreadsheet_strategies[prompt_name] = SpreadsheetPromptStrategy(
                    worksheet_name=ws_name,
                    store=self.store,
                )
            except Exception as e:
                logger.warning(f'Failed to initialize SpreadsheetPromptStrategy for {prompt_name}: {e}')
                self._spreadsheet_strategies[prompt_name] = False

        strat = self._spreadsheet_strategies[prompt_name]
        return strat if strat is not False else None

    def get_latest_version(self, prompt_name: str) -> Optional[str]:
        """
        Get the latest version for a prompt category.

        Args:
            prompt_name: Name of the prompt category

        Returns:
            Latest version string (e.g., 'v1') or None if not found
        """
        prompt_dir = self.prompts_dir / prompt_name

        if not prompt_dir.exists():
            logger.warning(f'Prompt category not found: {prompt_name}')
            return None

        # Find all version files (v0.jinja, v1.jinja, etc.)
        version_files = list(prompt_dir.glob('v*.jinja'))

        if not version_files:
            logger.warning(f'No version files found for prompt: {prompt_name}')
            return None

        # Extract version numbers and find the highest
        versions = []
        for file in version_files:
            match = re.match(r'v(\d+)\.jinja', file.name)
            if match:
                versions.append((int(match.group(1)), file.stem))

        if not versions:
            return None

        # Sort by version number and return the highest
        latest = sorted(versions, key=lambda x: x[0], reverse=True)[0][1]
        return latest

    def get_prompt(self, prompt_name: str, version: Optional[str] = None, context: Dict[str, Any] = None) -> str:
        """
        Get and render a prompt template.

        Args:
            prompt_name: Name of the prompt category (e.g., 'pii_detection')
            version: Version of the prompt (e.g., 'v0', 'v1', or None for latest)
            context: Context variables for template rendering

        Returns:
            Rendered prompt string

        Raises:
            FileNotFoundError: If template not found
        """
        if context is None:
            context = {}

        # Fetch active rules from Spreadsheet / Excel strategy if not manually passed
        if 'active_rules' not in context:
            strategy = self._get_spreadsheet_strategy(prompt_name)
            if strategy:
                rules = strategy.load_rules()
                if rules:
                    context['active_rules'] = rules

        # Auto-detect latest version if not specified
        if version is None or version == 'latest':
            version = self.get_latest_version(prompt_name)
            if version is None:
                raise FileNotFoundError(f'No versions found for prompt category: {prompt_name}')

        # Build template path
        template_path = f'{prompt_name}/{version}.jinja'

        try:
            template = self.env.get_template(template_path)
        except TemplateNotFound as e:
            logger.error(f'Template not found: {template_path}')
            raise FileNotFoundError(f'Template not found: {template_path}') from e

        try:
            rendered = template.render(**context)
            return rendered
        except Exception as e:
            logger.error(f'Failed to render template {template_path}: {e}')
            raise
