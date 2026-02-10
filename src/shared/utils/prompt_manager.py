"""Prompt manager for rendering Jinja2 templates."""

import logging
from pathlib import Path
from typing import Dict, Any, Optional
from jinja2 import Environment, FileSystemLoader
import re

logger = logging.getLogger(__name__)


class PromptManager:
    """
    Manages prompt templates using Jinja2.

    Loads templates from the src/prompts/ directory and renders them
    with provided context. Automatically detects the latest version
    of each prompt category.
    """

    def __init__(self, prompts_dir: str = 'src/prompts'):
        """
        Initialize prompt manager.

        Args:
            prompts_dir: Directory containing prompt templates
        """
        self.prompts_dir = Path(prompts_dir)

        if not self.prompts_dir.exists():
            raise FileNotFoundError(f'Prompts directory not found: {self.prompts_dir}')

        self.env = Environment(
            loader=FileSystemLoader(str(self.prompts_dir)),
            trim_blocks=True,
            lstrip_blocks=True
        )
        

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

    def get_prompt(
        self,
        prompt_name: str,
        version: Optional[str] = None,
        context: Dict[str, Any] = None
    ) -> str:
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

        # Auto-detect latest version if not specified
        if version is None or version == 'latest':
            version = self.get_latest_version(prompt_name)
            if version is None:
                raise FileNotFoundError(
                    f'No versions found for prompt category: {prompt_name}'
                )

        # Build template path
        template_path = f'{prompt_name}/{version}.jinja'

        try:
            template = self.env.get_template(template_path)
            rendered = template.render(**context)
            logger.info(f'Successfully rendered template: {rendered}')
            return rendered
        except Exception as e:
            logger.error(f'Failed to load template {template_path}: {e}')
            raise FileNotFoundError(
                f'Template not found or failed to render: {template_path}'
            ) from e
