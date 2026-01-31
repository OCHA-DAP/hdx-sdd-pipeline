"""Prompt manager for rendering Jinja2 templates."""

import logging
from pathlib import Path
from typing import Dict, Any
from jinja2 import Environment, FileSystemLoader

logger = logging.getLogger(__name__)


class PromptManager:
    """
    Manages prompt templates using Jinja2.

    Loads templates from the prompts/ directory and renders them
    with provided context.
    """

    def __init__(self, prompts_dir: str = 'prompts'):
        """
        Initialize prompt manager.

        Args:
            prompts_dir: Directory containing prompt templates
        """
        self.prompts_dir = Path(prompts_dir)

        if self.prompts_dir.exists():
            self.env = Environment(loader=FileSystemLoader(str(self.prompts_dir)), trim_blocks=True, lstrip_blocks=True)
        else:
            logger.warning(f'Prompts directory not found: {self.prompts_dir}')
            exit()
            self.env = None

    def get_prompt(self, prompt_name: str, version: str = 'v0', context: Dict[str, Any] = None) -> str:
        """
        Get and render a prompt template.

        Args:
            prompt_name: Name of the prompt (without extension)
            version: Version of the prompt (subdirectory)
            context: Context variables for template rendering

        Returns:
            Rendered prompt string

        Raises:
            FileNotFoundError: If template not found
        """
        if context is None:
            context = {}

        # Try to load template (FileSystemLoader already searches in prompts_dir)
        template_path = f'{prompt_name}/{version}.jinja'

        try:
            if self.env:
                template = self.env.get_template(template_path)
                return template.render(**context)
            else:
                # Fallback to simple templates
                return self._get_fallback_prompt(prompt_name, context)
        except Exception as e:
            logger.error(f'Failed to load template {template_path}: {e}')
            return self._get_fallback_prompt(prompt_name, context)

    def _get_fallback_prompt(self, prompt_name: str, context: Dict[str, Any]) -> str:
        """Fallback prompts when templates are not available"""

        if prompt_name == 'pii_detection':
            return f'''Classify the following column for PII (Personally Identifiable Information).

Column name: {context.get('column_name', 'unknown')}
Sample values: {context.get('sample_values', [])}

Identify the PII entity type. Respond with ONE of these types:
PERSON_NAME, EMAIL_ADDRESS, PHONE_NUMBER, LOCATION, ADDRESS, ID_NUMBER, 
AGE, DATE_OF_BIRTH, CREDIT_CARD, IP_ADDRESS, None

Respond with ONLY the entity type, nothing else.'''

        elif prompt_name == 'pii_reflection':
            return f'''Determine if the following PII column contains sensitive data.

Column name: {context.get('column_name', 'unknown')}
Entity type: {context.get('entity_type', 'unknown')}
Sample values: {context.get('sample_values', [])}
Table context: {context.get('table_context', 'unknown')}

Is this sensitive data? Respond with ONLY: "sensitive" or "non_sensitive".'''

        elif prompt_name == 'non_pii_classification':
            return f'''Classify the overall sensitivity of this table for non-PII aspects.

{context.get('table_summary', 'No summary available')}

ISP Rules: {context.get('isp_rules', {})}

Classify the sensitivity level. Respond with ONE of:
NON_SENSITIVE, MODERATE_SENSITIVE, HIGH_SENSITIVE, SEVERE_SENSITIVE

Provide your classification and brief explanation.'''

        else:
            return f'Prompt not found: {prompt_name}'
