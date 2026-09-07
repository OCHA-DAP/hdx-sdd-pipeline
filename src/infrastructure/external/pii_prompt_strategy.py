"""Strategy for loading and rendering PII detection prompts from Google Sheets."""

import logging
from typing import Dict, Any, Optional
from jinja2 import Environment

logger = logging.getLogger(__name__)


class GoogleSheetsPIIPromptStrategy:
    """
    Strategy to retrieve and construct PII detection prompt from Google Sheets.

    Reads the worksheet containing columns `section_id`, `type`, and `content`.
    Combines the `content` cells in row order into a single Jinja template string,
    and caches it for rendering.
    """

    def __init__(
        self,
        spreadsheet_url: str = 'https://docs.google.com/spreadsheets/d/1vbn0d3tqZB0dGJTUdBPfn-oRU9m7xPeIwjXH4HW0eYI/edit?gid=0#gid=0',
        worksheet_name: str = 'PII detection',
    ):
        self.spreadsheet_url = spreadsheet_url
        self.worksheet_name = worksheet_name
        self._cached_template_str: Optional[str] = None
        self._jinja_env = Environment(trim_blocks=True, lstrip_blocks=True)

    def load_template_string(self, force_refresh: bool = False) -> Optional[str]:
        """
        Fetch worksheet rows from Google Sheets and build prompt template string.

        Args:
            force_refresh: If True, bypass internal cache and reload from Google Sheets.

        Returns:
            Template string or None if loading fails.
        """
        if self._cached_template_str is not None and not force_refresh:
            return self._cached_template_str

        from src.infrastructure.external.google_sheets_client import get_gsheets

        try:
            spreadsheet = get_gsheets().open_by_url(self.spreadsheet_url)
            worksheet = spreadsheet.worksheet(self.worksheet_name)
            values = worksheet.get_all_values()
        except Exception as e:
            logger.error(f'Failed to read PII detection Google Sheet ({self.worksheet_name}): {e}')
            return None

        if not values or len(values) < 2:
            logger.error(f'PII detection Google Sheet worksheet "{self.worksheet_name}" has insufficient rows')
            return None

        header = [h.strip().lower() for h in values[0]]
        rows = values[1:]

        if 'content' not in header:
            logger.error(
                f'PII detection Google Sheet is missing required column "content". Available: {values[0]}'
            )
            return None

        content_idx = header.index('content')

        # Combine content from all non-empty rows in worksheet order
        prompt_parts = []
        for row in rows:
            if content_idx < len(row):
                cell_value = str(row[content_idx]).strip()
                if cell_value:
                    prompt_parts.append(cell_value)

        if not prompt_parts:
            logger.error(f'No content found in PII detection Google Sheet worksheet "{self.worksheet_name}"')
            return None

        template_str = '\n\n'.join(prompt_parts)
        self._cached_template_str = template_str
        logger.info(f'Successfully loaded PII detection prompt from Google Sheet ({len(prompt_parts)} sections)')
        return self._cached_template_str

    def render(self, context: Dict[str, Any], force_refresh: bool = False) -> Optional[str]:
        """
        Render the PII detection prompt template with provided context.

        Args:
            context: Template context dictionary (e.g. column_name, sample_values)
            force_refresh: If True, reload template from Google Sheets.

        Returns:
            Rendered prompt string or None if loading/rendering fails.
        """
        template_str = self.load_template_string(force_refresh=force_refresh)
        if not template_str:
            return None

        try:
            template = self._jinja_env.from_string(template_str)
            return template.render(**context)
        except Exception as e:
            logger.error(f'Failed to render PII detection template from Google Sheet: {e}')
            return None
