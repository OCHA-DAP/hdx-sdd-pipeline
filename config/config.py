"""config/config.py: Centralized configuration for the HDX SDD pipeline."""

import os
from dataclasses import dataclass
import logging
from urllib.parse import urlparse
import slack_sdk
import slack_sdk.errors as slack_errors
from dotenv import load_dotenv
from src.version import __version__

logger = logging.getLogger(__name__)

load_dotenv()


@dataclass
class Config:
    # Redis / Worker
    WORKER_ENABLED: bool = os.getenv('WORKER_ENABLED', 'true').lower() == 'true'
    REDIS_STREAM_STREAM_NAME: str = os.getenv('REDIS_STREAM_STREAM_NAME', 'hdx_event_stream')
    REDIS_STREAM_GROUP_NAME: str = os.getenv('REDIS_STREAM_GROUP_NAME', 'hdx_sdd_group')
    REDIS_STREAM_CONSUMER_NAME: str = os.getenv('REDIS_STREAM_CONSUMER_NAME', 'hdx_sdd_consumer_1')
    REDIS_STREAM_HOST: str = os.getenv('REDIS_STREAM_HOST', 'redis')
    REDIS_STREAM_PORT: int = int(os.getenv('REDIS_STREAM_PORT', '6379'))
    REDIS_STREAM_DB: int = int(os.getenv('REDIS_STREAM_DB', '7'))
    # CKAN
    HDX_URL: str = os.getenv('HDX_URL', '')
    HDX_KEY: str = os.getenv('HDX_KEY', '')
    HDX_URL_PROD: str = os.getenv('HDX_URL_PROD', '')
    HDX_KEY_PROD: str = os.getenv('HDX_KEY_PROD', '')
    SDD_USER_AGENT: str = os.getenv('SDD_USER_AGENT', f'HDXINTERNAL:SDDPipeline/{__version__}')

    # Processing / Models
    RERUN: bool = os.getenv('RERUN', 'false').lower() == 'true'
    PII_DETECT_MODEL: str = os.getenv('PII_DETECT_MODEL', 'pii-detect-v1')
    PII_REFLECT_MODEL: str = os.getenv('PII_REFLECT_MODEL', 'pii-reflect-v1')
    NON_PII_DETECT_MODEL: str = os.getenv('NON_PII_DETECT_MODEL', 'non-pii-detect-v1')
    README_SCAN_MODEL: str = os.getenv('README_SCAN_MODEL', 'readme-scan-v1')

    # Processing steps
    PERSONAL_DATA_DETECTION = os.getenv('PERSONAL_DATA_DETECTION', 'true').lower() == 'true'
    PERSONAL_DATA_REFLECTION = os.getenv('PERSONAL_DATA_REFLECTION', 'true').lower() == 'true'
    NON_PERSONAL_DATA_DETECTION = os.getenv('NON_PERSONAL_DATA_DETECTION', 'true').lower() == 'true'
    README_SCAN = os.getenv('README_SCAN', 'true').lower() == 'true'
    CKAN_UPDATE = os.getenv('CKAN_UPDATE', 'true').lower() == 'true'

    # GLiNER fast PII pre-scan (FR-SDD-057)
    GLINER_SCAN: bool = os.getenv('GLINER_SCAN', 'false').lower() == 'true'
    GLINER_MODEL: str = os.getenv('GLINER_MODEL', 'gliner-community/gliner_small-v2.5')
    GLINER_THRESHOLD: float = float(os.getenv('GLINER_THRESHOLD', '0.9'))
    GLINER_BATCH_SIZE: int = int(os.getenv('GLINER_BATCH_SIZE', '0'))

    # Directories
    OUTPUT_DIR: str = os.getenv('OUTPUT_DIR', '/tmp/reports')
    DOWNLOAD_DIR: str = os.getenv('DOWNLOAD_DIR', '/tmp/download')

    # ISP Configuration
    ISP_STRATEGY: str = os.getenv('ISP_STRATEGY', 'local')  # 'local' or 'google_sheets'
    ISP_GOOGLE_SHEET_URL: str = os.getenv(
        'ISP_GOOGLE_SHEET_URL',
        'https://docs.google.com/spreadsheets/d/1Z5wj6H6WV2E8VN9r6y8AfdgOltTNL-z2KIlbnNzzPok/edit?gid=886310371#gid=886310371',
    )
    ISP_LOCAL_JSON_PATH: str = os.getenv('ISP_LOCAL_JSON_PATH', 'data/isps.json')
    GOOGLE_SHEETS_PRIVATE_KEY: str = os.getenv('GOOGLE_SHEETS_PRIVATE_KEY', '')
    GOOGLE_SHEETS_CLIENT_EMAIL: str = os.getenv('GOOGLE_SHEETS_CLIENT_EMAIL', '')
    GOOGLE_SHEETS_TOKEN_URI: str = os.getenv('GOOGLE_SHEETS_TOKEN_URI', '')

    # OpenAI
    OPENAI_ENDPOINT: str = os.getenv(
        'OPENAI_ENDPOINT', 'https://hdx-azurellm-classification.services.ai.azure.com/openai/v1'
    )
    OPENAI_API_KEY: str = os.getenv('OPENAI_API_KEY') or os.getenv('AZURE_OPENAI_API_KEY', '')

    # Slack
    HDX_SDD_SLACK_CHANNEL: str = os.getenv('HDX_SDD_SLACK_CHANNEL', 'topic-sensitive-data-alerts')
    HDX_SDD_SLACK_ACCESS_TOKEN: str = os.getenv('HDX_SDD_SLACK_ACCESS_TOKEN')


def get_config() -> Config:
    """Return config object for current environment."""
    return Config()


class SlackClientWrapper:
    def __init__(self) -> None:
        config = get_config()
        self.slack_channel = config.HDX_SDD_SLACK_CHANNEL
        self.message_prefix = self._derive_message_prefix(getattr(config, 'HDX_URL', ''))
        self.slack_client = None
        token = config.HDX_SDD_SLACK_ACCESS_TOKEN
        if token:
            self.slack_client = slack_sdk.WebClient(token=token)
            logger.debug('Slack client initialized')

    @staticmethod
    def _derive_message_prefix(hdx_url: str) -> str:
        """Derive Slack prefix from the first host label in HDX_URL."""
        if not hdx_url:
            return '[SDD Pipeline]'

        hostname = urlparse(hdx_url).hostname or hdx_url
        first_word = hostname.split('.')[0].strip() if hostname else ''
        if not first_word:
            return '[SDD Pipeline]'

        return f'[{first_word}]'

    def post_to_slack_channel(self, message: str):
        if self.slack_client:
            try:
                text = f'{self.message_prefix} {message}'
                self.slack_client.chat_postMessage(channel=self.slack_channel, text=text)
            except slack_errors.SlackApiError as e:
                # Log Slack API errors but don't raise to prevent blocking the pipeline
                logger.error(f'Got an error: {e.response["error"]}')
        else:
            logger.info(f'[instead of slack] {message}')
