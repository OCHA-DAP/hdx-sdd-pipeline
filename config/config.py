"""config/config.py: Centralized configuration for the HDX SDD pipeline."""

import os
from dataclasses import dataclass
import logging
import slack_sdk
import slack_sdk.errors as slack_errors
from dotenv import load_dotenv

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

    # Processing / Models
    RERUN: bool = os.getenv('RERUN', 'false').lower() == 'true'
    PII_DETECT_MODEL: str = os.getenv('PII_DETECT_MODEL', 'pii-detect-v1')
    PII_REFLECT_MODEL: str = os.getenv('PII_REFLECT_MODEL', 'pii-reflect-v1')
    NON_PII_DETECT_MODEL: str = os.getenv('NON_PII_DETECT_MODEL', 'non-pii-detect-v1')
    README_SCAN_MODEL: str = os.getenv('README_SCAN_MODEL', 'readme-scan-v1')

    # Directories
    # DOWNLOAD_DIR: str = os.getenv('DOWNLOAD_DIR', '/tmp/download')
    # OUTPUT_DIR: str = os.getenv('OUTPUT_DIR', '/tmp/reports')

    # Azure endpoints
    AZURE_OPENAI_ENDPOINT: str = os.getenv('AZURE_OPENAI_ENDPOINT', '')
    AZURE_OPENAI_API_KEY: str = os.getenv('AZURE_OPENAI_API_KEY', '')

    # Slack
    HDX_SDD_SLACK_CHANNEL: str = os.getenv('HDX_SDD_SLACK_CHANNEL', 'topic-sensitive-data-alerts')
    HDX_SDD_SLACK_ACCESS_TOKEN: str = os.getenv('HDX_SDD_SLACK_ACCESS_TOKEN')


def get_config() -> Config:
    """Return config object for current environment."""
    return Config()


class SlackClientWrapper():
    def __init__(self) -> None:
        config = get_config()
        self.slack_channel = config.HDX_SDD_SLACK_CHANNEL
        self.slack_client = None
        token = config.HDX_SDD_SLACK_ACCESS_TOKEN
        if token:
            self.slack_client = slack_sdk.WebClient(token=token)
            logger.debug('Slack client initialized')

    def post_to_slack_channel(self, message: str):
        if self.slack_client:
            try:
                text = f'[SDD Pipeline] {message}'
                self.slack_client.chat_postMessage(channel=self.slack_channel, text=text)
            except slack_errors.SlackApiError as e:
                # You will get a SlackApiError if "ok" is False
                logger.error(f"Got an error: {e.response['error']}")
        else:
            logger.info(f'[instead of slack] {message}')
