"""config/config.py: Centralized configuration for the HDX SDD pipeline."""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

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
    DOWNLOAD_DIR: str = os.getenv('DOWNLOAD_DIR', '/tmp/download')
    OUTPUT_DIR: str = os.getenv('OUTPUT_DIR', '/tmp/reports')


def get_config() -> Config:
    """Return config object for current environment."""
    return Config()
