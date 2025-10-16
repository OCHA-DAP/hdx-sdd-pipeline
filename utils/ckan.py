"""utils/ckan.py: Utility functions for CKAN API."""

import logging
import logging.config
import os
from pathlib import Path
from typing import Optional, Dict, Any
import requests

# --- Logging setup ---
logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)

# --- Configuration ---
CKAN_URL = os.getenv('CKAN_URL', 'https://data.humdata.org')
CKAN_API_TOKEN = os.getenv('CKAN_API_TOKEN')
PROJECT_ROOT = Path(__file__).resolve().parent.parent


# --- Core CKAN request helper ---
def _ckan_request(action: str, method: str = 'GET', **kwargs) -> Optional[dict]:
    """
    Internal helper for CKAN API requests.
    """
    headers = {'Authorization': CKAN_API_TOKEN}
    url = f'{CKAN_URL}/api/3/action/{action}'
    try:
        if method.upper() == 'GET':
            response = requests.get(url, timeout=30, headers=headers, **kwargs)
        else:
            response = requests.post(url, timeout=30, headers=headers, **kwargs)

        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError) as e:
        logger.error('CKAN request failed: %s', e)
        return None

    if data.get('success'):
        return data['result']
    logger.error('CKAN API returned error: %s', data.get('error'))
    return None


# --- API Wrappers ---
def package_show(package_id: str) -> Optional[dict]:
    """Fetch details about a dataset (package)."""
    if not isinstance(package_id, str):
        raise ValueError('package_id must be a string')
    logger.info('Fetching package: %s', package_id)
    return _ckan_request('package_show', params={'id': package_id})


def resource_show(resource_id: str) -> Optional[dict]:
    """Fetch details about a resource."""
    if not isinstance(resource_id, str):
        raise ValueError('resource_id must be a string')
    logger.info('Fetching resource: %s', resource_id)
    return _ckan_request('resource_show', params={'id': resource_id})


def update_resource_fields(resource_id: str, fields: Dict[str, Any]) -> Optional[dict]:
    """Update one or more fields of a CKAN resource."""
    if not isinstance(fields, dict):
        raise ValueError('fields must be a dictionary')
    if not isinstance(resource_id, str):
        raise ValueError('resource_id must be a string')
    if not CKAN_API_TOKEN:
        raise EnvironmentError('CKAN_API_TOKEN is required to update resources')

    payload = {'id': resource_id, **fields}

    logger.info('Updating resource %s with fields: %s', resource_id, list(fields.keys()))
    return _ckan_request('resource_patch', method='POST', json=payload)


def get_download_link(resource_id: str) -> Optional[str]:
    """Get the download link for a resource."""
    resource = resource_show(resource_id)
    if resource and resource.get('download_url'):
        return resource['download_url']
    logger.error('No download URL found for resource: %s', resource_id)
    return None


def download_file(url: str, filename: str, output_dir: Path) -> Path:
    """Download a file from a URL and save it locally."""
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / filename

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        file_path.write_bytes(response.content)
    except requests.RequestException as e:
        logger.error('Failed to download file: %s', e)
        raise

    return file_path


def download_resource(resource_id: str, filename: Optional[str] = None, output_dir: Optional[Path] = None) -> Path:
    """Download a CKAN resource by its ID."""
    output_dir = output_dir or (PROJECT_ROOT / 'resources')
    url = get_download_link(resource_id)
    if not url:
        raise ValueError(f'No download URL found for resource {resource_id}')

    filename = filename or Path(url).name
    return download_file(url, filename, output_dir)
