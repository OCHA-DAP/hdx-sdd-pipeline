"""utils/ckan.py: CKAN API client and utilities."""

import logging
import logging.config
import os
from pathlib import Path
from typing import Optional, Dict, Any
import requests


class CKANClient:
    """
    A client for interacting with the CKAN API.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_token: Optional[str] = None,
        logging_conf: str = 'logging.conf',
        logger: Optional[logging.Logger] = None,
    ):
        # --- Configuration ---
        self.base_url = base_url or os.getenv('CKAN_URL')
        self.api_token = api_token or os.getenv('CKAN_API_TOKEN')
        self.project_root = Path(__file__).resolve().parent.parent
        self.headers = {'Authorization': self.api_token} if self.api_token else {}

        # --- Logging setup ---
        if logger is None:
            if Path(logging_conf).exists():
                logging.config.fileConfig(logging_conf)
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

        self.logger.debug('Initialized CKANClient with base_url=%s', self.base_url)

    # --- Core request helper ---
    def _request(self, action: str, method: str = 'GET', **kwargs) -> Optional[dict]:
        """
        Internal helper for making CKAN API requests.
        """
        url = f'{self.base_url}/api/3/action/{action}'
        self.logger.debug('CKAN request: %s %s', method, url)

        try:
            if method.upper() == 'GET':
                response = requests.get(url, timeout=30, headers=self.headers, **kwargs)
            else:
                response = requests.post(url, timeout=30, headers=self.headers, **kwargs)

            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError) as e:
            self.logger.error('CKAN request failed: %s', e)
            return None

        if data.get('success'):
            return data['result']

        self.logger.error('CKAN API returned error: %s', data.get('error'))
        return None

    # --- API Methods ---
    def package_show(self, package_id: str) -> Optional[dict]:
        """Fetch details about a dataset (package)."""
        if not isinstance(package_id, str):
            raise ValueError('package_id must be a string')
        self.logger.info('Fetching package: %s', package_id)
        return self._request('package_show', params={'id': package_id})

    def resource_show(self, resource_id: str) -> Optional[dict]:
        """Fetch details about a resource."""
        if not isinstance(resource_id, str):
            raise ValueError('resource_id must be a string')
        self.logger.info('Fetching resource: %s', resource_id)
        return self._request('resource_show', params={'id': resource_id})

    def update_resource_fields(self, resource_id: str, fields: Dict[str, Any]) -> Optional[dict]:
        """Update one or more fields of a CKAN resource."""
        if not isinstance(fields, dict):
            raise ValueError('fields must be a dictionary')
        if not isinstance(resource_id, str):
            raise ValueError('resource_id must be a string')
        if not self.api_token:
            raise EnvironmentError('CKAN_API_TOKEN is required to update resources')

        payload = {'id': resource_id, **fields}
        self.logger.info('Updating resource %s with fields: %s', resource_id, list(fields.keys()))
        return self._request('resource_patch', method='POST', json=payload)

    def remove_resource_field(self, resource_id: str, field_name: str) -> Optional[dict]:
        """
        Remove (set to None) a specific field in a CKAN resource.
        """
        if not isinstance(resource_id, str):
            raise ValueError('resource_id must be a string')
        if not isinstance(field_name, str):
            raise ValueError('field_name must be a string')
        if not self.api_token:
            raise EnvironmentError('CKAN_API_TOKEN is required to modify resources')

        payload = {'id': resource_id, field_name: None}
        self.logger.info(f'Removing field {field_name} from resource {resource_id}')
        return self._request('resource_patch', method='POST', json=payload)

    def _get_download_link(self, resource_id: str) -> Optional[str]:
        """Get the download link for a resource."""
        resource = self.resource_show(resource_id)
        if resource and resource.get('download_url'):
            return resource['download_url']
        self.logger.error('No download URL found for resource: %s', resource_id)
        return None

    # --- File operations ---
    def _download_file(self, url: str, filename: str, output_dir: Path) -> Path:
        """Download a file from a URL and save it locally."""
        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / filename
        self.logger.info('Downloading file: %s', url)

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            file_path.write_bytes(response.content)
        except requests.RequestException as e:
            self.logger.error('Failed to download file: %s', e)
            raise

        self.logger.info('File saved to: %s', file_path)
        return file_path

    def download_resource(
        self, resource_id: str, filename: Optional[str] = None, output_dir: Optional[Path] = None
    ) -> Path:
        """Download a CKAN resource by its ID."""
        output_dir = output_dir or (self.project_root / 'resources')
        url = self._get_download_link(resource_id)
        if not url:
            raise ValueError(f'No download URL found for resource {resource_id}')

        filename = filename or Path(url).name
        return self._download_file(url, filename, output_dir)
