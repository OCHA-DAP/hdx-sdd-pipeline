"""utils/ckan.py: CKAN API client and utilities."""

import logging
from pathlib import Path
from typing import Optional, Dict, Any
import requests

from utils.exception_handler import handle_exception_wrap

logger = logging.getLogger(__name__)


class CKANClient:
    """
    A client for interacting with the CKAN API.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_token: Optional[str] = None,
    ):
        # --- Configuration ---
        self.base_url = base_url
        self.api_token = api_token
        self.project_root = Path(__file__).resolve().parent.parent
        self.headers = {'Authorization': self.api_token} if self.api_token else {}

    # --- Core request helper ---
    @handle_exception_wrap()
    def _request(self, action: str, method: str = 'GET', **kwargs) -> Optional[dict]:
        """
        Internal helper for making CKAN API requests.
        """
        url = f'{self.base_url}/api/3/action/{action}'

        if method.upper() == 'GET':
            response = requests.get(url, timeout=30, headers=self.headers, **kwargs)
        else:
            response = requests.post(url, timeout=30, headers=self.headers, **kwargs)
        response.raise_for_status()

        # Print request 200 or error
        if response.status_code == 200:
            logger.info('CKAN request successful')
        else:
            logger.error('CKAN request failed: %s', response.status_code)
        data = response.json()

        if data.get('success') is not True:
            logger.error('CKAN API returned error: %s', data.get('error'))
            return None

        return data['result']

    # --- API Methods ---
    @handle_exception_wrap()
    def package_show(self, package_id: str) -> Optional[dict]:
        """Fetch details about a dataset (package)."""
        logger.info('Fetching package: %s', package_id)
        return self._request('package_show', params={'id': package_id})

    @handle_exception_wrap()
    def resource_show(self, resource_id: str) -> Optional[dict]:
        """Fetch details about a resource."""
        logger.info('Fetching resource: %s', resource_id)
        return self._request('resource_show', params={'id': resource_id})

    @handle_exception_wrap()
    def update_resource_fields(self, resource_id: str, fields: Dict[str, Any]) -> Optional[dict]:
        """Update one or more fields of a CKAN resource."""
        if not self.api_token:
            raise EnvironmentError('CKAN_API_TOKEN is required to update resources')

        payload = {'id': resource_id, **fields}
        logger.info('Updating resource %s with fields: %s', resource_id, list(fields.keys()))

        updated_resource = self._request('resource_patch', method='POST', json=payload)
        if updated_resource:
            logger.info('Resource %s updated successfully', resource_id)
            return updated_resource
        else:
            logger.error('Failed to update resource %s', resource_id)
            return None

    @handle_exception_wrap()
    def remove_resource_field(self, resource_id: str, field_name: str) -> Optional[dict]:
        """
        Remove (set to None) a specific field in a CKAN resource.
        """
        if not self.api_token:
            raise EnvironmentError('CKAN_API_TOKEN is required to modify resources')

        payload = {'id': resource_id, field_name: None}
        logger.info(f'Removing field {field_name} from resource {resource_id}')
        return self._request('resource_patch', method='POST', json=payload)

    @handle_exception_wrap()
    def _get_download_link(self, resource_id: str) -> Optional[str]:
        """Get the download link for a resource."""
        resource = self.resource_show(resource_id)
        if resource and resource.get('download_url'):
            return resource['download_url']
        logger.info('No download URL found for resource: %s', resource_id)
        return None

    # --- File operations ---
    @handle_exception_wrap()
    def _download_file(self, url: str, filename: str, output_dir: Path) -> Path:
        """Download a file from a URL and save it locally."""
        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / filename
        logger.info('Downloading file: %s', url)

        response = requests.get(url, timeout=30, headers=self.headers)
        response.raise_for_status()
        file_path.write_bytes(response.content)

        logger.info('File saved to: %s', file_path)
        return file_path

    @handle_exception_wrap()
    def download_resource(
        self, resource_id: str, filename: Optional[str] = None, output_dir: Optional[Path] = None
    ) -> Path:
        """Download a CKAN resource by its ID."""
        output_dir = output_dir or (self.project_root / 'resources')
        url = self._get_download_link(resource_id)
        if not url:
            raise ValueError('No download URL found for resource: %s', resource_id)
        filename = filename or Path(url).name
        return self._download_file(url, filename, output_dir)
