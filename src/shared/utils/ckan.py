"""utils/ckan.py: CKAN API client and utilities."""

import logging
from typing import Optional, Dict, Any
import requests

from src.shared.utils.exception_handler import handle_exception

logger = logging.getLogger(__name__)


class CKANClient:
    """
    A client for interacting with the CKAN API.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_token: Optional[str] = None,
        user_agent: Optional[str] = None,
    ):
        # --- Configuration ---
        self.base_url = base_url
        self.api_token = api_token
        self.user_agent = user_agent
        self.headers = {'Authorization': self.api_token} if self.api_token else {}
        if self.user_agent:
            self.headers['User-Agent'] = self.user_agent

    # --- Core request helper ---
    @handle_exception()
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
    @handle_exception()
    def package_show(self, package_id: str) -> Optional[dict]:
        """Fetch details about a dataset (package)."""
        logger.info('Fetching package: %s', package_id)
        return self._request('package_show', params={'id': package_id})

    @handle_exception()
    def resource_show(self, resource_id: str) -> Optional[dict]:
        """Fetch details about a resource."""
        logger.info('Fetching resource: %s', resource_id)
        return self._request('resource_show', params={'id': resource_id})

    @handle_exception()
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
