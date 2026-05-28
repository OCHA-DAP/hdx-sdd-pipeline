"""
ISP retrieval and matching utilities.

This module handles loading ISP rules from JSON and matching them
based on ISO3 codes from package metadata and resource names.
"""

import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class ISPRetriever:
    """
    Handles retrieval and matching of ISP rules based on ISO3 codes.

    This class loads ISP rules from a JSON file and provides methods to match
    the appropriate ISP based on ISO3 codes found in package metadata
    or resource filenames.
    """

    def __init__(self, isp_file_path: str = 'data/isps.json'):
        """
        Initialize ISP retriever.

        Args:
            isp_file_path: Path to the ISP rules JSON file
        """
        self.isp_file_path = isp_file_path
        self._isps_cache = None

    def get_isp_rules(
        self, package_id: Optional[str], resource_name: Optional[str] = None, ckan_client=None
    ) -> Dict[str, Any]:
        """
        Get ISP rules based on package groups or resource name.

        Priority order:
        1. Match ISO3 from package groups (CKAN)
        2. Match ISO3 from resource name (filename stem)
        3. Fallback to default ISP

        Args:
            package_id: CKAN package ID
            resource_name: Resource filename
            ckan_client: CKAN client instance for fetching package data

        Returns:
            ISP rules dictionary
        """
        isps = self._load_isp_rules()
        if not isps:
            return {}

        default_isp = isps.get('default', {})

        # Try matching from package groups first
        matched_isp = self._match_from_package_groups(package_id, isps, ckan_client)
        if matched_isp:
            return matched_isp

        # Try matching from resource name (expected ISO3 filename stem)
        matched_isp = self._match_from_resource_name(resource_name, isps)
        if matched_isp:
            return matched_isp

        # Fallback to default
        logger.info('No specific ISP found - using ISP: default')
        return default_isp

    def match_country(self, iso3_code: Optional[str], isps: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Match ISO3 code against ISP country filters.

        Args:
            iso3_code: ISO3 code to match
            isps: Dictionary of ISP rules

        Returns:
            Matching ISP data or None
        """
        if not iso3_code:
            return None

        normalized_iso3 = iso3_code.strip().lower()
        if not normalized_iso3:
            return None

        for isp_name, isp_data in isps.items():
            country_filter = isp_data.get('country', '')
            if isinstance(country_filter, str) and country_filter.strip().lower() == normalized_iso3:
                logger.info(f'Using ISP: {isp_name} (matched ISO3: {normalized_iso3})')
                return isp_data

        return None

    def _load_isp_rules(self) -> Dict[str, Any]:
        """Load ISP rules from JSON file with caching."""
        if self._isps_cache is not None:
            return self._isps_cache

        try:
            with open(self.isp_file_path, 'r', encoding='utf-8') as f:
                self._isps_cache = json.load(f)
                return self._isps_cache
        except Exception as e:
            logger.error(f'Failed to load ISP rules file: {e}')
            return {}

    def _match_from_package_groups(
        self, package_id: Optional[str], isps: Dict[str, Any], ckan_client
    ) -> Optional[Dict[str, Any]]:
        """Try to match ISP from CKAN package groups."""
        if not package_id or not ckan_client:
            return None

        try:
            package = ckan_client.package_show(package_id)
            groups = package.get('groups', [])

            if not groups:
                return None

            for group in groups:
                if not isinstance(group, dict):
                    continue

                group_name = group.get('name')
                if not isinstance(group_name, str):
                    continue

                matched_isp = self.match_country(group_name, isps)
                if matched_isp:
                    return matched_isp

        except Exception as e:
            logger.warning('Failed to get groups from CKAN: %s', e)

        return None

    def _match_from_resource_name(self, resource_name: Optional[str], isps: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Try to match ISP from resource name by searching for ISO3 in the whole string."""
        if not resource_name:
            return None

        # Look for any country ISO3 in the whole string (case-insensitive)
        resource_name_lower = resource_name.lower()

        for isp_name, isp_data in isps.items():
            if isp_name == 'default':
                continue

            country_iso3 = isp_data.get('country', '')
            if not isinstance(country_iso3, str):
                continue

            normalized_iso3 = country_iso3.strip().lower()
            if normalized_iso3 and normalized_iso3 in resource_name_lower:
                logger.info(
                    f'Using ISP: {isp_name} (matched ISO3: {normalized_iso3} in resource name: {resource_name})'
                )
                return isp_data

        return None
