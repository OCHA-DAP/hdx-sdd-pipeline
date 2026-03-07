"""
ISP retrieval and matching utilities.

This module handles loading ISP rules from JSON and matching them
based on country names from package metadata and resource names.
"""

import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class ISPRetriever:
    """
    Handles retrieval and matching of ISP rules based on country information.

    This class loads ISP rules from a JSON file and provides methods to match
    the appropriate ISP based on country names found in package metadata
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
        self._country_mapping_cache = None

    def get_isp_rules(
        self, package_id: Optional[str], resource_name: Optional[str] = None, ckan_client=None
    ) -> Dict[str, Any]:
        """
        Get ISP rules based on dataset location or resource name.

        Priority order:
        1. Match country from package location (CKAN)
        2. Match country from resource name
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
        country_mapping = self._build_country_mapping(isps)

        # Try matching from package location first
        matched_isp = self._match_from_package_location(package_id, isps, country_mapping, ckan_client)
        if matched_isp:
            return matched_isp

        # Try matching from resource name
        matched_isp = self._match_from_resource_name(resource_name, isps, country_mapping)
        if matched_isp:
            return matched_isp

        # Fallback to default
        logger.info('No specific ISP found - using ISP: default')
        return default_isp

    def match_country(
        self, text: Optional[str], isps: Dict[str, Any], country_mapping: Dict[str, str]
    ) -> Optional[Dict[str, Any]]:
        """
        Match country in text using ISP country filters and partial mapping.

        Args:
            text: Text to search for country names
            isps: Dictionary of ISP rules
            country_mapping: Partial country name mappings

        Returns:
            Matching ISP data or None
        """
        if not text:
            return None

        text_lower = text.lower()

        # First try direct ISP country filter matching
        for isp_name, isp_data in isps.items():
            country_filter = isp_data.get('country', '')
            if country_filter and country_filter.lower() in text_lower:
                logger.info(f'Using ISP: {isp_name} (matched: {country_filter} in {text})')
                return isp_data

        # Then try partial mapping
        for partial, full_country in country_mapping.items():
            if partial in text_lower:
                # Find the ISP that matches this full country
                for isp_name, isp_data in isps.items():
                    if isp_data.get('country', '').lower() == full_country.lower():
                        logger.info(f'Using ISP: {isp_name} (matched partial: {partial} -> {full_country} in {text})')
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

    def _build_country_mapping(self, isps: Dict[str, Any]) -> Dict[str, str]:
        """Build partial country name mapping for robust matching with caching."""
        if self._country_mapping_cache is not None:
            return self._country_mapping_cache

        country_mapping = {}
        for isp_data in isps.values():
            country_filter = isp_data.get('country', '')
            if country_filter and country_filter != 'default':
                # Create partial mappings (first 3-4 chars)
                if len(country_filter) >= 3:
                    country_mapping[country_filter[:3].lower()] = country_filter
                if len(country_filter) >= 4:
                    country_mapping[country_filter[:4].lower()] = country_filter

        self._country_mapping_cache = country_mapping
        return country_mapping

    def _match_from_package_location(
        self, package_id: Optional[str], isps: Dict[str, Any], country_mapping: Dict[str, str], ckan_client
    ) -> Optional[Dict[str, Any]]:
        """Try to match ISP from package location data."""
        if not package_id or not ckan_client:
            return None

        try:
            package = ckan_client.package_show(package_id)
            solr_additions = package.get('solr_additions', {})

            if isinstance(solr_additions, str):
                solr_additions = json.loads(solr_additions)

            countries = solr_additions.get('countries', [])
            if not countries:
                return None

            if isinstance(countries, str):
                countries = [countries]

            for country in countries:
                matched_isp = self.match_country(country, isps, country_mapping)
                if matched_isp:
                    return matched_isp

        except Exception as e:
            logger.warning('Failed to get location from CKAN: %s', e)

        return None

    def _match_from_resource_name(
        self, resource_name: Optional[str], isps: Dict[str, Any], country_mapping: Dict[str, str]
    ) -> Optional[Dict[str, Any]]:
        """Try to match ISP from resource name."""
        if not resource_name:
            return None

        return self.match_country(resource_name, isps, country_mapping)

    def clear_cache(self):
        """Clear cached ISP data and country mappings."""
        self._isps_cache = None
        self._country_mapping_cache = None
