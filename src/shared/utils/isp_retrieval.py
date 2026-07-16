"""
ISP retrieval and matching utilities.

This module handles loading ISP rules from JSON and matching them
based on ISO3 codes from package metadata and resource names.
"""

import json
import logging
import re
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Map of country ISO3 codes to lowercase country names for countries with custom ISPs
CUSTOM_ISP_COUNTRIES = {
    'afg': 'afghanistan',
    'bdi': 'burundi',
    'cmr': 'cameroon',
    'cod': 'democratic republic of the congo',
    'irq': 'iraq',
    'moz': 'mozambique',
    'mmr': 'myanmar',
    'ner': 'niger',
    'pse': 'palestine',
    'som': 'somalia',
    'ssd': 'south sudan',
    'sdn': 'sudan',
    'syr': 'syria',
    'ukr': 'ukraine',
    'ven': 'venezuela',
    'yem': 'yemen',
}

ALT_NAMES = {
    'drc': 'cod',
    'democratic republic of congo': 'cod',
    'congo': 'cod',
    'occupied palestinian territory': 'pse',
    'opt': 'pse',
    'syrian arab republic': 'syr',
}


class ISPRetriever:
    """
    Handles retrieval and matching of ISP rules based on ISO3 codes.

    This class loads ISP rules from a JSON file and provides methods to match
    the appropriate ISP based on ISO3 codes found in package metadata.
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
        self,
        package_id: Optional[str],
        resource_name: Optional[str] = None,
        ckan_client=None,
        dataset_location: Optional[str] = None,
        dataset_title: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get ISP rules strictly based on package groups or dataset_location metadata.
        Filename and dataset title fallback matching are completely disabled.

        Args:
            package_id: CKAN package ID
            resource_name: Resource filename (unused, deprecated)
            ckan_client: CKAN client instance for fetching package data
            dataset_location: Comma-separated list of dataset locations
            dataset_title: Dataset title (unused, deprecated)

        Returns:
            ISP rules dictionary
        """
        isps = self._load_isp_rules()
        if not isps:
            return {}

        default_isp = isps.get('default', {})

        # Try package groups from CKAN first
        logger.info(f'package_id: {package_id}, ckan_client: {ckan_client}')
        if package_id and ckan_client:
            try:
                package = ckan_client.package_show(package_id)
                groups = package.get('groups', [])
                logger.info(f'Groups of package {package_id}: {groups}')
                if groups:
                    for group in groups:
                        group_name = None
                        if isinstance(group, dict):
                            group_name = group.get('name') or group.get('title')
                        elif isinstance(group, str):
                            group_name = group

                        if group_name:
                            iso3 = self._resolve_iso3(group_name, isps)
                            if iso3:
                                matched_isp = self.match_country(iso3, isps)
                                if matched_isp:
                                    return matched_isp

                    # Location is known from groups, but no specific ISP. Stop and use default.
                    logger.info('Location specified in package groups, but no specific ISP found. Using default ISP.')
                    return default_isp
            except Exception as e:
                logger.warning('Failed to get groups from CKAN: %s', e)

        # Try dataset_location metadata
        if dataset_location:
            iso3 = self._resolve_iso3(dataset_location, isps)
            if iso3:
                matched_isp = self.match_country(iso3, isps)
                if matched_isp:
                    return matched_isp
            # Location is known from metadata, but no specific ISP. Stop and use default.
            logger.info(f'Location specified as {dataset_location}, but no specific ISP found. Using default ISP.')
            return default_isp

        # Fallback to default
        logger.info('No location specified - using ISP: default')
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

    def _resolve_iso3(self, text: Optional[str], isps: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Resolve country ISO3 code from text string using word boundaries."""
        if not text:
            return None

        # Clean text: replace non-alphanumeric characters with spaces
        cleaned = re.sub(r'[^a-zA-Z0-9\s]', ' ', text).lower()
        cleaned_spaced = f' {cleaned} '

        # 1. Check for specific alternative names first
        alt_names_sorted = sorted(ALT_NAMES.keys(), key=len, reverse=True)
        for alt_name in alt_names_sorted:
            if f' {alt_name} ' in cleaned_spaced:
                return ALT_NAMES[alt_name]

        # 2. Check custom ISP country names
        for iso3, country_name in CUSTOM_ISP_COUNTRIES.items():
            if f' {country_name} ' in cleaned_spaced:
                return iso3

        # 3. Check custom ISP ISO3 codes
        for iso3 in CUSTOM_ISP_COUNTRIES.keys():
            if f' {iso3} ' in cleaned_spaced:
                return iso3

        # 4. Check if any other ISO3 matches a custom ISP country from loaded isps (e.g. mock test country)
        if isps:
            for isp_name, isp_data in isps.items():
                if isp_name == 'default':
                    continue
                country_iso3 = isp_data.get('country', '')
                if isinstance(country_iso3, str):
                    normalized_iso3 = country_iso3.strip().lower()
                    if normalized_iso3 and f' {normalized_iso3} ' in cleaned_spaced:
                        return normalized_iso3

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
                group_name = None
                if isinstance(group, dict):
                    group_name = group.get('name') or group.get('title')
                elif isinstance(group, str):
                    group_name = group

                if group_name:
                    matched_isp = self.match_country(group_name, isps)
                    if matched_isp:
                        return matched_isp

        except Exception as e:
            logger.warning('Failed to get groups from CKAN: %s', e)

        return None

    def _match_from_resource_name(self, resource_name: Optional[str], isps: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Deprecated: Matching from resource name is disabled."""
        return None
