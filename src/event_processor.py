"""  
Event processor for HDX resource events.

This is the main entry point for processing events from Redis streams.
It uses the clean architecture use cases to process datasets.
"""

import json
import logging
from typing import Dict, Any, Tuple
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

from src.infrastructure.factories import PipelineFactory
from src.domain.entities import SheetReport

# Legacy imports for CKAN and Redis (to be refactored later)
from src.shared.utils.ckan import CKANClient

from config import get_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class EventProcessor:
    """
    Processes HDX resource events from Redis streams.


    This class bridges the event-driven architecture with our
    clean architecture use cases.
    """

    def __init__(self):
        """Initialize event processor with all dependencies."""
        logger.info('Initializing Event Processor...')

        # Load configuration
        self.config = get_config()

        # Create pipeline using factory
        factory = PipelineFactory(self.config)
        self.pipeline = factory.create_pipeline(sample_size=5)

        # Setup CKAN client if CKAN_UPDATE is enabled
        if self.config.CKAN_UPDATE:
            self.ckan = CKANClient(
                base_url=self.config.HDX_URL,
                api_token=self.config.HDX_KEY
            )
            logger.info('CKAN client initialized')
        else:
            logger.info('CKAN_UPDATE is disabled - CKAN operations will be skipped')
            self.ckan = None

        logger.info('Event Processor initialized')

    def process_event(self, event: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Process a single HDX resource event.

        Args:
            event: Event data from Redis stream

        Returns:
            Tuple of (success, message)
        """
        logger.info(f'Processing event: {json.dumps(event, indent=2)}')

        # Extract resource ID
        resource_id = event.get('resource_id')
        if not resource_id:
            logger.error('Missing resource_id in event')
            return False, 'Missing resource_id'

        try:
            # Check if already processed (skip if CKAN disabled)
            if self.ckan and self._report_exists(resource_id):
                logger.info(f'Report already exists for {resource_id}')
                return True, 'Already processed'

            # Get resource info from CKAN
            if self.ckan:
                resource = self.ckan.resource_show(resource_id)
                download_url = resource.get('download_url')
            else:
                # When CKAN is disabled, expect download_url in event
                download_url = event.get('download_url')

            if not download_url:
                logger.error(f'No download URL for resource {resource_id}')
                return False, 'No download URL'

            # Get dataset location for ISP rules
            package_id = event.get('package_id')
            isp_rules = self._get_isp_rules(package_id)

            # Process dataset using our use case
            logger.info(f'Processing dataset from: {download_url}')
            reports = self.pipeline.execute(
                source=download_url, resource_id=resource_id, is_url=True, isp_rules=isp_rules
            )

            # Determine overall sensitivity
            sensitivity = self._determine_sensitivity(reports)

            # Save to CKAN
            self._save_to_ckan(resource_id, reports, sensitivity)

            logger.info(f'Successfully processed {resource_id}: {sensitivity}')
            return True, f'Processed successfully ({sensitivity})'

        except Exception as e:
            logger.error(f'Failed to process event: {e}', exc_info=True)
            return False, f'Processing failed: {str(e)}'

    def _report_exists(self, resource_id: str) -> bool:
        """Check if report already exists in CKAN."""
        if self.ckan is None:
            return False
        
        try:
            resource = self.ckan.resource_show(resource_id)
            return 'sdd_report' in resource and resource['sdd_report']
        except Exception:
            return False

    def _get_isp_rules(self, package_id: str) -> Dict[str, Any]:
        """Get ISP rules based on dataset location."""
        if not package_id:
            logger.debug('No package_id provided - using default ISP')
            try:
                with open('data/isps.json', 'r', encoding='utf-8') as f:
                    isps = json.load(f)
                default_isp = isps.get('default', {})
                logger.info('Using ISP: default')
                return default_isp
            except Exception as e:
                logger.error(f'Failed to load default ISP rules: {e}')
                return {}

        # If CKAN is disabled, return default ISP
        if self.ckan is None:
            logger.debug('CKAN disabled - using default ISP rules')
            try:
                with open('data/isps.json', 'r', encoding='utf-8') as f:
                    isps = json.load(f)
                default_isp = isps.get('default', {})
                logger.info('Using ISP: default')
                return default_isp
            except Exception as e:
                logger.error(f'Failed to load default ISP rules: {e}')
                return {}

        try:
            # Load ISP rules
            with open('data/isps.json', 'r', encoding='utf-8') as f:
                isps = json.load(f)

            # Get package info
            package = self.ckan.package_show(package_id)
            solr_additions = package.get('solr_additions', {})

            if isinstance(solr_additions, str):
                solr_additions = json.loads(solr_additions)

            countries = solr_additions.get('countries', [])

            if not countries:
                default_isp = isps.get('default', {})
                logger.info('No countries found - using ISP: default')
                return default_isp

            # Find matching ISP
            if isinstance(countries, str):
                countries = [countries]

            for country in countries:
                for isp_name, isp_data in isps.items():
                    country_filter = isp_data.get('country', '')
                    if country_filter and country_filter.lower() in country.lower():
                        logger.info(f'Using ISP: {isp_name} (matched country: {country})')
                        return isp_data

            default_isp = isps.get('default', {})
            logger.info('No matching ISP found - using ISP: default')
            return default_isp

        except Exception as e:
            logger.error(f'Failed to get ISP rules: {e}')
            return {}

    def _determine_sensitivity(self, reports: list) -> str:
        """Determine overall sensitivity from reports."""
        for report in reports:
            if isinstance(report, SheetReport) and report.is_sensitive():
                return 'sensitive'
        return 'non-sensitive'

    def _save_to_ckan(self, resource_id: str, reports: list, sensitivity: str):
        """Save results to CKAN or local file."""
        # Convert reports to dict
        reports_dict = [report.to_dict() if isinstance(report, SheetReport) else report for report in reports]

        # Check if CKAN updates are enabled
        if self.ckan is None:
            logger.warning('CKAN_UPDATE is disabled - saving to dev.json instead')
            self._save_to_local_file(resource_id, reports_dict, sensitivity)
            return

        # Update CKAN resource
        self.ckan.update_resource_fields(
            resource_id, {'sdd_report': json.dumps(reports_dict, indent=2), 'sensitive': sensitivity}
        )

        logger.info(f'Saved report to CKAN for resource {resource_id}')

    def _save_to_local_file(self, resource_id: str, reports_dict: list, sensitivity: str):
        """Save report to local dev.json file for testing."""
        
        # Create output directory if it doesn't exist
        output_dir = Path('dev_reports')
        output_dir.mkdir(exist_ok=True)

        # Create report structure
        report_data = {
            'resource_id': resource_id,
            'sensitive': sensitivity,
            'timestamp': datetime.now().isoformat(),
            'sdd_report': reports_dict
        }

        # Save to dev.json
        output_file = output_dir / 'dev.json'

        # Save updated data
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, default=str)

        logger.info(f'Saved report to {output_file} (sensitivity={sensitivity}, {len(reports_dict)} sheets)')


def main():
    """
    Main entry point for event processing.

    This can be run standalone or integrated with Redis event bus.
    """
    processor = EventProcessor()

    # Example: Process a test event
    test_event = {
        'resource_id': 'e7624ce3-a90a-4c94-958b-07468f513ee8',
        'package_id': 'test-package-123',
        'download_url': 'https://dev.data-humdata-org.ahconu.org/dataset/b9ba0513-f923-437e-9810-aa2ee02e91ad/resource/e7624ce3-a90a-4c94-958b-07468f513ee8/download/pii_dummy_dataset.csv',
        'event_type': 'resource-data-changed',
    }

    success, message = processor.process_event(test_event)

    if success:
        logger.info(f'✅ {message}')
    else:
        logger.error(f'❌ {message}')


if __name__ == '__main__':  # pragma: no cover
    main()
    
    # Test the dev.json file
    with open('dev_reports/dev.json', 'r', encoding='utf-8') as f:
        dev_data = json.load(f)
        
    # Assert if sdd_report is list
    assert isinstance(dev_data['sdd_report'], list)

    for sheet_report in dev_data['sdd_report']:
        # Check required fields
        assert 'resource_id' in sheet_report
        assert 'file_name' in sheet_report
        assert 'file_url' in sheet_report
        assert 'sheet_name' in sheet_report
        assert 'processing_timestamp' in sheet_report
        assert 'processing_success' in sheet_report
        assert 'n_records' in sheet_report
        assert 'n_columns' in sheet_report
        assert 'columns' in sheet_report
        assert 'completion_tokens' in sheet_report
        assert 'prompt_tokens' in sheet_report
        assert 'personal_data_sensitive' in sheet_report
        assert 'non_personal_data_sensitive' in sheet_report
        
        # Model fields are optional (None when steps are disabled)
        # Just verify they exist in the report, even if None
        assert 'non_pii_model' in sheet_report or 'non_personal_data' in sheet_report
        
        logger.info('✅ All required fields present in dev.json')