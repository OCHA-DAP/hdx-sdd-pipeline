"""
Event processor for HDX resource events.

This is the main entry point for processing events from Redis streams.
It uses the clean architecture use cases to process datasets.
"""

import json
import logging
from typing import Dict, Any, Tuple, Optional
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

from src.infrastructure.pipeline_factory import PipelineFactory
from src.domain.entities import SheetReport
from src.shared.utils.isp_retrieval import ISPRetriever

# Legacy imports for CKAN and Redis (to be refactored later)
from src.shared.utils.ckan import CKANClient

from config import get_config
from config.config import SlackClientWrapper

logger = logging.getLogger(__name__)

load_dotenv()


class EventProcessor:
    """
    Processes HDX resource events from Redis streams.


    This class bridges the event-driven architecture with our
    clean architecture use cases.
    """

    def __init__(self, custom_output_path: Optional[str] = None, config=None):
        """Initialize event processor with all dependencies."""
        logger.info('Initializing Event Processor...')

        # Load configuration
        self.config = config if config else get_config()

        # Set custom output path if provided
        self.custom_output_path = Path(custom_output_path) if custom_output_path else None

        # Create pipeline using factory
        factory = PipelineFactory(self.config)
        self.pipeline = factory.create_pipeline(sample_size=5)

        # Initialize ISP retriever
        self.isp_retriever = ISPRetriever()
        self.slack = SlackClientWrapper()

        # Setup CKAN client if CKAN_UPDATE is enabled
        if self.config.CKAN_UPDATE and not self.custom_output_path:
            self.ckan = CKANClient(
                base_url=self.config.HDX_URL,
                api_token=self.config.HDX_KEY,
                user_agent=self.config.SDD_USER_AGENT,
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
            resource = None
            resource_name = None
            if self.ckan:
                resource = self.ckan.resource_show(resource_id)
                download_url = resource.get('download_url') if resource else None
                resource_name = resource.get('name', 'unknown_dataset.csv') if resource else 'unknown_dataset.csv'
            else:
                # When CKAN is disabled, expect download_url in event
                download_url = event.get('download_url')
                resource_name = event.get('file_name') or event.get(
                    'resource_name'
                )  # Attempt to get filename from event if available

            if not download_url:
                logger.error(f'No download URL for resource {resource_id}')
                return False, 'No download URL'

            # Extract metadata
            metadata = {
                'resource_name': event.get('file_name') or event.get('resource_name'),
                'resource_description': event.get('resource_description') or event.get('description'),
                'dataset_title': event.get('dataset_title') or event.get('package_title'),
                'dataset_description': event.get('dataset_description')
                or event.get('dataset_notes')
                or event.get('notes'),
                'dataset_source': event.get('dataset_source'),
                'dataset_location': event.get('dataset_location') or event.get('location'),
                'organization_title': event.get('organization_title') or event.get('org_title'),
            }

            if resource:
                if resource.get('name'):
                    metadata['resource_name'] = resource.get('name')
                if resource.get('description'):
                    metadata['resource_description'] = resource.get('description')

            # Get dataset location for ISP rules
            package_id = (
                event.get('dataset_id') or event.get('package_id') or (resource.get('package_id') if resource else None)
            )
            isp_rules = self.isp_retriever.get_isp_rules(package_id, resource_name, self.ckan)

            # Fetch package metadata if CKAN is enabled and package_id is available
            if self.ckan and package_id:
                try:
                    package = self.ckan.package_show(package_id)
                    if package:
                        if package.get('title'):
                            metadata['dataset_title'] = package.get('title')
                        if package.get('notes'):
                            metadata['dataset_description'] = package.get('notes')
                        elif package.get('description'):
                            metadata['dataset_description'] = package.get('description')

                        if package.get('dataset_source'):
                            metadata['dataset_source'] = package.get('dataset_source')

                        groups = package.get('groups', [])
                        locations = [
                            g.get('title') or g.get('display_name') or g.get('name')
                            for g in groups
                            if isinstance(g, dict)
                        ]
                        loc_str = ', '.join([loc for loc in locations if loc])
                        if loc_str:
                            metadata['dataset_location'] = loc_str

                        org = package.get('organization')
                        if isinstance(org, dict):
                            org_title = org.get('title') or org.get('name')
                            if org_title:
                                metadata['organization_title'] = org_title
                except Exception as e:
                    logger.warning(f'Error fetching package metadata from CKAN for package {package_id}: {e}')

            # Check if there is a local metadata file in research/metadata
            meta_filename = resource_name or (Path(download_url).name if download_url else None)
            if meta_filename:
                basename = Path(meta_filename).name
                metadata_dir = Path(__file__).parent.parent / 'research' / 'metadata'
                local_metadata_path = metadata_dir / f'{basename}.json'
                if not local_metadata_path.exists():
                    local_metadata_path = (
                        Path('/Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/research/metadata')
                        / f'{basename}.json'
                    )
                if local_metadata_path.exists():
                    try:
                        with open(local_metadata_path, 'r', encoding='utf-8') as f:
                            local_meta = json.load(f)
                            logger.info(f'Loaded local metadata from {local_metadata_path}')
                            for key in [
                                'dataset_title',
                                'dataset_description',
                                'dataset_source',
                                'dataset_location',
                                'organization_title',
                                'resource_name',
                                'resource_description',
                            ]:
                                if local_meta.get(key) is not None:
                                    metadata[key] = local_meta[key]
                    except Exception as e:
                        logger.warning(f'Failed to read local metadata from {local_metadata_path}: {e}')

            # Process dataset using our use case
            logger.info(f'Processing dataset from: {download_url}')
            http_headers = self.ckan.headers if self.ckan else {}
            reports = self.pipeline.execute(
                source=download_url,
                resource_id=resource_id,
                is_url=download_url.startswith(('http://', 'https://')),
                isp_rules=isp_rules,
                http_headers=http_headers,
                metadata=metadata,
            )

            # Determine overall sensitivity
            sensitivity = self._determine_sensitivity(reports)
            sensitivity_level = self._determine_sensitivity_level(reports)

            # Save to CKAN
            self._save_to_ckan(resource_id, reports, sensitivity, sensitivity_level)

            logger.info(f'Successfully processed {resource_id}: {sensitivity}')
            return True, f'Processed successfully ({sensitivity})'

        except Exception as e:
            logger.error(f'Failed to process event: {e}', exc_info=True)
            self._notify_important_processing_error(event, e)
            return False, f'Processing failed: {str(e)}'

    def _notify_important_processing_error(self, event: Dict[str, Any], error: Exception) -> None:
        """Notify Slack for critical processing failures without alerting on expected non-critical outcomes."""
        resource_id = event.get('resource_id', 'unknown-resource')
        package_id = event.get('package_id', 'unknown-package')
        event_type = event.get('event_type', 'unknown-event')
        message = (
            f'Important processing error for resource={resource_id} '
            f'package={package_id} event_type={event_type}: {error}'
        )
        self.slack.post_to_slack_channel(message)

    def _report_exists(self, resource_id: str) -> bool:
        """Check if report already exists in CKAN."""
        if self.ckan is None:
            return False

        resource = self.ckan.resource_show(resource_id)
        return 'sdd_report' in resource and resource['sdd_report']

    def _determine_sensitivity(self, reports: list) -> str:
        """Determine overall sensitivity from reports."""
        has_personal_sensitive = False
        has_non_personal_sensitive = False

        for report in reports:
            if isinstance(report, SheetReport):
                if report.personal_data_sensitive:
                    has_personal_sensitive = True
                if report.non_personal_data_sensitive:
                    has_non_personal_sensitive = True

        if has_personal_sensitive and has_non_personal_sensitive:
            return 'sensitive-pd-and-non-pd'
        elif has_personal_sensitive:
            return 'sensitive-pd'
        elif has_non_personal_sensitive:
            return 'sensitive-non-pd'
        else:
            return 'not-sensitive'

    def _determine_sensitivity_level(self, reports: list) -> int:
        """Determine overall sensitive level from reports."""
        max_risk = 0
        for report in reports:
            if isinstance(report, SheetReport):
                sheet_risk = max(report.personal_data_risk_level, report.non_personal_data_risk_level)
                if sheet_risk > max_risk:
                    max_risk = sheet_risk
            elif isinstance(report, dict):
                pd_risk = report.get('personal_data_risk_level', 0)
                npd_risk = report.get('non_personal_data_risk_level', 0)
                sheet_risk = max(pd_risk, npd_risk)
                if sheet_risk > max_risk:
                    max_risk = sheet_risk
        return max_risk

    def _save_to_ckan(self, resource_id: str, reports: list, sensitivity: str, sensitivity_level: int = 0):
        """Save results to CKAN or local file."""
        # Convert reports to dict
        reports_dict = [report.to_dict() if isinstance(report, SheetReport) else report for report in reports]

        # Check if CKAN updates are enabled
        if self.ckan is None:
            logger.warning('CKAN_UPDATE is disabled - saving to dev.json instead')
            self._save_to_local_file(resource_id, reports_dict, sensitivity, sensitivity_level)
            return

        # Update CKAN resource
        self.ckan.update_resource_fields(
            resource_id,
            {
                'sdd_report': json.dumps(reports_dict, indent=2),
                'sensitive': sensitivity,
                'sensitivity_level': sensitivity_level,
            },
        )

        logger.info(f'Saved report to CKAN for resource {resource_id}')

    def _save_to_local_file(self, resource_id: str, reports_dict: list, sensitivity: str, sensitivity_level: int = 0):
        """Save report to local file with configurable output path."""

        # Determine output directory and filename
        if self.custom_output_path:
            # Use custom output path
            if self.custom_output_path.is_dir():
                # If it's a directory, save as resource_id.json
                output_file = self.custom_output_path / f'{resource_id}.json'
            else:
                # If it's a file path, use it directly
                output_file = self.custom_output_path
            output_file.parent.mkdir(parents=True, exist_ok=True)
        else:
            # Fallback to default dev_reports directory
            output_dir = Path('dev_reports')
            output_dir.mkdir(exist_ok=True)
            output_file = output_dir / 'dev.json'

        # Create report structure
        report_data = {
            'resource_id': resource_id,
            'sensitive': sensitivity,
            'sensitivity_level': sensitivity_level,
            'timestamp': datetime.now().isoformat(),
            'sdd_report': reports_dict,
        }

        # Save report
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, default=str)

        logger.info(
            f'Saved report to {output_file} (sensitivity={sensitivity}, '
            f'sensitivity_level={sensitivity_level}, {len(reports_dict)} sheets)'
        )


def main():
    """
    Main entry point for event processing.

    This can be run standalone or integrated with Redis event bus.
    """
    processor = EventProcessor()

    # Example: Process a test event
    test_event = {
        'resource_id': 'test-resource-id',
        'package_id': 'test-package-123',
        'download_url': 'download-url.nl',
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
    assert 'sensitivity_level' in dev_data

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
        assert 'personal_data_risk_level' in sheet_report
        assert 'non_personal_data_risk_level' in sheet_report

        # Model fields are optional (None when steps are disabled)
        # Just verify they exist in the report, even if None
        assert 'non_pii_model' in sheet_report or 'non_personal_data' in sheet_report

        logger.info('✅ All required fields present in dev.json')
