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

from src.infrastructure.factories import PipelineFactory
from src.domain.entities import SheetReport
from src.shared.utils.isp_retrieval import ISPRetriever

# Legacy imports for CKAN and Redis (to be refactored later)
from src.shared.utils.ckan import CKANClient
from hdx_redis_lib import connect_to_key_value_store_with_env_vars

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

    def __init__(self, custom_output_path: Optional[str] = None):
        """Initialize event processor with all dependencies."""
        logger.info('Initializing Event Processor...')

        # Load configuration
        self.config = get_config()

        # Set custom output path if provided
        self.custom_output_path = Path(custom_output_path) if custom_output_path else None

        # Create pipeline using factory
        factory = PipelineFactory(self.config)
        self.pipeline = factory.create_pipeline(sample_size=5)

        # Initialize ISP retriever
        isp_strategy_name = self.config.ISP_STRATEGY.lower()
        if isp_strategy_name == 'google_sheets':
            from src.infrastructure.external.isp_strategies import GoogleSheetsISPStrategy

            strategy = GoogleSheetsISPStrategy(spreadsheet_url=self.config.ISP_GOOGLE_SHEET_URL)
            logger.info(f'Using GoogleSheetsISPStrategy for ISP retrieval ({self.config.ISP_GOOGLE_SHEET_URL})')
        else:
            from src.infrastructure.external.isp_strategies import LocalJSONISPStrategy

            strategy = LocalJSONISPStrategy(json_path=self.config.ISP_LOCAL_JSON_PATH)
            logger.info(f'Using LocalJSONISPStrategy for ISP retrieval ({self.config.ISP_LOCAL_JSON_PATH})')

        kv_store = None
        if self.config.WORKER_ENABLED:
            try:
                kv_store = connect_to_key_value_store_with_env_vars(expire_in_seconds=60 * 60 * 12)
            except Exception as e:
                logger.warning(f'Could not initialize Redis KV store: {e}')

        self.isp_retriever = ISPRetriever(strategy=strategy, store=kv_store)
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
            resource_name = None
            dataset_context = {}
            resource_context = {}

            if self.ckan:
                resource = self.ckan.resource_show(resource_id)
                download_url = resource.get('download_url')
                resource_name = resource.get('name', 'unknown_dataset.csv')

                package_id = event.get('package_id') or event.get('dataset_id')
                package = self.ckan.package_show(package_id) if package_id else {}

                dataset_context = {
                    'Title': package.get('title'),
                    'Description': package.get('notes'),
                    'Source': package.get('dataset_source'),
                    'Geography': (
                        ', '.join(g.get('title', g.get('name', '')) for g in package.get('groups', []))
                        if package.get('groups')
                        else None
                    ),
                    'Organization': (
                        package.get('organization', {}).get('title') if package.get('organization') else None
                    ),
                }
                resource_context = {
                    'Name': resource_name,
                    'Description': resource.get('description'),
                }
            else:
                # When CKAN is disabled, expect download_url in event
                download_url = event.get('download_url')
                resource_name = event.get('file_name') or event.get(
                    'resource_name'
                )  # Attempt to get filename from event if available
                dataset_context = {
                    'Title': event.get('dataset_title'),
                    'Description': event.get('dataset_description'),
                }
                resource_context = {'Name': resource_name, 'Description': event.get('resource_description')}

            def clean_metadata(md: dict) -> dict:
                cleaned = {}
                for k, v in md.items():
                    if v is None:
                        continue
                    v_str = str(v).strip()
                    if not v_str:
                        continue
                    cleaned[k] = v_str[:300]
                return cleaned

            dataset_context = clean_metadata(dataset_context)
            resource_context = clean_metadata(resource_context)

            if not download_url:
                logger.error(f'No download URL for resource {resource_id}')
                return False, 'No download URL'

            # Get dataset location for ISP rules
            package_id = event.get('dataset_id')
            isp_rules = self.isp_retriever.get_isp_rules(package_id, resource_name, self.ckan)

            # Process dataset using our use case
            logger.info(f'Processing dataset from: {download_url}')
            http_headers = self.ckan.headers if self.ckan else {}
            reports = self.pipeline.execute(
                source=download_url,
                resource_id=resource_id,
                is_url=self.custom_output_path is None,
                isp_rules=isp_rules,
                http_headers=http_headers,
                dataset_context=dataset_context,
                resource_context=resource_context,
            )

            # Determine overall sensitivity
            sensitivity = self._determine_sensitivity(reports)

            # Save to CKAN
            self._save_to_ckan(resource_id, reports, sensitivity)

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
            'timestamp': datetime.now().isoformat(),
            'sdd_report': reports_dict,
        }

        # Save report
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
