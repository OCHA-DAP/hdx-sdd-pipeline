"""
Event processor for HDX resource events.

This is the main entry point for processing events from Redis streams.
It uses the clean architecture use cases to process datasets.
"""

import json
import logging
import os
from typing import Dict, Any, Tuple
from dotenv import load_dotenv

from src.application.use_cases.process_dataset import ProcessDatasetUseCase
from src.infrastructure.llm.azure_openai_provider import AzureOpenAIProvider
from src.infrastructure.storage.data_loader import SmartDataLoader
from src.shared.utils.prompt_manager import PromptManager
from src.domain.entities import SheetReport

# Legacy imports for CKAN and Redis (to be refactored later)
from utils.ckan import CKANClient

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

        # Setup pipeline
        self.pipeline = self._setup_pipeline()

        # Setup CKAN client
        self.ckan = CKANClient(
            base_url=os.getenv('HDX_URL', 'https://data.humdata.org'), api_token=os.getenv('HDX_KEY', '')
        )

        logger.info('Event Processor initialized')

    def _setup_pipeline(self) -> ProcessDatasetUseCase:
        """Setup the processing pipeline."""
        # Data loader
        data_loader = SmartDataLoader(max_rows=1000)

        # LLM providers
        pii_llm = AzureOpenAIProvider(
            model_name=os.getenv('PII_DETECT_MODEL', 'gpt-4.1-nano'),
            azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT'),
            api_key=os.getenv('AZURE_OPENAI_API_KEY'),
        )

        pii_reflection_llm = AzureOpenAIProvider(
            model_name=os.getenv('PII_REFLECT_MODEL', 'gpt-4.1-nano'),
            azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT'),
            api_key=os.getenv('AZURE_OPENAI_API_KEY'),
        )

        non_pii_llm = AzureOpenAIProvider(
            model_name=os.getenv('NON_PII_DETECT_MODEL', 'gpt-4.1-nano'),
            azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT'),
            api_key=os.getenv('AZURE_OPENAI_API_KEY'),
        )

        # Prompt manager
        prompt_manager = PromptManager(prompts_dir='src/prompts')

        # Create use case
        return ProcessDatasetUseCase(
            data_loader=data_loader,
            pii_llm_provider=pii_llm,
            pii_reflection_llm_provider=pii_reflection_llm,
            non_pii_llm_provider=non_pii_llm,
            prompt_manager=prompt_manager,
            sample_size=5,
        )

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
            # Check if already processed
            if self._report_exists(resource_id):
                logger.info(f'Report already exists for {resource_id}')
                return True, 'Already processed'

            # Get resource info from CKAN
            resource = self.ckan.resource_show(resource_id)
            download_url = resource.get('download_url')

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
        try:
            resource = self.ckan.resource_show(resource_id)
            return 'sdd_report' in resource and resource['sdd_report']
        except Exception:
            return False

    def _get_isp_rules(self, package_id: str) -> Dict[str, Any]:
        """Get ISP rules based on dataset location."""
        if not package_id:
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
                return {'default': isps.get('default', {})}

            # Find matching ISP
            if isinstance(countries, str):
                countries = [countries]

            for country in countries:
                for isp_name, isp_data in isps.items():
                    if isp_data.get('country', '').lower() in country.lower():
                        return {isp_name: isp_data}

            return {'default': isps.get('default', {})}

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
        """Save results to CKAN."""
        # Convert reports to dict
        reports_dict = [report.to_dict() if isinstance(report, SheetReport) else report for report in reports]

        # Update CKAN resource
        self.ckan.update_resource_fields(
            resource_id, {'sdd_report': json.dumps(reports_dict, indent=2), 'sensitive': sensitivity}
        )

        logger.info(f'Saved report to CKAN for resource {resource_id}')


def main():
    """
    Main entry point for event processing.

    This can be run standalone or integrated with Redis event bus.
    """
    processor = EventProcessor()

    # Example: Process a test event
    test_event = {
        'resource_id': 'test-resource-123',
        'package_id': 'test-package-456',
        'event_type': 'resource-data-changed',
    }

    success, message = processor.process_event(test_event)

    if success:
        logger.info(f'✅ {message}')
    else:
        logger.error(f'❌ {message}')


if __name__ == '__main__':
    main()
