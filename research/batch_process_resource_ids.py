"""
Batch Process HDX Resource IDs

This script processes a specific list of HDX resource IDs and saves the reports
to research/results/false_positives_prod/.

Usage:
    uv run python research/batch_process_resource_ids.py --ids 1a13db39-bce9-4565-845c-ad3299206dfd \
    68b7d855-add3-4705-8afb-1df65b4a2d65 \
    13899e52-5d6e-4b05-b8a1-2b378b5b2cda \
    7b7331a2-304a-42ff-ac46-8e72e7434c1d \
    1e10fb4e-0670-435e-b788-9edd633f9d1a \
    ba40c81d-ad3a-465e-9851-c916d9a2e38f \
    daf46c0a-a7a3-4e05-b057-72d78fa4226f \
    fbca2eaa-3a87-48c3-a1c1-4932814de642 \
    e1c0902c-0c6c-4d08-8db4-388e68b5a3d6 \
    f1a0e86a-5615-4c07-9595-1b413e0089eb \
    87b89f79-d096-4b8a-b142-d27041202d8b \
    58451add-37e2-4228-8e78-677a65407169 \
    8ec923c2-1287-490f-954f-ccf0057714b7 \
    600bc14a-bf1d-4c90-bd5c-55f46e476dee \
    0ae0101c-dcf2-4228-80f4-4d9c9cc2f448 \
    83cfb940-b848-4e26-a9e3-7f318de51b39 \
    d669fd25-9e1b-4d37-aab7-9049970cc207 \
    5e812b48-eef4-4600-890c-0a3e47a44b4c \
    1e544ee4-22c5-45db-84b7-a7f891c49aab \
    d6629f7a-4417-4b47-8e50-439b49547d24 \
    bcf66afc-b2c2-43ce-b355-5424898548aa \
    59ae2ec0-0f33-4654-ac5f-ff10061da8d5 \
    5f489493-363c-4516-ac85-bd87de860117
""" 
import json
import logging
import argparse
import sys
import os
from pathlib import Path
from typing import List, Optional
from dotenv import load_dotenv

# Add project root to sys.path
sys.path.append(os.getcwd())

# Import from clean architecture
from config import get_config
from src.event_processor import EventProcessor
from src.shared.utils.ckan import CKANClient

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def setup_event_processor(model_name: str, output_dir: Path) -> EventProcessor:
    """
    Setup the EventProcessor with the specified model and output directory.
    """
    logger.info(f'Setting up EventProcessor with model: {model_name}')

    # Initialize config
    config = get_config()

    # Override model configuration to use the specified model for all tasks
    config.PII_DETECT_MODEL = model_name
    config.PII_REFLECT_MODEL = model_name
    config.NON_PII_DETECT_MODEL = model_name

    # Ensure all detection steps are enabled
    config.PERSONAL_DATA_DETECTION = True
    config.PERSONAL_DATA_REFLECTION = True
    config.NON_PERSONAL_DATA_DETECTION = True

    # Disable CKAN updates for batch processing
    config.CKAN_UPDATE = False

    # Create EventProcessor with custom output directory
    event_processor = EventProcessor(custom_output_path=str(output_dir))

    logger.info('EventProcessor setup complete!')
    return event_processor


def get_resource_metadata(ckan: CKANClient, resource_id: str) -> Optional[dict]:
    """
    Fetch necessary metadata for a resource from CKAN.
    """
    logger.info(f'Fetching metadata for resource: {resource_id}')
    resource = ckan.resource_show(resource_id)
    
    if not resource:
        logger.error(f'Resource {resource_id} not found in CKAN')
        return None
        
    download_url = resource.get('download_url')
    if not download_url:
        logger.error(f'No download URL for resource {resource_id}')
        return None
        
    # Get package ID for ISP rules
    package_id = resource.get('package_id')
    file_name = resource.get('name', f'{resource_id}.csv')
    
    return {
        'resource_id': resource_id,
        'dataset_id': package_id,
        'download_url': download_url,
        'file_name': file_name,
        'event_type': 'batch-processing',
    }


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Batch process specific HDX resource IDs')
    parser.add_argument('--model', type=str, default='gpt-4.1-nano', help='Model name to use')
    parser.add_argument('--ids', nargs='+', help='List of resource IDs to process')
    parser.add_argument('--skip-existing', action='store_true', help='Skip already processed IDs')

    args = parser.parse_args()

    # Default list of IDs if none provided via CLI
    # The user can edit this list in the script
    resource_ids = args.ids or [
        # Add default IDs here if needed
    ]

    if not resource_ids:
        logger.error('No resource IDs provided! Use --ids <id1> <id2> or edit the script.')
        return

    print('=' * 70)
    print(f'Batch Processing {len(resource_ids)} resources with Model: {args.model}')
    print(f'Output directory: research/results/false_positives_prod/')
    print('=' * 70)
    print()

    # Setup output directory
    output_dir = Path('research/results/false_positives_prod')
    output_dir.mkdir(parents=True, exist_ok=True)

    # Initialize CKAN client for metadata fetching
    config = get_config()
    ckan = CKANClient(
        base_url=config.HDX_URL_PROD,
        api_token=config.HDX_KEY_PROD,
        user_agent=config.SDD_USER_AGENT,
    )

    # Setup EventProcessor
    event_processor = setup_event_processor(args.model, output_dir)

    # Process each resource
    total = len(resource_ids)
    successful = 0
    failed = 0
    skipped = 0

    for i, resource_id in enumerate(resource_ids, 1):
        print(f'[{i}/{total}] Resource: {resource_id}')
        
        output_file = output_dir / f'{resource_id}.json'
        if output_file.exists():
            print(f'⏭️  Skipping {resource_id} (already exists)')
            skipped += 1
            continue

        # Get metadata
        event = get_resource_metadata(ckan, resource_id)
        if not event:
            print(f'❌ Failed to get metadata for {resource_id}')
            failed += 1
            continue

        # Process
        try:
            success, message = event_processor.process_event(event)
            if success:
                print(f'✅ Successfully processed {resource_id}: {message}')
                successful += 1
            else:
                print(f'❌ Failed to process {resource_id}: {message}')
                failed += 1
        except Exception as e:
            print(f'❌ Error processing {resource_id}: {e}')
            failed += 1
        
        print()

    # Print summary
    print('=' * 70)
    print('BATCH PROCESSING COMPLETE')
    print('=' * 70)
    print(f'Total resources: {total}')
    print(f'✅ Successful: {successful}')
    print(f'⏭️  Skipped: {skipped}')
    print(f'❌ Failed: {failed}')
    print(f'\nResults saved to: {output_dir}')
    print('=' * 70)


if __name__ == '__main__':
    main()
