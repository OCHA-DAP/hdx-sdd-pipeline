"""
Batch Process Datasets with a Specific Model

This script processes all datasets that exist in groundtruth2 with a specified model.
Useful for running a new model on your test set.

Usage:
    uv run python batch_process_model.py --model DeepSeek-V4-Flash --skip-existing
    uv run python batch_process_model.py --model gpt-4.1-nano --skip-existing
"""

import json
import logging
import argparse
from pathlib import Path
from typing import List
from dotenv import load_dotenv

# Import from clean architecture
from config import get_config
from src.event_processor import EventProcessor
from src.shared.utils.ckan import CKANClient

# Non-sensitive resource IDs
NON_SENSITIVE_RESOURCE_IDS = {
    "1a13db39-bce9-4565-845c-ad3299206dfd",
    "ba40c81d-ad3a-465e-9851-c916d9a2e38f",
    "87b89f79-d096-4b8a-b142-d27041202d8b",
    # "1e10fb4e-0670-435e-b788-9edd633f9d1a",
    "5e812b48-eef4-4600-890c-0a3e47a44b4c",
    "0ae0101c-dcf2-4228-80f4-4d9c9cc2f448",
    "e1c0902c-0c6c-4d08-8db4-388e68b5a3d6",
    "d669fd25-9e1b-4d37-aab7-9049970cc207",
    "5f489493-363c-4516-ac85-bd87de860117",
    "68b7d855-add3-4705-8afb-1df65b4a2d65",
    "83cfb940-b848-4e26-a9e3-7f318de51b39",
    "7b7331a2-304a-42ff-ac46-8e72e7434c1d",
    "58451add-37e2-4228-8e78-677a65407169",
    "59ae2ec0-0f33-4654-ac5f-ff10061da8d5",
    "8ec923c2-1287-490f-954f-ccf0057714b7",
    "bcf66afc-b2c2-43ce-b355-5424898548aa",
    "daf46c0a-a7a3-4e05-b057-72d78fa4226f",
    "d40af4ec-39bc-4dbb-afa2-100b14faebcd",
    "d6629f7a-4417-4b47-8e50-439b49547d24",
    "13899e52-5d6e-4b05-b8a1-2b378b5b2cda",
    "f1a0e86a-5615-4c07-9595-b1413e0089eb",
    "d68f4008-938a-4d13-be3d-0ea2f833b62b",
    "8c9ea9cb-0184-402f-b596-a49aa38e3706",
    "5d382ae2-d52e-457c-a98c-040eb4f42421"
}

# Fallback mapping for private or missing resources on CKAN to their local filenames
RESOURCE_ID_TO_LOCAL_FILE = {
    "ba40c81d-ad3a-465e-9851-c916d9a2e38f": "Event Data UKR.csv",
}

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def setup_event_processor(model_name: str, output_dir: Path) -> EventProcessor:
    """
    Setup the EventProcessor with the specified model and output directory.

    Args:
        model_name: Name of the model to use for all LLM tasks
        output_dir: Directory to save results

    Returns:
        Configured EventProcessor
    """
    print(f'Setting up EventProcessor with model: {model_name}')

    # Initialize config
    config = get_config()

    # Override model configuration to use the specified model for all tasks
    config.PII_DETECT_MODEL = model_name
    config.PII_REFLECT_MODEL = model_name
    config.NON_PII_DETECT_MODEL = model_name
    config.README_SCAN_MODEL = model_name

    # Ensure all detection steps are enabled
    config.PERSONAL_DATA_DETECTION = True
    config.PERSONAL_DATA_REFLECTION = True
    config.NON_PERSONAL_DATA_DETECTION = True

    # Disable CKAN updates for batch processing
    config.CKAN_UPDATE = False

    # Create EventProcessor with custom output directory
    event_processor = EventProcessor(custom_output_path=str(output_dir), config=config)

    print('EventProcessor setup complete!')
    return event_processor


def get_groundtruth_datasets() -> List[str]:
    """
    Get list of all dataset filenames from groundtruth2 directory.

    Returns:
        List of dataset filenames (without .json extension)
    """
    groundtruth_dir = Path('research/results/test_results/groundtruth2')

    if not groundtruth_dir.exists():
        logger.error(f'Groundtruth directory not found: {groundtruth_dir}')
        return []

    # Get all JSON files and remove the .json extension
    datasets = [f.stem for f in groundtruth_dir.glob('*.json')]

    print(f'Found {len(datasets)} datasets in groundtruth2')
    return datasets


def get_source_file_path(dataset_name: str) -> Path:
    """
    Find the source data file for a given dataset name.

    Args:
        dataset_name: Name of the dataset (e.g., "Event Data AFG.csv")

    Returns:
        Path to the source file
    """
    data_dir = Path('research/data')

    # Try to find the file
    source_file = data_dir / dataset_name

    if source_file.exists():
        return source_file

    # If not found, log warning
    logger.warning(f'Source file not found: {source_file}')
    return None


def process_dataset(
    event_processor: EventProcessor,
    dataset_name: str,
    model_name: str,
    output_dir: Path,
    ckan: CKANClient,
    skip_existing: bool = False,
) -> bool:
    """
    Process a single dataset using EventProcessor.

    Args:
        event_processor: Configured EventProcessor instance
        dataset_name: Name of the dataset file
        model_name: Model name for output directory
        output_dir: Directory to save results
        ckan: CKANClient instance
        skip_existing: Skip if output file already exists

    Returns:
        True if successful, False otherwise
    """
    output_file = output_dir / f'{dataset_name}.json'

    # Check if already processed
    if skip_existing and output_file.exists():
        print(f'⏭️  Skipping {dataset_name} (already exists)')
        return True

    is_resource_id = dataset_name in NON_SENSITIVE_RESOURCE_IDS

    download_url = None
    file_name = dataset_name

    if is_resource_id:
        # Try to resolve metadata from CKAN
        try:
            resource = ckan.resource_show(dataset_name)
            if resource:
                download_url = resource.get('download_url')
                file_name = resource.get('name') or dataset_name
        except Exception as e:
            logger.warning(f"Could not fetch metadata from CKAN for {dataset_name}: {e}")

        # Fallback to local file if CKAN fails or if we have a hardcoded local file mapping
        if not download_url:
            local_name = RESOURCE_ID_TO_LOCAL_FILE.get(dataset_name)
            if local_name:
                source_file = Path('research/data') / local_name
                if source_file.exists():
                    download_url = str(source_file)
                    file_name = local_name
                    logger.info(f"Using local fallback file for {dataset_name}: {source_file}")

        if not download_url:
            logger.error(f'❌ Cannot process {dataset_name}: failed to get download URL from CKAN and no local fallback available')
            return False
    else:
        # Sensitive dataset (local file)
        source_file = get_source_file_path(dataset_name)
        if source_file is None:
            logger.error(f'❌ Cannot process {dataset_name}: source file not found')
            return False
        download_url = str(source_file)

    print(f'📊 Processing: {dataset_name} (resolved as: {file_name})')

    try:
        # Create event for EventProcessor
        event = {
            'resource_id': dataset_name,
            'download_url': download_url,
            'file_name': file_name,
            'event_type': 'batch-processing',
        }

        # Process using EventProcessor - it will write directly to {output_dir}/{dataset_name}.json
        success, message = event_processor.process_event(event)

        if success:
            # Read the generated report from the output file
            if output_file.exists():
                with output_file.open('r', encoding='utf-8') as f:
                    report_data = json.load(f)

                # Log summary
                # Support both wrapped dict and raw list formats
                reports = report_data.get('sdd_report', []) if isinstance(report_data, dict) else report_data
                sensitive_sheets = sum(
                    1
                    for r in reports
                    if isinstance(r, dict)
                    and (r.get('personal_data_sensitive') or r.get('non_personal_data_sensitive'))
                )
                print(f'✅ Completed {dataset_name}: {len(reports)} sheets, {sensitive_sheets} sensitive')
            else:
                logger.error(f'❌ No report generated for {dataset_name}')
                return False
        else:
            logger.error(f'❌ Failed to process {dataset_name}: {message}')
            return False

        return True

    except Exception as e:
        logger.error(f'❌ Failed to process {dataset_name}: {e}', exc_info=True)
        return False


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Batch process datasets with a specific model')
    parser.add_argument('--model', type=str, required=True, help='Model name to use (e.g., gpt-4.1, gpt-5-nano)')
    parser.add_argument('--skip-existing', action='store_true', help='Skip datasets that already have results')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of datasets to process (for testing)')

    args = parser.parse_args()

    print('=' * 70)
    print(f'Batch Processing with Model: {args.model}')
    print('=' * 70)
    print()

    # Get list of datasets
    datasets = get_groundtruth_datasets()

    if not datasets:
        logger.error('No datasets found in groundtruth2!')
        return

    # Apply limit if specified
    if args.limit:
        datasets = datasets[: args.limit]
        print(f'Limited to first {args.limit} datasets')

    # Setup output directory
    output_dir = Path(f'research/results/test_results/{args.model}')
    output_dir.mkdir(parents=True, exist_ok=True)

    # Setup EventProcessor with custom output directory
    event_processor = setup_event_processor(args.model, output_dir)

    # Process each dataset
    total = len(datasets)
    successful = 0
    failed = 0
    skipped = 0

    print(f'\nProcessing {total} datasets...\n')

    # Initialize CKAN client for downloading non-sensitive datasets
    config = get_config()
    ckan = CKANClient(
        base_url=config.HDX_URL_PROD,
        api_token=config.HDX_KEY_PROD,
        user_agent=config.SDD_USER_AGENT,
    )

    for i, dataset_name in enumerate(datasets, 1):
        print(f'[{i}/{total}] {dataset_name}')

        success = process_dataset(
            event_processor=event_processor,
            dataset_name=dataset_name,
            model_name=args.model,
            output_dir=output_dir,
            ckan=ckan,
            skip_existing=args.skip_existing,
        )

        if success:
            if args.skip_existing and (output_dir / f'{dataset_name}.json').exists():
                skipped += 1
            else:
                successful += 1
        else:
            failed += 1

        print()

    # Print summary
    print('=' * 70)
    print('BATCH PROCESSING COMPLETE')
    print('=' * 70)
    print(f'Total datasets: {total}')
    print(f'✅ Successful: {successful}')
    print(f'⏭️  Skipped: {skipped}')
    print(f'❌ Failed: {failed}')
    print(f'\nResults saved to: {output_dir}')
    print('=' * 70)


if __name__ == '__main__':
    main()
