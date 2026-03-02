"""
Batch Process Datasets with a Specific Model

This script processes all datasets that exist in groundtruth2 with a specified model.
Useful for running a new model on your test set.

Usage:
    uv run python batch_process_model.py --model gpt-4.1-nano
    uv run python batch_process_model.py --model gpt-4.1-nano --skip-existing
"""

import json
import logging
import argparse
from pathlib import Path
from typing import List
from dotenv import load_dotenv

# Import from clean architecture
from config.config import Config
from src.infrastructure.factories.pipeline_factory import PipelineFactory
from src.domain.entities import SheetReport
from src.application.use_cases.process_dataset import ProcessDatasetUseCase

# Setup logging
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def setup_pipeline(model_name: str) -> ProcessDatasetUseCase:
    """
    Setup the pipeline with the specified model using PipelineFactory.

    Args:
        model_name: Name of the model to use for all LLM tasks

    Returns:
        Configured ProcessDatasetUseCase
    """
    print(f'Setting up pipeline with model: {model_name}')

    # Initialize config
    config = Config()

    # Override model configuration to use the specified model for all tasks
    config.PII_DETECT_MODEL = model_name
    config.PII_REFLECT_MODEL = model_name
    config.NON_PII_DETECT_MODEL = model_name

    # Ensure all detection steps are enabled
    config.PERSONAL_DATA_DETECTION = True
    config.PERSONAL_DATA_REFLECTION = True
    config.NON_PERSONAL_DATA_DETECTION = True

    # Initialize factory with overridden config
    factory = PipelineFactory(config)

    # Create pipeline
    use_case = factory.create_pipeline(sample_size=5)

    print('Pipeline setup complete!')
    return use_case


def load_isp_rules_from_filename(filename: str) -> dict:
    """
    Load ISP (Information Sensitivity Protocol) rules using EventProcessor's matching logic.

    Args:
        filename: Name of the file to extract country from

    Returns:
        Dictionary containing ISP rules for the matched country or default
    """
    # Import here to avoid circular imports
    from src.event_processor import EventProcessor

    # Create a minimal EventProcessor instance just for ISP matching
    # (we don't need the full pipeline for this)
    processor = EventProcessor()

    # Use the EventProcessor's ISP matching logic
    return processor._get_isp_rules(package_id=None, resource_name=filename)


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
    pipeline: ProcessDatasetUseCase, dataset_name: str, model_name: str, output_dir: Path, skip_existing: bool = False
) -> bool:
    """
    Process a single dataset with the specified model.

    Args:
        pipeline: Configured pipeline
        dataset_name: Name of the dataset file
        model_name: Model name for output directory
        output_dir: Directory to save results
        skip_existing: Skip if output file already exists

    Returns:
        True if successful, False otherwise
    """
    output_file = output_dir / f'{dataset_name}.json'

    # Check if already processed
    if skip_existing and output_file.exists():
        print(f'⏭️  Skipping {dataset_name} (already exists)')
        return True

    # Find source file
    source_file = get_source_file_path(dataset_name)

    if source_file is None:
        logger.error(f'❌ Cannot process {dataset_name}: source file not found')
        return False

    print(f'📊 Processing: {dataset_name}')

    try:
        # Load ISP rules based on filename
        isp_rules = load_isp_rules_from_filename(dataset_name)

        # Process the dataset
        sheet_reports: List[SheetReport] = pipeline.execute(
            source=str(source_file),
            resource_id=dataset_name,
            is_url=False,
            isp_rules=isp_rules,
        )

        # Convert to dictionaries
        reports_dict = [report.to_dict() for report in sheet_reports]

        # Save results
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with output_file.open('w', encoding='utf-8') as f:
            json.dump(reports_dict, f, indent=2, ensure_ascii=False)

        # Log summary
        sensitive_sheets = sum(1 for r in sheet_reports if r.is_sensitive())
        print(f'✅ Completed {dataset_name}: {len(sheet_reports)} sheets, {sensitive_sheets} sensitive')

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

    # Setup pipeline
    pipeline = setup_pipeline(args.model)

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

    # Process each dataset
    total = len(datasets)
    successful = 0
    failed = 0
    skipped = 0

    print(f'\nProcessing {total} datasets...\n')

    for i, dataset_name in enumerate(datasets, 1):
        print(f'[{i}/{total}] {dataset_name}')

        success = process_dataset(
            pipeline=pipeline,
            dataset_name=dataset_name,
            model_name=args.model,
            output_dir=output_dir,
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
