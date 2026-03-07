#!/usr/bin/env python3
"""
Enhanced Batch Processing Script for Local Documents

This script provides flexible batch processing capabilities for local documents
with configurable output paths and multiple input options.

Usage:
    # Process single local file
    python enhanced_batch_processor.py --file "/Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/research/data/congo dataset test.xlsx" --model gpt-4.1-nano --output "/Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/research/results/test_results/gpt-4.1-nano/test.json"

    # Process all files in directory
    python enhanced_batch_processor.py --input-dir /path/to/files --model gpt-4.1 --output-dir /path/to/results

    # Process with groundtruth datasets (original functionality)
    python enhanced_batch_processor.py --groundtruth --model gpt-4.1 --output-dir /custom/path
"""

import json
import logging
import argparse
from pathlib import Path
from typing import List, Optional
from dotenv import load_dotenv

# Import from clean architecture
from config import get_config
from src.event_processor import EventProcessor

# Setup logging
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def setup_event_processor(model_name: str, custom_output_path: Optional[str] = None) -> EventProcessor:
    """
    Setup the EventProcessor with the specified model and custom output path.

    Args:
        model_name: Name of the model to use for all LLM tasks
        custom_output_path: Custom path for output files (optional)

    Returns:
        Configured EventProcessor
    """
    print(f'Setting up EventProcessor with model: {model_name}')
    if custom_output_path:
        print(f'Custom output path: {custom_output_path}')

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

    # Create EventProcessor with custom output path
    event_processor = EventProcessor(custom_output_path=custom_output_path)

    print('EventProcessor setup complete!')
    return event_processor


def get_local_files(input_dir: Path, extensions: List[str] = None) -> List[Path]:
    """
    Get list of local files from directory with specified extensions.

    Args:
        input_dir: Directory to search for files
        extensions: List of file extensions to include (default: ['.csv', '.xlsx', '.json'])

    Returns:
        List of file paths
    """
    if extensions is None:
        extensions = ['.csv', '.xlsx', '.json']

    if not input_dir.exists():
        logger.error(f'Input directory not found: {input_dir}')
        return []

    files = []
    for ext in extensions:
        files.extend(input_dir.glob(f'*{ext}'))
        files.extend(input_dir.glob(f'*{ext.upper()}'))

    print(f'Found {len(files)} files in {input_dir}')
    return files


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


def process_single_file(event_processor: EventProcessor, file_path: Path, output_path: Path) -> bool:
    """
    Process a single local file using EventProcessor.

    Args:
        event_processor: Configured EventProcessor instance
        file_path: Path to the input file
        output_path: Path for the output file

    Returns:
        True if successful, False otherwise
    """
    print(f'📊 Processing: {file_path.name}')

    try:
        # Create event for EventProcessor
        event = {
            'resource_id': file_path.stem,
            'download_url': str(file_path),
            'file_name': file_path.name,
            'event_type': 'local-processing',
        }

        # Set custom output path for this specific file
        event_processor.custom_output_path = output_path

        # Process using EventProcessor
        success, message = event_processor.process_event(event)

        if success:
            # Verify the output file was created
            if output_path.exists():
                # Read the report to log summary
                with output_path.open('r', encoding='utf-8') as f:
                    report_data = json.load(f)

                # Extract sdd_report for summary
                reports = report_data.get('sdd_report', [])
                sensitive_sheets = sum(
                    1 for r in reports if r.get('personal_data_sensitive') or r.get('non_personal_data_sensitive')
                )
                print(f'✅ Completed {file_path.name}: {len(reports)} sheets, {sensitive_sheets} sensitive')
            else:
                logger.error(f'❌ No report generated for {file_path.name}')
                return False
        else:
            logger.error(f'❌ Failed to process {file_path.name}: {message}')
            return False

        return True

    except Exception as e:
        logger.error(f'❌ Failed to process {file_path.name}: {e}', exc_info=True)
        return False


def process_groundtruth_dataset(
    event_processor: EventProcessor, dataset_name: str, output_dir: Path, skip_existing: bool = False
) -> bool:
    """
    Process a single groundtruth dataset using EventProcessor.

    Args:
        event_processor: Configured EventProcessor instance
        dataset_name: Name of the dataset file
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

    return process_single_file(event_processor, source_file, output_file)


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Enhanced batch processing for local documents')

    # Input options (mutually exclusive)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('--file', type=str, help='Process a single local file')
    input_group.add_argument('--input-dir', type=str, help='Process all files in a directory')
    input_group.add_argument('--groundtruth', action='store_true', help='Process groundtruth datasets')

    # Model and output options
    parser.add_argument('--model', type=str, required=True, help='Model name to use (e.g., gpt-4.1, gpt-5-nano)')
    parser.add_argument('--output', type=str, help='Output file path (for single file processing)')
    parser.add_argument('--output-dir', type=str, help='Output directory path (for batch processing)')

    # Additional options
    parser.add_argument('--skip-existing', action='store_true', help='Skip files that already have results')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of files to process (for testing)')
    parser.add_argument(
        '--extensions',
        nargs='+',
        default=['.csv', '.xlsx', '.json'],
        help='File extensions to process (default: .csv .xlsx .json)',
    )

    args = parser.parse_args()

    print('=' * 70)
    print(f'Enhanced Batch Processing with Model: {args.model}')
    if args.file:
        print(f'Input File: {args.file}')
        print(f'Output File: {args.output}')
    elif args.input_dir:
        print(f'Input Directory: {args.input_dir}')
        print(f'Output Directory: {args.output_dir}')
    elif args.groundtruth:
        print('Processing Groundtruth Datasets')
        print(f'Output Directory: {args.output_dir}')
    print('=' * 70)
    print()

    # Setup output paths
    if args.file:
        if not args.output:
            logger.error('--output is required when processing a single file')
            return
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        files_to_process = [Path(args.file)]
    elif args.input_dir:
        if not args.output_dir:
            logger.error('--output-dir is required when processing a directory')
            return
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        input_dir = Path(args.input_dir)
        files_to_process = get_local_files(input_dir, args.extensions)
    elif args.groundtruth:
        if not args.output_dir:
            # Default output directory for groundtruth
            output_dir = Path(f'research/results/test_results/{args.model}')
        else:
            output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        dataset_names = get_groundtruth_datasets()

    # Setup EventProcessor
    if args.file:
        event_processor = setup_event_processor(args.model, str(output_path))
    else:
        event_processor = setup_event_processor(args.model, str(output_dir))

    # Process files
    total = 0
    successful = 0
    failed = 0
    skipped = 0

    if args.file:
        # Single file processing
        total = 1
        success = process_single_file(event_processor, files_to_process[0], output_path)
        if success:
            successful = 1
        else:
            failed = 1

    elif args.input_dir:
        # Directory processing
        files_to_process = files_to_process[: args.limit] if args.limit else files_to_process
        total = len(files_to_process)

        print(f'\nProcessing {total} files...\n')

        for i, file_path in enumerate(files_to_process, 1):
            print(f'[{i}/{total}] {file_path.name}')

            output_file = output_dir / f'{file_path.stem}.json'
            if args.skip_existing and output_file.exists():
                print(f'⏭️  Skipping {file_path.name} (already exists)')
                skipped += 1
                continue

            success = process_single_file(event_processor, file_path, output_file)
            if success:
                successful += 1
            else:
                failed += 1
            print()

    elif args.groundtruth:
        # Groundtruth processing
        dataset_names = dataset_names[: args.limit] if args.limit else dataset_names
        total = len(dataset_names)

        print(f'\nProcessing {total} datasets...\n')

        for i, dataset_name in enumerate(dataset_names, 1):
            print(f'[{i}/{total}] {dataset_name}')

            success = process_groundtruth_dataset(
                event_processor=event_processor,
                dataset_name=dataset_name,
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
    print('ENHANCED BATCH PROCESSING COMPLETE')
    print('=' * 70)
    print(f'Total items: {total}')
    print(f'✅ Successful: {successful}')
    print(f'⏭️  Skipped: {skipped}')
    print(f'❌ Failed: {failed}')

    if args.file:
        print(f'\nResult saved to: {output_path}')
    else:
        print(f'\nResults saved to: {output_dir}')
    print('=' * 70)


if __name__ == '__main__':
    main()
