"""
Batch Process HDX Resource IDs

Usage:
    uv run python research/run_sdd_hdx.py --ids 5999dffc-dfe0-40e3-9411-0f77d43d1e13
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = Path('research/results/false_positives_prod')


def _bootstrap() -> None:
    """Extend sys.path and load .env before local imports are resolved."""
    from dotenv import load_dotenv

    sys.path.append(os.getcwd())
    load_dotenv()


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------


@dataclass
class BatchResult:
    total: int
    successful: int = 0
    skipped: int = 0
    failed: int = 0

    def record(self, outcome: str) -> None:
        setattr(self, outcome, getattr(self, outcome) + 1)

    def print_summary(self, output_dir: Path) -> None:
        print('=' * 70)
        print('BATCH PROCESSING COMPLETE')
        print('=' * 70)
        print(f'Total:      {self.total}')
        print(f'✅ Success: {self.successful}')
        print(f'⏭️  Skipped: {self.skipped}')
        print(f'❌ Failed:  {self.failed}')
        print(f'\nResults saved to: {output_dir}')
        print('=' * 70)


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


def build_event_processor(model_name: str, output_dir: Path):
    from config import get_config
    from src.event_processor import EventProcessor

    logger.info('Setting up EventProcessor with model: %s', model_name)

    config = get_config()
    config.PII_DETECT_MODEL = model_name
    config.PII_REFLECT_MODEL = model_name
    config.NON_PII_DETECT_MODEL = model_name
    config.PERSONAL_DATA_DETECTION = True
    config.PERSONAL_DATA_REFLECTION = True
    config.NON_PERSONAL_DATA_DETECTION = True
    config.CKAN_UPDATE = False

    return EventProcessor(custom_output_path=str(output_dir))


def build_ckan_client():
    from config import get_config
    from src.shared.utils.ckan import CKANClient

    config = get_config()
    return CKANClient(
        base_url=config.HDX_URL_PROD,
        api_token=config.HDX_KEY_PROD,
        user_agent=config.SDD_USER_AGENT,
    )


# ---------------------------------------------------------------------------
# Resource helpers
# ---------------------------------------------------------------------------


def fetch_event(ckan, resource_id: str) -> Optional[dict]:
    logger.info('Fetching metadata for resource: %s', resource_id)

    resource = ckan.resource_show(resource_id)
    if not resource:
        logger.error('Resource %s not found in CKAN', resource_id)
        return None

    download_url = resource.get('download_url')
    if not download_url:
        logger.error('No download URL for resource %s', resource_id)
        return None

    return {
        'resource_id': resource_id,
        'dataset_id': resource.get('package_id'),
        'download_url': download_url,
        'file_name': resource.get('name', f'{resource_id}.csv'),
        'event_type': 'batch-processing',
    }


def process_resource(
    resource_id: str,
    ckan,
    processor,
    output_dir: Path,
    skip_existing: bool,
) -> str:
    output_file = output_dir / f'{resource_id}.json'

    if skip_existing and output_file.exists():
        print(f'⏭️  Skipping {resource_id} (already exists)')
        return 'skipped'

    event = fetch_event(ckan, resource_id)
    if not event:
        print(f'❌ Failed to get metadata for {resource_id}')
        return 'failed'

    try:
        success, message = processor.process_event(event)
    except Exception as e:
        logger.exception('Unexpected error processing %s', resource_id)
        print(f'❌ Error processing {resource_id}: {e}')
        return 'failed'

    if success:
        print(f'✅ {resource_id}: {message}')
        return 'successful'

    print(f'❌ {resource_id}: {message}')
    return 'failed'


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Batch process specific HDX resource IDs')
    parser.add_argument('--model', default='gpt-4.1-nano', help='Model name to use')
    parser.add_argument('--ids', nargs='+', help='Resource IDs to process')
    parser.add_argument('--skip-existing', action='store_true', help='Skip already-processed IDs')
    return parser.parse_args()


def main() -> None:
    _bootstrap()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    args = parse_args()

    resource_ids: list[str] = args.ids or []
    if not resource_ids:
        logger.error('No resource IDs provided. Use --ids <id1> <id2> …')
        sys.exit(1)

    output_dir = DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    print('=' * 70)
    print(f'Batch Processing {len(resource_ids)} resource(s)  |  model: {args.model}')
    print(f'Output: {output_dir}')
    print('=' * 70)

    ckan = build_ckan_client()
    processor = build_event_processor(args.model, output_dir)
    results = BatchResult(total=len(resource_ids))

    for i, resource_id in enumerate(resource_ids, 1):
        print(f'\n[{i}/{results.total}] {resource_id}')
        outcome = process_resource(resource_id, ckan, processor, output_dir, args.skip_existing)
        results.record(outcome)

    print()
    results.print_summary(output_dir)


if __name__ == '__main__':
    main()
