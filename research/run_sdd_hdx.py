"""
Batch Process HDX Resource IDs

Usage:
    uv run python research/run_sdd_hdx.py --model DeepSeek-V4-Flash --ids \
0ae0101c-dcf2-4228-80f4-4d9c9cc2f448 \
13899e52-5d6e-4b05-b8a1-2b378b5b2cda \
1a13db39-bce9-4565-845c-ad3299206dfd \
1e10fb4e-0670-435e-b788-9edd633f9d1a \
1e544ee4-22c5-45db-84b7-a7f891c49aab \
58451add-37e2-4228-8e78-677a65407169 \
59ae2ec0-0f33-4654-ac5f-ff10061da8d5 \
5e812b48-eef4-4600-890c-0a3e47a44b4c \
5f489493-363c-4516-ac85-bd87de860117 \
600bc14a-bf1d-4c90-bd5c-55f46e476dee \
68b7d855-add3-4705-8afb-1df65b4a2d65 \
7b7331a2-304a-42ff-ac46-8e72e7434c1d \
83cfb940-b848-4e26-a9e3-7f318de51b39 \
87b89f79-d096-4b8a-b142-d27041202d8b \
8ec923c2-1287-490f-954f-ccf0057714b7 \
ba40c81d-ad3a-465e-9851-c916d9a2e38f \
bcf66afc-b2c2-43ce-b355-5424898548aa \
d6629f7a-4417-4b47-8e50-439b49547d24 \
d669fd25-9e1b-4d37-aab7-9049970cc207 \
daf46c0a-a7a3-4e05-b057-72d78fa4226f \
e1c0902c-0c6c-4d08-8db4-388e68b5a3d6 \
f1a0e86a-5615-4c07-9595-1b413e0089eb \
fbca2eaa-3a87-48c3-a1c1-4932814de642
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

    return EventProcessor(custom_output_path=str(output_dir), config=config)


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
