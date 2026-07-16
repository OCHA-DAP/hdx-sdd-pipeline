"""
GLiNER-only Batch Processing for Groundtruth Datasets

This script runs the local GLiNER fast PII pre-scan on all datasets
in groundtruth2 and saves the GLiNER results to research/results/gliner.

Usage:
    uv run python run_gliner_batch.py [--skip-existing]
"""

import json
import logging
import argparse
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv

from config import get_config
from src.infrastructure.data_loader import SmartDataLoader
from src.infrastructure.gliner_scanner import GliNERScanner
from src.shared.utils.ckan import CKANClient

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def get_groundtruth_datasets() -> List[str]:
    """Get list of all dataset filenames from groundtruth2 directory."""
    groundtruth_dir = Path('research/results/test_results/groundtruth2')
    if not groundtruth_dir.exists():
        logger.error(f'Groundtruth directory not found: {groundtruth_dir}')
        return []
    return [f.stem for f in groundtruth_dir.glob('*.json') if f.name != 'remove.py']


def get_source_file_path(dataset_name: str) -> Path:
    """Find the source data file for a given dataset name."""
    data_dir = Path('research/data')
    source_file = data_dir / dataset_name
    if source_file.exists():
        return source_file
    logger.warning(f'Source file not found: {source_file}')
    return None


def run_gliner_on_dataset(
    dataset_name: str,
    data_loader: SmartDataLoader,
    scanner: GliNERScanner,
    ckan: CKANClient,
) -> Dict[str, Any]:
    """Run GLiNER scanner on all sheets of a dataset."""
    non_sensitive_ids = {
        '1a13db39-bce9-4565-845c-ad3299206dfd',
        'ba40c81d-ad3a-465e-9851-c916d9a2e38f',
        '87b89f79-d096-4b8a-b142-d27041202d8b',
        '5e812b48-eef4-4600-890c-0a3e47a44b4c',
        '0ae0101c-dcf2-4228-80f4-4d9c9cc2f448',
        'e1c0902c-0c6c-4d08-8db4-388e68b5a3d6',
        'd669fd25-9e1b-4d37-aab7-9049970cc207',
        '5f489493-363c-4516-ac85-bd87de860117',
        '68b7d855-add3-4705-8afb-1df65b4a2d65',
        '83cfb940-b848-4e26-a9e3-7f318de51b39',
        '7b7331a2-304a-42ff-ac46-8e72e7434c1d',
        '58451add-37e2-4228-8e78-677a65407169',
        '59ae2ec0-0f33-4654-ac5f-ff10061da8d5',
        '8ec923c2-1287-490f-954f-ccf0057714b7',
        'bcf66afc-b2c2-43ce-b355-5424898548aa',
        'daf46c0a-a7a3-4e05-b057-72d78fa4226f',
        'd40af4ec-39bc-4dbb-afa2-100b14faebcd',
        'd6629f7a-4417-4b47-8e50-439b49547d24',
        '13899e52-5d6e-4b05-b8a1-2b378b5b2cda',
        'f1a0e86a-5615-4c07-9595-b1413e0089eb',
        'd68f4008-938a-4d13-be3d-0ea2f833b62b',
        '8c9ea9cb-0184-402f-b596-a49aa38e3706',
        '5d382ae2-d52e-457c-a98c-040eb4f42421',
    }
    local_files = {
        'ba40c81d-ad3a-465e-9851-c916d9a2e38f': 'Event Data UKR.csv',
    }
    is_resource_id = dataset_name in non_sensitive_ids

    download_url = None
    file_name = dataset_name

    if is_resource_id:
        try:
            resource = ckan.resource_show(dataset_name)
            if resource:
                download_url = resource.get('download_url') or resource.get('url')
                file_name = resource.get('name') or dataset_name
        except Exception as e:
            logger.warning(f'Could not fetch metadata from CKAN for {dataset_name}: {e}')

        if not download_url:
            local_name = local_files.get(dataset_name)
            if local_name:
                source_file = Path('research/data') / local_name
                if source_file.exists():
                    download_url = str(source_file)
                    file_name = local_name

        if not download_url:
            raise ValueError('failed to get download URL from CKAN and no local fallback')
    else:
        source_file = get_source_file_path(dataset_name)
        if source_file is None:
            raise FileNotFoundError('source file not found')
        download_url = str(source_file)

    # Load sheets
    actual_is_url = download_url.startswith(('http://', 'https://'))
    if actual_is_url:
        sheets = data_loader.load_from_url(download_url)
    else:
        sheets = data_loader.load_from_file(download_url)

    gliner_reports = []

    for sheet_name, df in sheets.items():
        # Skip readme sheets
        normalized = sheet_name.lower().replace(' ', '')
        if any(keyword in normalized for keyword in ['readme', 'instructions', 'metadata', 'info']):
            continue

        print(f"  Scanning sheet '{sheet_name}'...")
        scan_result = scanner.scan_dataframe(df)

        explanation = ''
        if scan_result.flagged:
            # Group hits by column
            from collections import Counter, defaultdict

            col_label_counts = defaultdict(Counter)
            for hit in scan_result.evidence:
                col_label_counts[hit['column']][hit['label']] += 1

            col_summaries = []
            for col_name, label_counter in col_label_counts.items():
                parts = [f'{label} \u00d7{count}' for label, count in label_counter.most_common()]
                col_summaries.append(f"'{col_name}': {', '.join(parts)}")

            explanation = (
                f"GLiNER pre-scan detected personal data ({len(scan_result.evidence)} "
                f"hit(s) across {len(col_label_counts)} column(s)): {'; '.join(col_summaries)}."
            )

        gliner_reports.append(
            {
                'sheet_name': sheet_name,
                'personal_data_sensitive': scan_result.flagged,
                'explanation': explanation,
                'gliner_scan_evidence': scan_result.evidence,
            }
        )

    return {'resource_id': dataset_name, 'file_name': file_name, 'gliner_reports': gliner_reports}


def main():
    parser = argparse.ArgumentParser(description='Run GLiNER scan on all datasets in groundtruth2')
    parser.add_argument('--skip-existing', action='store_true', help='Skip datasets that already have reports')
    args = parser.parse_args()

    datasets = get_groundtruth_datasets()
    if not datasets:
        print('No datasets found in groundtruth2!')
        return

    output_dir = Path('research/results/gliner')
    output_dir.mkdir(parents=True, exist_ok=True)

    config = get_config()
    data_loader = SmartDataLoader(
        max_rows=None,
        user_agent=config.SDD_USER_AGENT,
        hdx_base_url=config.HDX_URL,
    )
    scanner = GliNERScanner(
        model_name=config.GLINER_MODEL,
        threshold=config.GLINER_THRESHOLD,
        batch_size=config.GLINER_BATCH_SIZE,
    )
    ckan = CKANClient(
        base_url=config.HDX_URL_PROD,
        api_token=config.HDX_KEY_PROD,
        user_agent=config.SDD_USER_AGENT,
    )

    total = len(datasets)
    successful = 0
    failed = 0
    skipped = 0

    print(f'Running GLiNER scan on {total} datasets...')

    for i, dataset_name in enumerate(datasets, 1):
        output_file = output_dir / f'{dataset_name}.json'
        if args.skip_existing and output_file.exists():
            print(f'[{i}/{total}] ⏭️  Skipping {dataset_name} (already exists)')
            skipped += 1
            continue

        print(f'[{i}/{total}] Processing {dataset_name}')
        try:
            report = run_gliner_on_dataset(dataset_name, data_loader, scanner, ckan)
            with output_file.open('w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            print(f'  ✅ Saved report to {output_file}')
            successful += 1
        except Exception as e:
            print(f'  ❌ Failed to process {dataset_name}: {e}')
            failed += 1

    print('\n' + '=' * 50)
    print('GLiNER BATCH PROCESSING COMPLETE')
    print('=' * 50)
    print(f'Total datasets: {total}')
    print(f'✅ Successful: {successful}')
    print(f'⏭️  Skipped: {skipped}')
    print(f'❌ Failed: {failed}')
    print(f'Reports saved to: {output_dir}')
    print('=' * 50)


if __name__ == '__main__':
    main()
