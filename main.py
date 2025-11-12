"""main-sdd.py: Main script for the SDD Pipeline (refactored)."""

import os
import sys
import json
import logging
from datetime import datetime

import dotenv
import pandas as pd

from models.sdd_report import SDDReport
from utils.main_config import RERUN, PII_DETECT_MODEL, PII_REFLECT_MODEL, NON_PII_DETECT_MODEL, README_SCAN_MODEL
from utils.ckan import CKANClient
from utils.processing import DataSampler
from classifiers.pii_classifier import PIIClassifier
from classifiers.non_pii_classifier import NonPIIClassifier
from classifiers.pii_reflection_classifier import PIIReflectionClassifier
from classifiers.readme_scan import ReadMeScanClassifier
import logging
import logging.config
from hdx_redis_lib import connect_to_hdx_event_bus, RedisConfig

logging.config.fileConfig('logging.conf')

logger = logging.getLogger(__name__)

stream_name = os.getenv('REDIS_STREAM_STREAM_NAME', 'hdx_event_stream')
group_name = os.getenv('REDIS_STREAM_GROUP_NAME', 'hdx_sdd_group')
consumer_name = os.getenv('REDIS_STREAM_CONSUMER_NAME', 'hdx_sdd_consumer_1')
redis_stream_host = os.getenv('REDIS_STREAM_HOST', 'redis')
redis_stream_port = os.getenv('REDIS_STREAM_PORT', 6379)
redis_stream_db = os.getenv('REDIS_STREAM_DB', 7)

event_bus = connect_to_hdx_event_bus(
    stream_name,
    group_name,
    consumer_name,
    RedisConfig(host=redis_stream_host, db=redis_stream_db, port=redis_stream_port),
)


def event_processor(event):
    # Process the event (this is just a placeholder)
    logger.info('Handling event: %s', event)
    return True, 'Success'


def load_isp_info(file_name: str) -> dict:
    """Load ISP configuration and determine matching or default ISP."""
    with open('data/isps.json', 'r') as f:
        isps = json.load(f)

    for isp_name, isp_data in isps.items():
        if isp_data.get('country', '').lower() in file_name.lower():
            return {isp_name: isp_data}

    return {'default': isps.get('default')}


def table_markdown(report: SDDReport) -> str:
    """Generate a markdown table from the report sample columns."""
    column_samples = {}

    for col in report.columns:
        key = (
            f"{col.column_name} - {col.pii.get('entity_type')}"
            if col.pii.get('entity_type') != 'None'
            else col.column_name
        )
        column_samples[key] = col.sample_values

    max_len = max(len(values) for values in column_samples.values())
    for key, values in column_samples.items():
        column_samples[key] = values + [''] * (max_len - len(values))

    return pd.DataFrame(column_samples).to_markdown(index=False) or ''


def process_sheet(df, sheet_name, file_name, download_url, resource_id, isp, logger):
    """Process a single sheet: PII, Reflection, Non-PII classification."""
    logger.info('Processing sheet: %s', sheet_name)

    report = SDDReport(
        resource_id=resource_id,
        file_name=file_name,
        file_url=download_url,
        sheet_name=sheet_name,
        processing_timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        processing_success=True,
        n_records=len(df),
        n_columns=len(df.columns),
    )

    if report.pii_classifier_model is None:
        logger.info('Running PII detection...')
        report = PIIClassifier(model_name=PII_DETECT_MODEL).classify_df(df, report)

    if report.pii_reflection_model is None:
        logger.info('Running PII Reflection detection...')
        report = PIIReflectionClassifier(model_name=PII_REFLECT_MODEL).classify_df(table_markdown(report), report)

    if report.non_pii is None:
        logger.info('Running Non-PII classification...')
        report = NonPIIClassifier(model_name=NON_PII_DETECT_MODEL).classify(table_markdown(report), report, isp)

    return report.to_dict()


def determine_sensitivity(reports: list) -> str:
    """Determine overall sensitivity from sheet-level reports."""
    for r in reports:
        if r.get('pii_sensitive') and r.get('non_pii_sensitive'):
            return 'sensitive-pii-and-non-pii'
    for r in reports:
        if r.get('pii_sensitive') and not r.get('non_pii_sensitive'):
            return 'sensitive-pii'
    for r in reports:
        if r.get('non_pii_sensitive') and not r.get('pii_sensitive'):
            return 'sensitive-non-pii'
    return 'not-sensitive'


def main():
    event_bus.hdx_listen(event_processor, allowed_event_types={'resource-data-changed'}, max_iterations=10_000)

    dotenv.load_dotenv()

    # === CKAN setup ===
    ckan = CKANClient(
        base_url=os.getenv('HDX_URL'),
        api_token=os.getenv('HDX_KEY'),
    )

    RESOURCE_ID = 'e031354c-cd95-471b-a7f4-1a87d30981f7'
    resource = ckan.resource_show(RESOURCE_ID)

    if resource is None:
        logger.error('Resource %s not found', RESOURCE_ID)
        sys.exit(1)

    download_url = resource.get('download_url')
    file_name = resource.get('name', 'unknown_dataset.csv')

    if resource.get('sdd_report') and RERUN is False:
        logger.info('SDD Report already exists. Exiting.')
        sys.exit(1)

    isp = load_isp_info(file_name)

    sampler = DataSampler()
    dfs = sampler.sample_from_url(download_url)

    output_dir = 'reports'
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f'{file_name}_sdd_report.json')

    reports = []
    for sheet_name, df in dfs.items():
        if (
            'readme' in sheet_name.lower()
            or 'instrucciones' in sheet_name.lower()
            or 'instructions' in sheet_name.lower()
            or 'metadata' in sheet_name.lower()
        ):
            readme_string = df.to_string()
            report, completion_tokens, prompt_tokens = ReadMeScanClassifier(
                model_name=README_SCAN_MODEL
            ).classify_readme(readme_string)
            reports.append(
                {
                    'sheet_name': sheet_name,
                    # 'readme_string': readme_string,
                    'completion_tokens': completion_tokens,
                    'prompt_tokens': prompt_tokens,
                    'pii_sensitive': report.get('contains_pii', False),
                    'report': report,
                }
            )
            print(f'Readme/instructions sheet: {sheet_name} - {report}')
            continue
        else:
            reports.append(process_sheet(df, sheet_name, file_name, download_url, RESOURCE_ID, isp, logger))

    sensitivity = determine_sensitivity(reports)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(reports, f, indent=2)

    ckan.update_resource_fields(
        RESOURCE_ID,
        {'sdd_report': json.dumps(reports, indent=2), 'sensitive': sensitivity},
    )

    logger.info('Report updated in CKAN (sensitive = %s)', sensitivity)
    print(f'Report updated in CKAN and set sensitive to: {sensitivity}')


if __name__ == '__main__':
    main()
