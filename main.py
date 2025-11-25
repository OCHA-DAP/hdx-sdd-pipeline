"""main.py: HDX SDD pipeline listener and event processor."""

import logging.config
logging.config.fileConfig('logging.conf')

import json
import datetime
import pandas as pd
from hdx_redis_lib import connect_to_hdx_event_bus, RedisConfig
from config.config import get_config
from models.sdd_report import SDDReport
from utils.ckan import CKANClient
from utils.processing import DataSampler
from classifiers.pii_classifier import PIIClassifier
from classifiers.non_pii_classifier import NonPIIClassifier
from classifiers.pii_reflection_classifier import PIIReflectionClassifier
from classifiers.readme_scan import ReadMeScanClassifier

# logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)

config = get_config()

event_bus = connect_to_hdx_event_bus(
    config.REDIS_STREAM_STREAM_NAME,
    config.REDIS_STREAM_GROUP_NAME,
    config.REDIS_STREAM_CONSUMER_NAME,
    RedisConfig(
        host=config.REDIS_STREAM_HOST,
        db=config.REDIS_STREAM_DB,
        port=config.REDIS_STREAM_PORT,
    ),
)


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
            f'{col.column_name} - {col.pii.get("entity_type", "None")}'
            if col.pii.get('entity_type') != 'None'
            else col.column_name
        )
        column_samples[key] = col.sample_values

    max_len = max(len(values) for values in column_samples.values())
    for key, values in column_samples.items():
        column_samples[key] = values + [''] * (max_len - len(values))

    return pd.DataFrame(column_samples).to_markdown(index=False) or ''


def process_sheet(df, sheet_name, file_name, download_url, resource_id, isp):
    """Process a single sheet: PII, Reflection, and Non-PII classification."""
    logger.info('Processing sheet: %s', sheet_name)

    report = SDDReport(
        resource_id=resource_id,
        file_name=file_name,
        file_url=download_url,
        sheet_name=sheet_name,
        processing_timestamp=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        processing_success=True,
        n_records=len(df),
        n_columns=len(df.columns),
    )

    report = PIIClassifier(
        model_name=config.PII_DETECT_MODEL,
        azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
        api_key=config.AZURE_OPENAI_API_KEY,
    ).classify_df(df, report)
    report = PIIReflectionClassifier(
        model_name=config.PII_REFLECT_MODEL,
        azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
        api_key=config.AZURE_OPENAI_API_KEY,
    ).classify_df(table_markdown(report), report)
    report = NonPIIClassifier(
        model_name=config.NON_PII_DETECT_MODEL,
        azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
        api_key=config.AZURE_OPENAI_API_KEY,
    ).classify(table_markdown(report), report, isp)

    return report.to_dict()


def determine_sensitivity(reports: list) -> str:
    """Determine overall sensitivity from sheet-level reports."""
    pii_sensitive = [r.get('pii_sensitive') for r in reports]
    non_pii_sensitive = [r.get('non_pii_sensitive') for r in reports]

    if any(pii_sensitive) and any(non_pii_sensitive):
        return 'sensitive-pii-and-non-pii'
    if any(pii_sensitive) and not any(non_pii_sensitive):
        return 'sensitive-pii'
    if not any(pii_sensitive) and any(non_pii_sensitive):
        return 'sensitive-non-pii'
    return 'not-sensitive'


def event_processor(event):
    """Main event processor. Handles one HDX resource-data-changed event."""
    logger.info('Received event: %s', json.dumps(event, ensure_ascii=False, indent=2))
    start_time = datetime.datetime.now()

    resource_id = event.get('resource_id')
    if not resource_id:
        logger.error('Missing resource_id in event.')
        return False, 'Missing resource_id'

    ckan = CKANClient(base_url=config.HDX_URL, api_token=config.HDX_KEY)

    try:
        resource = ckan.resource_show(resource_id)
        if not resource:
            logger.error('Resource %s not found', resource_id)
            return False, 'Resource not found'

        if resource.get('sdd_report') and not config.RERUN:
            logger.info('SDD report already exists. Skipping.')
            return True, 'Already processed'

        download_url = resource.get('download_url')
        file_name = resource.get('name', 'unknown_dataset.csv')
        isp = load_isp_info(file_name)

        sampler = DataSampler()
        dfs = sampler.sample(download_url)

        reports = []
        for sheet_name, df in dfs.items():
            if any(k in sheet_name.lower() for k in ['readme', 'instrucciones', 'instructions', 'metadata']):
                readme_string = df.to_string()
                report, completion_tokens, prompt_tokens = ReadMeScanClassifier(
                    model_name=config.README_SCAN_MODEL
                ).classify_readme(readme_string)
                reports.append(
                    {
                        'sheet_name': sheet_name,
                        'completion_tokens': completion_tokens,
                        'prompt_tokens': prompt_tokens,
                        'pii_sensitive': report.get('contains_pii', False),
                        'report': report,
                    }
                )
            else:
                reports.append(process_sheet(df, sheet_name, file_name, download_url, resource_id, isp))

        sensitivity = determine_sensitivity(reports)

        # Directly update CKAN (no file saving)
        ckan.update_resource_fields(
            resource_id,
            {'sdd_report': json.dumps(reports, indent=2), 'sensitive': sensitivity},
        )

        elapsed = datetime.datetime.now() - start_time
        logger.info(
            f'Finished processing resource {resource_id} ({file_name}) in {elapsed}. Sensitivity: {sensitivity}'
        )

        return True, f'Processed successfully ({sensitivity})'

    except Exception as e:
        logger.exception('Error processing resource %s: %s', resource_id, e)
        return False, str(e)


if __name__ == '__main__':
    if not config.WORKER_ENABLED:
        logger.info('WORKER_ENABLED is false. Sleeping indefinitely...')
        from time import sleep

        while True:
            sleep(3600)
    else:
        event_bus.hdx_listen(
            event_processor,
            allowed_event_types={'resource-created', 'resource-data-changed'},
            max_iterations=10_000,
        )
