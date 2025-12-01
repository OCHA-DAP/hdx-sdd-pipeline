"""main.py: HDX SDD pipeline listener and event processor."""

import json
import logging.config
import datetime

from hdx_redis_lib import connect_to_hdx_event_bus, RedisConfig

from config.config import get_config
from models.sdd_report import SDDReport
from utils.ckan import CKANClient
from utils.exception_handler import handle_exception_wrap
from utils.processing import DataSampler
from utils.utils import report_exists_in_ckan, determine_sensitivity, table_markdown
from classifiers.pii_classifier import PIIClassifier
from classifiers.non_pii_classifier import NonPIIClassifier
from classifiers.pii_reflection_classifier import PIIReflectionClassifier
from classifiers.readme_scan import ReadMeScanClassifier
from llm_model.azure_strategy import AzureOpenAIStrategy


logging.config.fileConfig('logging.dev.conf')


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


def get_dataset_location(ckan: CKANClient, package_id: str | None) -> str | None:
    if not package_id:
        return None
    package = ckan.package_show(package_id)
    solr_additions = package.get('solr_additions', {})
    if isinstance(solr_additions, str):
        solr_additions = json.loads(solr_additions)
    return solr_additions.get('countries', [None])[0] or ''


def get_isp(string: str) -> dict:
    """Load ISP configuration and determine matching or default ISP."""

    with open('data/isps.json', 'r', encoding='utf-8') as f:
        isps = json.load(f)

    if not string or string.strip() == '':
        return {'default': isps.get('default')}

    for isp_name, isp_data in isps.items():
        if isp_data.get('country', '').lower() in string.lower():
            return {isp_name: isp_data}

    return {'default': isps.get('default')}


def get_classifier(classifier_cls, model_name):
    return classifier_cls(
        model=AzureOpenAIStrategy(
            model_name=model_name,
            azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
            api_key=config.AZURE_OPENAI_API_KEY,
        )
    )


@handle_exception_wrap
def pii_classification(df, model=None):
    if model:
        classifier = get_classifier(PIIClassifier, model)
    else:
        classifier = get_classifier(PIIClassifier, config.PII_DETECT_MODEL)
    return classifier.classify_df(df)


@handle_exception_wrap
def pii_reflection_classification(sdd_report, model=None):
    if model:
        classifier = get_classifier(PIIReflectionClassifier, model)
    else:
        classifier = get_classifier(PIIReflectionClassifier, config.PII_REFLECT_MODEL)
    return classifier.classify_df(table_markdown(sdd_report), sdd_report.columns)


@handle_exception_wrap
def non_pii_classification(sdd_report, isp, model=None):
    if model:
        classifier = get_classifier(NonPIIClassifier, model)
    else:
        classifier = get_classifier(NonPIIClassifier, config.NON_PII_DETECT_MODEL)
    return classifier.classify(table_markdown(sdd_report), sdd_report, isp)


@handle_exception_wrap
def readme_scan_classification(sdd_report, model=None):
    if model:
        classifier = get_classifier(ReadMeScanClassifier, model)
    else:
        classifier = get_classifier(ReadMeScanClassifier, config.README_SCAN_MODEL)
    return classifier.classify(table_markdown(sdd_report))


@handle_exception_wrap
def sheet_processor(sdd_report, isp, df, model=None):
    sheet_name = sdd_report.sheet_name
    if 'readme' in sheet_name.lower() or 'instructions' in sheet_name.lower() or 'metadata' in sheet_name.lower():
        prediction, comp, prompt = readme_scan_classification(sdd_report, model=model)
        sdd_report.readme = prediction
        sdd_report.completion_tokens += comp
        sdd_report.prompt_tokens += prompt
        sdd_report.readme_model = model if model else config.README_SCAN_MODEL
        return sdd_report

    # PII classification
    pii_columns, comp, prompt, _ = pii_classification(df, model=model)
    sdd_report.columns = pii_columns
    sdd_report.completion_tokens += comp
    sdd_report.prompt_tokens += prompt
    sdd_report.pii_classifier_model = model if model else config.PII_DETECT_MODEL

    # PII reflection
    pii_reflections, comp, prompt, _ = pii_reflection_classification(sdd_report, model=model)
    sdd_report.columns = pii_reflections
    sdd_report.completion_tokens += comp
    sdd_report.prompt_tokens += prompt
    sdd_report.pii_reflection_model = model if model else config.PII_REFLECT_MODEL

    # Non-PII classification
    sdd_report.non_pii = non_pii_classification(sdd_report, isp, model=model)
    sdd_report.non_pii_model = model if model else config.NON_PII_DETECT_MODEL
    return sdd_report


def event_processor(event):
    """Main event processor. Handles one HDX resource-data-changed event."""
    logger.info('Received event: %s', json.dumps(event, ensure_ascii=False, indent=2))

    resource_id = event.get('resource_id')
    if not resource_id:
        logger.error('Missing resource_id in event.')
        return False, 'Missing resource_id'

    ckan = CKANClient(base_url=config.HDX_URL, api_token=config.HDX_KEY)

    resource = ckan.resource_show(resource_id)

    if report_exists_in_ckan(ckan, resource_id):
        logger.info('SDD report already exists. Skipping.')
        return True, 'Already processed'

    download_url = resource.get('download_url')
    file_name = resource.get('name', 'unknown_dataset.csv')
    dataset_location = get_dataset_location(ckan, event.get('package_id'))
    isp = get_isp(dataset_location)
    # If key is default then try with filename
    if 'default' in isp:
        isp = get_isp(file_name)
        if 'default' in isp:
            logger.info('No ISP found for dataset location or filename.')

    sampler = DataSampler()
    dfs = sampler.sample(download_url)

    reports = []

    for sheet_name, df in dfs.items():
        sdd_report = SDDReport(
            resource_id=resource_id,
            file_name=file_name,
            file_url=download_url,
            sheet_name=sheet_name,
            processing_timestamp=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            processing_success=True,
            n_records=len(df),
            n_columns=len(df.columns),
        )
        try:
            sdd_report = sheet_processor(sdd_report, isp, df)
            reports.append(sdd_report.to_dict())

        except Exception as e:
            logger.error(e)
            sdd_report.processing_success = False
            sdd_report.error_message = str(e)
            reports.append(sdd_report.to_dict())

    sensitivity = determine_sensitivity(reports)

    # Directly update CKAN (no file saving)
    ckan.update_resource_fields(
        resource_id,
        {'sdd_report': json.dumps(reports, indent=2), 'sensitive': sensitivity},
    )
    return True, f'Processed successfully ({sensitivity})'


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
