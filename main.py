"""main.py: HDX SDD pipeline listener and event processor."""

import json
import logging.config
from hdx_redis_lib import connect_to_hdx_event_bus, RedisConfig

from config.config import get_config
from utils.ckan import CKANClient
from utils.error_constants import (
    ERROR_SOURCE_PII_CLASSIFICATION,
    ERROR_SOURCE_PII_REFLECTION,
    ERROR_SOURCE_NON_PII_CLASSIFICATION,
    ERROR_SOURCE_README_SCAN,
)
from utils.exception_handler import handle_exception_wrap
from utils.processing import create_report
from utils.utils import report_exists_in_ckan, determine_sensitivity, table_markdown
from classifiers.pii_classifier import PIIClassifier
from classifiers.non_pii_classifier import NonPIIClassifier
from classifiers.pii_reflection_classifier import PIIReflectionClassifier
from classifiers.readme_scan import ReadMeScanClassifier
from llm_model.azure_strategy import AzureOpenAIStrategy

logging.config.fileConfig('logging.conf')

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
    return solr_additions.get('countries', None)


def get_isp(input_location: str | list | None) -> dict:
    """Load ISP configuration and determine matching or default ISP."""

    with open('data/isps.json', 'r', encoding='utf-8') as f:
        isps = json.load(f)

    if not input_location:
        return {'default': isps.get('default')}

    if isinstance(input_location, str):
        if input_location.strip() == '':
            return {'default': isps.get('default')}
        input_location = [input_location]
    for location in input_location:
        for isp_name, isp_data in isps.items():
            if isp_data.get('country', '').lower() in location.lower():
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


@handle_exception_wrap()
def pii_classification(sdd_report, model=None):
    if sdd_report.get('pii_classifier_model'):
        logger.info(f'PII classifier model already set to {sdd_report.get("pii_classifier_model")}. Skipping.')
        return sdd_report
    if model:
        classifier = get_classifier(PIIClassifier, model)
    else:
        classifier = get_classifier(PIIClassifier, config.PII_DETECT_MODEL)
    return classifier.classify_df(sdd_report)


@handle_exception_wrap()
def pii_reflection_classification(sdd_report, model=None):
    if sdd_report.get('pii_reflection_model'):
        logger.info(f'PII reflection model already set to {sdd_report.get("pii_reflection_model")}. Skipping.')
        return sdd_report
    if model:
        classifier = get_classifier(PIIReflectionClassifier, model)
    else:
        classifier = get_classifier(PIIReflectionClassifier, config.PII_REFLECT_MODEL)
    return classifier.classify_df(sdd_report)


@handle_exception_wrap()
def non_pii_classification(sdd_report, isp, model=None):
    if sdd_report.get('non_pii_model'):
        logger.info(f'Non-PII model already set to {sdd_report.get("non_pii_model")}. Skipping.')
        return sdd_report
    if model:
        classifier = get_classifier(NonPIIClassifier, model)
    else:
        classifier = get_classifier(NonPIIClassifier, config.NON_PII_DETECT_MODEL)
    return classifier.classify(sdd_report, isp)


@handle_exception_wrap()
def readme_scan_classification(sdd_report, model=None):
    if model:
        classifier = get_classifier(ReadMeScanClassifier, model)
    else:
        classifier = get_classifier(ReadMeScanClassifier, config.README_SCAN_MODEL)
    return classifier.classify(table_markdown(sdd_report))


@handle_exception_wrap()
def sheet_processor(sdd_report, isp, model=None):
    sheet_name = sdd_report['sheet_name']
    if 'readme' in sheet_name.lower() or 'instructions' in sheet_name.lower() or 'metadata' in sheet_name.lower():
        try:
            prediction, comp, prompt = readme_scan_classification(sdd_report, model=model)
            sdd_report.readme = prediction
            sdd_report.completion_tokens += comp
            sdd_report.prompt_tokens += prompt
            sdd_report.readme_model = model if model else config.README_SCAN_MODEL
            return sdd_report
        except Exception as e:
            logger.error(f'Error in README scan classification: {e}')
            sdd_report['error_source'] = ERROR_SOURCE_README_SCAN
            sdd_report['error_message'] = str(e)
            return sdd_report

    # PII classification
    try:
        sdd_report = pii_classification(sdd_report, model=model)
    except Exception as e:
        sdd_report['error_source'] = ERROR_SOURCE_PII_CLASSIFICATION
        sdd_report['error_message'] = str(e)
        return sdd_report

    # PII reflection
    try:
        sdd_report = pii_reflection_classification(sdd_report, model=model)
    except Exception as e:
        sdd_report['error_source'] = ERROR_SOURCE_PII_REFLECTION
        sdd_report['error_message'] = str(e)
        return sdd_report

    # Check if any column is sensitive
    if any(column['pii'].get('sensitive', True) for column in sdd_report['columns']):
        sdd_report['pii_sensitive'] = True

    # Non-PII classification
    try:
        sdd_report = non_pii_classification(sdd_report, isp, model=model)
        print(f'Non-PII classification: {sdd_report}')
    except Exception as e:
        sdd_report['error_source'] = ERROR_SOURCE_NON_PII_CLASSIFICATION
        sdd_report['error_message'] = str(e)
        return sdd_report

    # Check if any column is sensitive
    if sdd_report['non_pii']['sensitivity'].lower() in [
        'high',
        'high_sensitive',
        'moderate',
        'moderate_sensitive',
        'severe',
        'severe_sensitive',
    ]:
        sdd_report['non_pii_sensitive'] = True

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

    sdd_reports = create_report(download_url)
    logger.debug(f'SDD report: {sdd_reports}')

    for sdd_report in sdd_reports:
        sdd_report = sheet_processor(sdd_report, isp)
        # If error_source is not None then set processing_success to False
        if sdd_report.get('error_source', None):
            sdd_report['processing_success'] = False

    sensitivity = determine_sensitivity(sdd_reports)

    # Directly update CKAN (no file saving)
    ckan.update_resource_fields(
        resource_id,
        {'sdd_report': json.dumps(sdd_reports, indent=2), 'sensitive': sensitivity},
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
