"""main.py: HDX SDD pipeline listener and event processor."""

import logging.config

logging.config.fileConfig('logging.conf')

import json  # noqa: E402
import datetime  # noqa: E402

from hdx_redis_lib import connect_to_hdx_event_bus, RedisConfig  # noqa: E402

from config.config import get_config  # noqa: E402
from models.sdd_report import SDDReport  # noqa: E402
from utils.ckan import CKANClient  # noqa: E402
from utils.processing import DataSampler  # noqa: E402
from classifiers.pii_classifier import PIIClassifier  # noqa: E402
from classifiers.non_pii_classifier import NonPIIClassifier  # noqa: E402
from classifiers.pii_reflection_classifier import PIIReflectionClassifier  # noqa: E402
from classifiers.readme_scan import ReadMeScanClassifier  # noqa: E402
from utils.utils import report_exists_in_ckan, determine_sensitivity  # noqa: E402
from llm_model.azure_strategy import AzureOpenAIStrategy  # noqa: E402
from utils.exception_handler import handle_exception  # noqa: E402
from utils.utils import table_markdown  # noqa: E402

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


def load_isp_info(ckan: CKANClient, package_id: str | None, file_name: str | None) -> dict:
    """Load ISP configuration and determine matching or default ISP."""

    with open('data/isps.json', 'r') as f:
        isps = json.load(f)
    if not package_id or not file_name:
        return {'default': isps.get('default')}

    if package_id:
        package = ckan.package_show(package_id)
        solr_additions = package.get('solr_additions', {})
        logger.info(f'Solr additions: {solr_additions}')

    if isinstance(solr_additions, str):
        solr_additions = json.loads(solr_additions)
    if solr_additions and solr_additions.get('countries'):
        country = solr_additions.get('countries')[0]
        for isp_name, isp_data in isps.items():
            if isp_data.get('country', '').lower() in country.lower():
                return {isp_name: isp_data}
    elif file_name:
        for isp_name, isp_data in isps.items():
            if isp_data.get('country', '').lower() in file_name.lower():
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


@handle_exception
def pii_classification(df):
    return get_classifier(PIIClassifier, config.PII_DETECT_MODEL).classify_df(df)


@handle_exception
def pii_reflection_classification(sdd_report):
    return get_classifier(PIIReflectionClassifier, config.PII_REFLECT_MODEL).classify_df(
        table_markdown(sdd_report), sdd_report.columns
    )


@handle_exception
def non_pii_classification(sdd_report, isp):
    return get_classifier(NonPIIClassifier, config.NON_PII_DETECT_MODEL).classify(
        table_markdown(sdd_report), sdd_report, isp
    )


@handle_exception
def readme_scan_classification(sdd_report):
    return get_classifier(ReadMeScanClassifier, config.README_SCAN_MODEL).classify(
        table_markdown(sdd_report), sdd_report.columns
    )


@handle_exception
def sheet_processor(sdd_report, isp, df):
    sheet_name = sdd_report.sheet_name
    if 'readme' in sheet_name.lower() or 'instructions' in sheet_name.lower() or 'metadata' in sheet_name.lower():
        prediction, comp, prompt = readme_scan_classification(sdd_report)
        sdd_report.readme = prediction
        sdd_report.completion_tokens += comp
        sdd_report.prompt_tokens += prompt
        sdd_report.readme_model = config.README_SCAN_MODEL
        return sdd_report

    # PII classification
    pii_columns, comp, prompt, _ = pii_classification(df)
    sdd_report.columns = pii_columns
    sdd_report.completion_tokens += comp
    sdd_report.prompt_tokens += prompt
    sdd_report.pii_classifier_model = config.PII_DETECT_MODEL

    # PII reflection
    pii_reflections, comp, prompt, _ = pii_reflection_classification(sdd_report)
    sdd_report.columns = pii_reflections
    sdd_report.completion_tokens += comp
    sdd_report.prompt_tokens += prompt
    sdd_report.pii_reflection_model = config.PII_REFLECT_MODEL

    # Non-PII classification
    sdd_report.non_pii = non_pii_classification(sdd_report, isp)
    sdd_report.non_pii_model = config.NON_PII_DETECT_MODEL
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
    isp = load_isp_info(ckan, event.get('package_id'), file_name)

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
