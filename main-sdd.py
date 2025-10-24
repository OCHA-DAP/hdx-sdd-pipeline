"""main-sdd.py: Main script for the SDD Pipeline."""

import dotenv
import os
from datetime import datetime
from pathlib import Path
from utils.ckan import CKANClient
from utils.processing import DataSampler
from classifiers.pii_classifier import PIIClassifier
from classifiers.non_pii_classifier import NonPIIClassifier
from classifiers.pii_reflection_classifier import PIIReflectionClassifier
import pandas as pd
import json
from utils.main_config import ISP_DEFAULT
from models.sdd_report import SDDReport
import logging
import logging.config


def table_markdown(report: SDDReport):
    """Generate a markdown table from the report."""
    columns_data = report.columns
    column_samples = {}

    # Build dict of column -> list of values
    for column in columns_data:
        if column.pii.get('entity_type') != 'None':
            column_key = f"{column.column_name} - {column.pii.get('entity_type')}"
        else:
            column_key = column.column_name
        column_samples[column_key] = column.sample_values

    # Find the maximum number of samples among all columns
    max_len = max(len(v) for v in column_samples.values())

    # Pad shorter columns with empty strings
    for key, values in column_samples.items():
        if len(values) < max_len:
            column_samples[key] = values + [''] * (max_len - len(values))

    # Create DataFrame and return as markdown
    table_data = pd.DataFrame(column_samples)
    return table_data.to_markdown(index=False)


if __name__ == '__main__':
    # ===== Logging =====
    if Path('logging.conf').exists():
        logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
    else:
        logging.basicConfig(
            filename='logs/sdd.log', level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        )

    logger = logging.getLogger(__name__)
    logger.propagate = True

    # ===== Environment Variables =====
    dotenv.load_dotenv()

    # ===== CKAN Client =====
    CKAN_URL = os.getenv('CKAN_URL')
    CKAN_API_TOKEN = os.getenv('CKAN_API_TOKEN')

    ckan_logger = logger.getChild('ckan')
    ckan = CKANClient(base_url=CKAN_URL, api_token=CKAN_API_TOKEN, logger=ckan_logger)

    # ===== Fetch resource metadata =====
    RESOURCE_ID = 'ffedbcea-4a02-46b4-b5e2-e2cde32627e8'
    resource = ckan.resource_show(RESOURCE_ID)
    download_url = resource.get('download_url')
    file_name = resource.get('name', 'unknown_dataset.csv')

    OUTPUT_DIR = 'reports'
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, f'{file_name}_sdd_report.json')

    # Check if sdd_report is already in the resource
    # if resource.get('sdd_report'):
    #     logger.info('SDD Report already exists in the resource')
    #     report = SDDReport.from_json(resource.get('sdd_report'))
    #     sys.exit(1)
    # else:
    #     logger.info('SDD Report does not exist in the resource')

    # # Check if report file already exists
    # if os.path.exists(output_path):
    #     logger.info('Report already exists at %s', output_path)

    #     # Load an existing report
    #     with open(output_path, 'r', encoding='utf-8') as f:
    #         report = SDDReport.from_json(f.read())

    # ===== Preprocessing & Sampling =====
    sampler = DataSampler()
    dfs_by_sheet = sampler.sample_from_url(download_url)  # returns dict: sheet_name -> df

    reports = []  # List to hold multiple reports if needed
    output_path = os.path.join(OUTPUT_DIR, f'{file_name}_sdd_report.json')

    for sheet_name, df in dfs_by_sheet.items():
        if 'readme' in sheet_name.lower():
            logger.info('Skipping readme sheet')
            continue
        logger.info('Processing sheet: %s', sheet_name)

        report = SDDReport(
            resource_id=RESOURCE_ID,
            file_name=file_name,
            file_url=download_url,
            sheet_name=sheet_name,
            processing_timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            processing_success=True,
            n_records=len(df),
            n_columns=len(df.columns),
        )

        # ===== PII Detection =====
        if report.pii_classifier_model is None:
            logger.info("Starting PII Detection for sheet '%s'...", sheet_name)
            pii_detector = PIIClassifier(model_name='gpt-4.1-nano')
            report = pii_detector.classify_df(df=df, report=report)
        else:
            logger.info("PII Detection already performed, skipping sheet '%s'", sheet_name)

        # ===== PII Reflection Detection =====
        if report.pii_reflection_model is None:
            logger.info("Starting PII Reflection Detection for sheet '%s'...", sheet_name)
            pii_reflection_classifier = PIIReflectionClassifier(model_name='gpt-4.1-nano')
            report = pii_reflection_classifier.classify_df(table_markdown=table_markdown(report), report=report)
        else:
            logger.info("PII Reflection Detection already performed, skipping sheet '%s'", sheet_name)

        # ===== Non-PII Classification =====
        if report.non_pii is None:
            print(f'NON-PII CLASSIFICATION PERFORMED FOR {sheet_name}')
            logger.info("Starting Non-PII Classification for sheet '%s'...", sheet_name)
            non_pii_classifier = NonPIIClassifier(model_name='gpt-4.1-nano')
            report = non_pii_classifier.classify(table_markdown=table_markdown(report), report=report, isp=ISP_DEFAULT)
        else:
            print(f'NON-PII CLASSIFICATION ALREADY PERFORMED FOR {sheet_name}')
            logger.info("Non-PII Classification already performed, skipping sheet '%s'", sheet_name)
        reports.append(report.to_dict())

    SENSITIVE = False
    for report in reports:
        if report.get('pii_sensitive') == True:
            SENSITIVE = True
            break
    for report in reports:
        if report.get('non_pii_sensitive') == True:
            SENSITIVE = True
            break

    # ===== Save report =====
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps(reports, indent=2))
    logger.info("Report saved for sheet '%s' at %s", sheet_name, output_path)
    print(f'Report saved for sheet {sheet_name} at {output_path}')
    # Append to list if you want to keep track of all sheet reports

    ckan.update_resource_fields(RESOURCE_ID, {'sdd_report': json.dumps(reports, indent=2), 'sensitive': SENSITIVE})
    print(f'Report updated in CKAN and set sensitive to: {SENSITIVE}')

    logger.info('Report updated in CKAN and set sensitive to: %s', SENSITIVE)
