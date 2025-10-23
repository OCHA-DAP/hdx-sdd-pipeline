"""main-sdd.py: Main script for the SDD Pipeline."""

import dotenv
import os
from datetime import datetime

from utils.ckan import CKANClient
from utils.processing import DataSampler
from classifiers.pii_classifier import PIIClassifier
from classifiers.non_pii_classifier import NonPIIClassifier
from classifiers.pii_reflection_classifier import PIIReflectionClassifier
import pandas as pd

from utils.main_config import ISP_DEFAULT
from models.sdd_report import SDDReport
import logging



logger = logging.getLogger(__name__)

dotenv.load_dotenv()

# ===== Configuration =====
CKAN_URL = os.getenv('CKAN_URL')
CKAN_API_TOKEN = os.getenv('CKAN_API_TOKEN')

ckan = CKANClient(base_url=CKAN_URL, api_token=CKAN_API_TOKEN)

# ===== Fetch resource metadata =====
RESOURCE_ID = '4ef001d1-7888-4f5d-98ce-0ca8006787f7'
resource = ckan.resource_show(RESOURCE_ID)
download_url = resource.get('download_url')
file_name = resource.get('name', 'unknown_dataset.csv')

OUTPUT_DIR = 'reports'
os.makedirs(OUTPUT_DIR, exist_ok=True)
output_path = os.path.join(OUTPUT_DIR, f'{file_name}_sdd_report.json')

# Check if report file already exists
if os.path.exists(output_path):
    print(f'[INFO] Report already exists at {output_path}')

    # Load an existing report
    with open(output_path, 'r', encoding='utf-8') as f:
        report = SDDReport.from_json(f.read())

else:
    report = None
# ===== Preprocessing & Sampling =====
sampler = DataSampler()
df = sampler.sample_from_url(download_url)
print('[INFO] Sampled data:')
print(df.head(), '\n')

# Alternatively, create a new report
if report is None:
    report = SDDReport(
        resource_id=RESOURCE_ID,
        file_name=file_name,
        file_url=download_url,
        processing_timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        processing_success=True,
        n_records=len(df),
        n_columns=len(df.columns),
    )

# ===== PII Detection =====
pii_detector = PIIClassifier(model_name='gpt-4.1-nano')
print('[INFO] Starting PII Detection...\n')

report = pii_detector.classify_df(df=df, report=report)

# ===== PII Reflection Detection =====
pii_reflection_classifier = PIIReflectionClassifier(model_name='gpt-4.1-nano')


def table_markdown(report: SDDReport):
    columns_data = report.columns
    column_samples = {}

    for column in columns_data:
        if column.pii.get('entity_type') != 'None':
            column_key = column.column_name + ' - ' + column.pii.get('entity_type')
            column_samples[column_key] = column.sample_values
        else:
            column_samples[column.column_name] = column.sample_values

    table_data = pd.DataFrame(column_samples)
    return table_data.to_markdown()


report = pii_reflection_classifier.classify_df(
    table_markdown=table_markdown(report),
    report=report,
)

# ===== (Optional) Non-PII Classification =====
non_pii_classifier = NonPIIClassifier(model_name='gpt-4.1-nano')
report = non_pii_classifier.classify(
    table_markdown=table_markdown(report),
    report=report,
    isp=ISP_DEFAULT,
)

# ===== Save report =====


with open(output_path, 'w', encoding='utf-8') as f:
    f.write(report.to_json(indent=2))

print(f'[INFO] Report saved to {output_path}')

