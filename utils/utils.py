import logging
import pandas as pd
from typing import Dict, Any
from utils.exception_handler import handle_exception_wrap

logger = logging.getLogger(__name__)


@handle_exception_wrap()
def report_exists_in_ckan(ckan, resource_id):
    """Check if a report exists in CKAN."""
    resource = ckan.resource_show(resource_id)
    return resource.get('sdd_report') is not None


@handle_exception_wrap()
def determine_sensitivity(reports: list) -> str:
    """Determine overall sensitivity from sheet-level reports."""
    personal_data = any(r.get('personal_data_sensitive') for r in reports)
    non_personal_data = any(r.get('non_personal_data_sensitive') for r in reports)

    if personal_data and non_personal_data:
        return 'sensitive-personal-data-and-non-personal-data'
    if personal_data:
        return 'sensitive-personal-data'
    if non_personal_data:
        return 'sensitive-non-personal-data'
    return 'not-sensitive'


@handle_exception_wrap()
def table_markdown(report: Dict[str, Any]) -> str:
    """Generate a markdown table from the report sample columns."""
    column_samples = {}
    for col in report['columns']:
        key = (
            f'{col["column_name"]} - {col["personal_data"].get("entity_type", "None")}'
            if col['personal_data'].get('entity_type') != 'None'
            else col['column_name']
        )
        column_samples[key] = col['sample_values']

    max_len = max(len(values) for values in column_samples.values())
    for key, values in column_samples.items():
        column_samples[key] = values + [''] * (max_len - len(values))

    return pd.DataFrame(column_samples).to_markdown(index=False) or ''
