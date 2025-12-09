import logging
import pandas as pd
from models.sdd_report import SDDReport
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
    pii = any(r.get('pii_sensitive') for r in reports)
    non_pii = any(r.get('non_pii_sensitive') for r in reports)

    if pii and non_pii:
        return 'sensitive-pii-and-non-pii'
    if pii:
        return 'sensitive-pii'
    if non_pii:
        return 'sensitive-non-pii'
    return 'not-sensitive'


@handle_exception_wrap()
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

    if not column_samples:
        return ''

    max_len = max(len(values) for values in column_samples.values())
    for key, values in column_samples.items():
        column_samples[key] = values + [''] * (max_len - len(values))

    return pd.DataFrame(column_samples).to_markdown(index=False) or ''
