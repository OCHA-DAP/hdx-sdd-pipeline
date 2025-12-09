"""llm_research.py: Offline testing of SDD classifiers for a single LLM."""

import datetime
from models.sdd_report import SDDReport, PIIColumnReport, NonPIIReport


def init_report(df, sheet_name, file_name, download_url, resource_id):
    return SDDReport(
        resource_id=resource_id,
        file_name=file_name,
        file_url=download_url,
        sheet_name=sheet_name,
        processing_timestamp=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        processing_success=True,
        n_records=len(df),
        n_columns=len(df.columns),
    )


def process_sheet_for_groundtruth(df, sheet_name, file_name, download_url, resource_id, isp):
    """Create empty ground truth SDDReport (no LLM classification)."""
    report = init_report(df, sheet_name, file_name, download_url, resource_id)

    # Add empty PII column reports for all columns
    for col in df.columns:
        column_report = PIIColumnReport(
            column_name=col,
            sample_values=df[col].dropna().astype(str).head(3).tolist(),  # small sample
            pii={'entity_type': 'unknown', 'sensitive': False},
        )
        report.add_pii_column(column_report)

    # Add empty non-PII report
    non_pii_report = NonPIIReport(
        model_name='none',
        isp_used=list(isp.keys())[0],
        sensitivity='none',
        sensitive_columns=[],
        cited_isp_rules=[],
        explanation='Empty groundtruth; no classification run.',
    )
    report.add_non_pii_report(non_pii_report)

    return report.to_dict()
