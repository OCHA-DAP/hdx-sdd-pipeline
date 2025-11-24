"""llm_research.py: Offline testing of SDD classifiers for a single LLM."""

import json
import os
import datetime
from pathlib import Path
import pandas as pd
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score  # type: ignore

from config.config import get_config
from models.sdd_report import SDDReport, PIIColumnReport, NonPIIReport
from utils.processing import table_markdown
from classifiers.pii_classifier import PIIClassifier
from classifiers.non_pii_classifier import NonPIIClassifier
from classifiers.pii_reflection_classifier import PIIReflectionClassifier

config = get_config()


def load_isp_info(file_name: str) -> dict:
    """Load ISP configuration and determine matching or default ISP."""
    with open('data/isps.json', 'r') as f:
        isps = json.load(f)
    for isp_name, isp_data in isps.items():
        if isp_data.get('country', '').lower() in file_name.lower():
            return {isp_name: isp_data}
    return {'default': isps.get('default')}


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


def load_test_data(test_file: str):
    """Load CSV or Excel test datasets. Returns dict of sheet_name -> DataFrame."""
    if test_file.endswith('.csv'):
        df = pd.read_csv(test_file)
        return {'sheet1': df}
    else:
        xls = pd.ExcelFile(test_file)
        return {sheet_name: xls.parse(sheet_name) for sheet_name in xls.sheet_names}


def process_sheet_for_testing(df, sheet_name, file_name, download_url, resource_id, isp, llm_model: str):
    """Process a single sheet using the specified LLM model for all classifiers."""
    report = init_report(df, sheet_name, file_name, download_url, resource_id)

    report = PIIClassifier(
        model_name=llm_model,
        azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
        api_key=config.AZURE_OPENAI_API_KEY,
    ).classify_df(df, report)
    report = PIIReflectionClassifier(
        model_name=llm_model,
        azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
        api_key=config.AZURE_OPENAI_API_KEY,
    ).classify_df(table_markdown(report), report)
    report = NonPIIClassifier(
        model_name=llm_model,
        azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
        api_key=config.AZURE_OPENAI_API_KEY,
    ).classify(table_markdown(report), report, isp)

    return report.to_dict()


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


def run_test_pipeline_groundtruth(files: list, output_dir: str):
    """
    Generate ground truth JSON for test files without running LLM.
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)

    groundtruth_path = output_path / 'groundtruth'
    groundtruth_path.mkdir(exist_ok=True)

    for test_file in files:
        if os.path.exists(groundtruth_path / f'{Path(test_file).stem}.json'):
            print(f'Skipping {test_file} groundtruth because it already exists')
            continue
        dfs = load_test_data(test_file)
        isp = load_isp_info(test_file)
        resource_id = Path(test_file).stem
        reports = []

        for sheet_name, df in dfs.items():
            report_dict = process_sheet_for_groundtruth(
                df, sheet_name, Path(test_file).name, test_file, resource_id, isp
            )
            reports.append(report_dict)

        gt_file = groundtruth_path / f'{Path(test_file).stem}.json'
        with open(gt_file, 'w', encoding='utf-8') as f:
            json.dump(reports, f, indent=2)

        print(f'Empty groundtruth reports generated for {test_file} at: {groundtruth_path}')


def run_test_pipeline(files: list, llm_model: str, output_dir: str):
    """
    Run pipeline on multiple test files using a single LLM.
    Computes table-level and column-level aggregated PII metrics (all columns combined).
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)

    # Folders
    groundtruth_path = output_path / 'groundtruth'
    groundtruth_path.mkdir(exist_ok=True)
    llm_folder = output_path / llm_model
    llm_folder.mkdir(exist_ok=True)

    metrics_summary = {}

    for test_file in files:
        llm_file_path = llm_folder / f'{Path(test_file).stem}.json'
        if llm_file_path.exists():
            print(f'Skipping {test_file} because it already exists')
            with open(llm_file_path, 'r') as f:
                reports = json.load(f)
        else:
            dfs = load_test_data(test_file)
            isp = load_isp_info(test_file)
            resource_id = Path(test_file).stem
            reports = []
            for sheet_name, df in dfs.items():
                report_dict = process_sheet_for_testing(
                    df, sheet_name, Path(test_file).name, test_file, resource_id, isp, llm_model
                )
                reports.append(report_dict)
            # Save LLM output
            with open(llm_file_path, 'w', encoding='utf-8') as f:
                json.dump(reports, f, indent=2)

        # Load ground truth
        gt_file_path = groundtruth_path / f'{Path(test_file).stem}.json'
        with open(gt_file_path, 'r', encoding='utf-8') as f:
            gt_reports = json.load(f)

        # Flatten all columns across sheets
        column_records = []
        for gt, pred in zip(gt_reports, reports):
            gt_columns = {col['column_name']: col['pii']['sensitive'] for col in gt['columns']}
            pred_columns = {col['column_name']: col['pii']['sensitive'] for col in pred['columns']}
            for col_name in gt_columns:
                column_records.append({'pii_true': gt_columns[col_name], 'pii_pred': pred_columns.get(col_name, False)})

        df_columns = pd.DataFrame(column_records)

        # Compute aggregated metrics across all columns
        aggregated_metrics = {
            'accuracy': accuracy_score(df_columns['pii_true'], df_columns['pii_pred']),
            'precision': precision_score(df_columns['pii_true'], df_columns['pii_pred'], zero_division=0),
            'recall': recall_score(df_columns['pii_true'], df_columns['pii_pred'], zero_division=0),
            'f1': f1_score(df_columns['pii_true'], df_columns['pii_pred'], zero_division=0),
        }

        metrics_summary[Path(test_file).stem] = {'pii_sensitivity': aggregated_metrics}

    # Save summary metrics
    metrics_file = output_path / f'{llm_model}_metrics_summary.json'
    with open(metrics_file, 'w', encoding='utf-8') as f:
        json.dump(metrics_summary, f, indent=2)

    print(f'Test pipeline finished for LLM {llm_model}. Metrics saved to {metrics_file}')
    return metrics_summary


if __name__ == '__main__':
    # Example usage: specify the LLM model name
    LLM_MODEL = 'gpt-4.1-mini'
    test_files = ['research/data/data.xlsx']
    OUTPUT_DIR = 'research/results/test_results'

    run_test_pipeline_groundtruth(test_files, OUTPUT_DIR)

    run_test_pipeline(test_files, LLM_MODEL, OUTPUT_DIR)
