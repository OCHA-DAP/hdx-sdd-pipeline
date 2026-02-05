#!/usr/bin/env python3
"""
calculate_scores.py

Calculate aggregated PII and non-PII sensitivity metrics given
a ground truth JSON file and a predicted JSON file, and save
the results as a JSON file.

python llm_research_scores.py
    research/results/test_results/groundtruth/data.json
    research/results/test_results/gpt-4.1-mini/data.json
    gpt-4.1-mini
"""

import json
import sys
import pandas as pd
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score


def load_json(file_path: str):
    """Load JSON file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def compare_pii_columns(gt_reports, pred_reports):
    """Compare PII sensitivity for all columns across sheets."""
    records = []
    for gt, pred in zip(gt_reports, pred_reports):
        gt_cols = {c['column_name']: c['personal_data']['sensitive'] for c in gt['columns']}
        pred_cols = {c['column_name']: c['personal_data']['sensitive'] for c in pred['columns']}
        for col_name in gt_cols:
            records.append({'column_name': col_name, 'true': gt_cols[col_name], 'pred': pred_cols.get(col_name, False)})
    return pd.DataFrame(records)


def compare_pii_table_level(gt_reports, pred_reports):
    """Compare PII sensitivity at the table level."""
    records = []
    for gt, pred in zip(gt_reports, pred_reports):
        records.append({
            'true': gt.get('personal_data_sensitive', False),
            'pred': pred.get('personal_data_sensitive', False)
        })
    return pd.DataFrame(records)


def compare_non_pii_table_level(gt_reports, pred_reports):
    """Compare non-PII sensitivity at the table level."""
    records = []
    for gt, pred in zip(gt_reports, pred_reports):
        records.append({
            'true': gt.get('non_personal_data_sensitive', False),
            'pred': pred.get('non_personal_data_sensitive', False)
        })
    return pd.DataFrame(records)


def calculate_metrics(df: pd.DataFrame):
    """Compute accuracy, precision, recall, and F1 score."""
    return {
        'accuracy': accuracy_score(df['true'], df['pred']),
        'precision': precision_score(df['true'], df['pred'], zero_division=0),
        'recall': recall_score(df['true'], df['pred'], zero_division=0),
        'f1': f1_score(df['true'], df['pred'], zero_division=0),
    }


def main():
    if len(sys.argv) != 4:
        print('Usage: python llm_research_scores.py <groundtruth.json> <predictions.json> <llm_model>')
        sys.exit(1)

    gt_file = sys.argv[1]
    pred_file = sys.argv[2]
    llm_model = sys.argv[3]

    gt_reports = load_json(gt_file)
    pred_reports = load_json(pred_file)

    metrics = {
        pred_file: {
            'pii_columns': calculate_metrics(compare_pii_columns(gt_reports, pred_reports)),
            'pii_table_level': calculate_metrics(compare_pii_table_level(gt_reports, pred_reports)),
            'non_pii_table_level': calculate_metrics(compare_non_pii_table_level(gt_reports, pred_reports)),
        }
    }

    # Generate output filename
    output_file = f'research/results/test_results/{llm_model}_scores.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, indent=2)
    print(f'Metrics saved to {output_file}')


if __name__ == '__main__':
    main()
