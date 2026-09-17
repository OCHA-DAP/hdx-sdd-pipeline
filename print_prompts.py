#!/usr/bin/env python3
"""
print_prompts.py: Utility script to render and print all prompt templates
integrated with Google Sheets and local Excel fallback.

Usage:
    python print_prompts.py
    python print_prompts.py --category pii_detection
    python print_prompts.py --category non_pii_default
    python print_prompts.py --excel-only
"""

import argparse
import sys
from typing import Dict, Any
from src.shared.utils.prompt_manager import PromptManager
from src.infrastructure.external.pii_prompt_strategy import SpreadsheetPromptStrategy
from src.infrastructure.external.isp_strategies import GoogleSheetsISPStrategy

DUMMY_ISP = {
    'country': 'Default',
    'sensitivity_rules': {
        'SEVERE_SENSITIVE': {'data and information type': ['Rule A: Direct Identifiers']},
        'HIGH_SENSITIVE': {'data and information type': ['Rule B: Microdata quasi-identifiers']},
        'MEDIUM_SENSITIVE': {'data and information type': ['Rule C: Facility-level counts']},
        'LOW/NON_SENSITIVE': {'data and information type': ['Rule D: Aggregated public indicators']},
    },
}

try:
    isp_strategy = GoogleSheetsISPStrategy()
    REAL_ISPS = isp_strategy.get_isps()
except Exception as err:
    print(f'Warning: Could not fetch real ISPs ({err}). Falling back to dummy ISP.', file=sys.stderr)
    REAL_ISPS = {}

afghanistan_isp = REAL_ISPS.get('Afghanistan') or REAL_ISPS.get('Default') or DUMMY_ISP
default_isp = REAL_ISPS.get('Default') or REAL_ISPS.get('default') or DUMMY_ISP

SAMPLE_CONTEXTS: Dict[str, Dict[str, Any]] = {
    'pii_detection': {
        'column_name': 'phone_number',
        'sample_values': ['+249912345678', '+249987654321'],
    },
    'pii_reflection': {
        'column_name': 'district_name',
        'initial_pii_type': 'PERSON_NAME',
        'sample_values': ['Kabul', 'Herat', 'Kandahar'],
        'dataset_title': 'Afghanistan Food Security Survey 2024',
        'dataset_description': 'Assessing food security and demographic distribution across provinces.',
        'dataset_source': 'WFP',
        'dataset_location': 'Afghanistan',
        'organization_title': 'UN WFP',
        'resource_name': 'afg_food_sec_2024.csv',
        'resource_description': 'Raw survey responses',
    },
    'non_pii_classification': {
        'column_name': 'adm2_name',
        'sample_values': ['Locality 1', 'Locality 2'],
        'dataset_title': 'Sudan Humanitarian Needs 2024',
        'dataset_description': 'Aggregated population displacement counts by locality.',
        'dataset_source': 'OCHA',
        'dataset_location': 'Sudan',
        'organization_title': 'UN OCHA',
        'resource_name': 'sudan_idp_counts.csv',
        'resource_description': 'Admin 2 level aggregated data',
        'isp': afghanistan_isp,
    },
    'non_pii_default': {
        'column_name': 'count_val',
        'sample_values': ['150', '230', '45'],
        'dataset_title': 'Generic Indicator Counts',
        'dataset_description': 'Numeric counts',
        'dataset_source': 'Partner',
        'dataset_location': 'Global',
        'organization_title': 'Partner Org',
        'resource_name': 'counts.csv',
        'resource_description': 'Indicator table',
        'isp': default_isp,
    },
    'readme_scan': {
        'readme_string': (
            'Dataset collected by OCHA Sudan. For questions contact info@ocha.org or data@worldbank.org.'
        ),
    },
}


def print_prompt_category(
    pm: PromptManager,
    cat_name: str,
    excel_only: bool = False,
    excel_path: str = 'src/prompts/prompts_dev.xlsx',
):
    prompt_key_map = {
        'pii_detection': 'pii_detection',
        'pii_reflection': 'pii_reflection',
        'non_pii_classification': 'non_pii_classification',
        'non_pii_default': 'non_pii_classification/default',
        'readme_scan': 'readme_scan',
    }

    ws_map = {
        'pii_detection': 'personal_data_detection',
        'pii_reflection': 'personal_data_reflection',
        'non_pii_classification': 'non_personal_data_classificatio',
        'non_pii_default': 'non_personal_data_default_class',
        'readme_scan': 'readme',
    }

    pm_key = prompt_key_map.get(cat_name, cat_name)
    context = SAMPLE_CONTEXTS.get(cat_name, {}).copy()

    if excel_only:
        ws_name = ws_map.get(cat_name, cat_name)
        strat = SpreadsheetPromptStrategy(
            worksheet_name=ws_name,
            spreadsheet_url='http://force-excel-fallback',
            excel_path=excel_path,
        )
        context['active_rules'] = strat.load_rules(force_refresh=True)

    rendered = pm.get_prompt(pm_key, context=context)

    divider = '=' * 80
    print(f'\n{divider}')
    print(f' CATEGORY: {cat_name.upper()} (Target: {pm_key})')
    print(f'{divider}\n')
    print(rendered)
    print('\n')


def main():
    parser = argparse.ArgumentParser(description='Print rendered prompts with Google Sheets / Excel rules.')
    parser.add_argument(
        '--category',
        '-c',
        choices=['pii_detection', 'pii_reflection', 'non_pii_classification', 'non_pii_default', 'readme_scan', 'all'],
        default='all',
        help='Specific prompt category to print (default: all)',
    )
    parser.add_argument(
        '--excel-only',
        action='store_true',
        help='Bypass Google Sheets and load rules directly from local Excel file (prompts_dev.xlsx)',
    )
    parser.add_argument(
        '--excel-path',
        default='src/prompts/prompts_dev.xlsx',
        help='Path to local Excel file (default: src/prompts/prompts_dev.xlsx)',
    )

    args = parser.parse_args()
    pm = PromptManager()

    categories = (
        ['pii_detection', 'pii_reflection', 'non_pii_classification', 'non_pii_default', 'readme_scan']
        if args.category == 'all'
        else [args.category]
    )

    for cat in categories:
        try:
            print_prompt_category(pm, cat, excel_only=args.excel_only, excel_path=args.excel_path)
        except Exception as e:
            print(f'Error rendering {cat}: {e}', file=sys.stderr)


if __name__ == '__main__':
    main()
