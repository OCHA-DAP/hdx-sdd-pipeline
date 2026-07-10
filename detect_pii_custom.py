#!/usr/bin/env python3
"""
Custom script to run PII detection on a specific column and values using multiple models.
Usage: uv run python detect_pii_custom.py
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Ensure the project root is in the path
ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

# Load environment variables
load_dotenv()

from config import get_config  # noqa: E402
from src.infrastructure.openai_provider import OpenAIProvider  # noqa: E402
from src.shared.utils.prompt_manager import PromptManager  # noqa: E402


def main():
    print('=' * 60)
    print('         Multi-Model PII Entity Detection Tester')
    print('=' * 60)

    # Load configuration
    config = get_config()
    api_key = os.getenv('OPENAI_API_KEY') or config.OPENAI_API_KEY
    endpoint = os.getenv('OPENAI_ENDPOINT') or config.OPENAI_ENDPOINT

    if not api_key:
        print('Error: OPENAI_API_KEY is not set. Please set it in your .env file.')
        sys.exit(1)

    # List of models to test
    models_to_test = [
        'gpt-5.4-nano',
        'gpt-5.4-mini',
        'gpt-5.4',
        'DeepSeek-V3.1',
        'DeepSeek-V4-Flash',
        'DeepSeek-V4-Pro',
    ]

    try:
        prompt_manager = PromptManager()
    except Exception as e:
        print(f'Failed to initialize prompt manager: {e}')
        sys.exit(1)

    column_name = ' Unnamed_Column_20'
    sample_values = ['Siribala', 'Hamzakoma', 'Kerena', 'Adjelhoc', 'Tonka']
    column_name = 'Settlement Name - #adm2+code'
    sample_values = ['Raama Cadey', 'Koban Dheere', 'Madawarabe', 'Mahad Alle-3', 'Moori Dhir']
    column_name = 'Please specify if other'
    sample_values = ['-', 'Tchinhungue', 'Nharingas', 'Chiute', 'Nharinga']

    print(f"Column name:   '{column_name}'")
    print(f'Sample values: {sample_values}')
    print('=' * 60)

    # Render prompt once
    prompt = prompt_manager.get_prompt(
        'pii_detection',
        version=None,
        context={
            'column_name': column_name,
            'sample_values': sample_values,
        },
    )

    for model_name in models_to_test:
        # print(f"Testing Model: {model_name}...")
        try:
            provider = OpenAIProvider(
                model_name=model_name,
                endpoint=endpoint,
                api_key=api_key,
            )
            result, comp_tokens, prompt_tokens = provider.generate(prompt, max_tokens=24)
            print(f'Model: {model_name} | Classification: {result.strip()}')
        except Exception as e:
            print(f'Model: {model_name} | Error: {e}')


if __name__ == '__main__':
    main()
