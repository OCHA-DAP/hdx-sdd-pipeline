import os
import sys
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from src.infrastructure.external.isp_strategies import GoogleSheetsISPStrategy  # noqa: E402
from src.shared.utils.prompt_manager import PromptManager  # noqa: E402


def main():
    print('==========================================================')
    print('Google Sheets ISP Rules Prompt Renderer')
    print('==========================================================')

    url = (
        os.getenv('ISP_GOOGLE_SHEET_URL')
        or 'https://docs.google.com/spreadsheets/d/1Z5wj6H6WV2E8VN9r6y8AfdgOltTNL-z2KIlbnNzzPok/edit?gid=0#gid=0'
    )
    print(f'Loading sheet from: {url}\n')

    # Verify credentials present
    private_key = os.getenv('GOOGLE_SHEETS_PRIVATE_KEY')
    client_email = os.getenv('GOOGLE_SHEETS_CLIENT_EMAIL')

    if not private_key or not client_email:
        print('WARNING: GOOGLE_SHEETS_PRIVATE_KEY and/or GOOGLE_SHEETS_CLIENT_EMAIL are not set in the environment.')
        print('Please check your .env file. The script will now exit.')
        return

    # Load strategy
    strategy = GoogleSheetsISPStrategy(spreadsheet_url=url)
    isps = strategy.get_isps()

    prompt_manager = PromptManager()

    # Select countries to print prompts for
    countries_to_test = ['Afghanistan', 'default']

    # Sample table markdown
    sample_table = (
        '| ID | Partner | Location | Activity | Beneficiaries |\n'
        '|----|---------|----------|----------|---------------|\n'
        '| 1  | UNHCR   | Kabul    | Shelter  | 1500          |\n'
        '| 2  | UNICEF  | Herat    | WASH     | 850           |'
    )

    sample_metadata = {
        'dataset_title': 'Humanitarian Assistance Operations 2026',
        'dataset_description': (
            'This dataset captures the operational presence and caseloads for partner organizations.'
        ),
        'dataset_source': 'OCHA Partner Portal',
        'dataset_location': 'Afghanistan',
        'organization_title': 'UN Office for the Coordination of Humanitarian Affairs',
        'resource_name': 'ocha_presence_afg.csv',
        'resource_description': 'Caseloads and partner activities by locality.',
    }

    for country in countries_to_test:
        isp_data = isps.get(country)
        if not isp_data:
            print(f'No rules found for {country} in sheet.\n')
            continue

        print('\n==========================================================')
        print(f'Rendered Non-PII Prompt for Country: {country}')
        print('==========================================================')

        prompt_name = 'non_pii_classification'
        if isp_data.get('country') == 'default' or isp_data.get('is_default'):
            prompt_name = 'non_pii_classification/default'

        prompt = prompt_manager.get_prompt(
            prompt_name,
            version=None,  # Latest
            context={
                'table_name': 'caseloads_data',
                'table_markdown': sample_table,
                'isp': isp_data,
                **sample_metadata,
            },
        )
        print(prompt)
        print('\n' + '=' * 58 + '\n')


if __name__ == '__main__':
    main()
