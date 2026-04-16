"""
A script to load an HDX resource and print its metadata.
Usage: python scripts/read_hdx_metadata.py <resource_id>
"""

import sys
import json
import logging
from pathlib import Path
from src.shared.utils.ckan import CKANClient
from config.config import get_config

# Add the project root to sys.path to allow imports from src
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def main():
    # Load configuration
    config = get_config()

    # Check if resource ID is provided as argument
    if len(sys.argv) < 2:
        print('Usage: python scripts/read_hdx_metadata.py <resource_id>')
        print('Example: python scripts/read_hdx_metadata.py a448773e-3f71-460d-959c-6b3a010d24c0')
        sys.exit(1)

    resource_id = sys.argv[1]

    # Initialize CKAN client
    # Use defaults from configuration (which reads from .env)
    ckan = CKANClient(
        base_url=config.HDX_URL or 'https://data.humdata.org',
        api_token=config.HDX_KEY,
        user_agent=config.SDD_USER_AGENT,
    )

    logger.info(f'Fetching metadata for resource: {resource_id}')

    try:
        # Fetch resource metadata
        metadata = ckan.resource_show(resource_id)

        if metadata:
            print('\n' + '=' * 50)
            print(f'METADATA FOR RESOURCE: {resource_id}')
            print('=' * 50)

            # Print formatted JSON
            print(json.dumps(metadata, indent=4, default=str))

            print('=' * 50)
            print('Metadata retrieval complete.')
        else:
            logger.error(f'Could not retrieve metadata for resource: {resource_id}')

    except Exception as e:
        logger.error(f'An error occurred: {e}')


if __name__ == '__main__':
    main()
