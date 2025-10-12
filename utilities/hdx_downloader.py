"""utilities/hdx_downloader.py: HDX data downloader for the SSD Pipeline."""

from urllib.parse import urlparse
import logging
import os
import requests

from .main_config import HDX_API_BASE_URL, INPUT_DIR

logger = logging.getLogger(__name__)


def download_resource(resource_id: str, output_dir: str = None) -> str:
    """
    Download a resource from HDX by resource ID.

    Args:
        resource_id: The HDX resource ID
        output_dir: Directory to save the file (defaults to INPUT_DIR)

    Returns:
        str: Local file path of the downloaded file

    Raises:
        Exception: If download fails
    """
    if output_dir is None:
        output_dir = INPUT_DIR

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Get resource metadata from HDX API
        resource_url = f'{HDX_API_BASE_URL}/resource_show?id={resource_id}'
        logger.info(f'Fetching resource metadata from: {resource_url}')

        response = requests.get(resource_url, timeout=30)
        response.raise_for_status()

        resource_data = response.json()

        if not resource_data.get('success'):
            raise Exception(f"HDX API returned error: {resource_data.get('error', 'Unknown error')}")

        result = resource_data.get('result', {})
        resource_url = result.get('url')
        resource_name = result.get('name', f'resource_{resource_id}')

        if not resource_url:
            raise Exception('No download URL found in resource metadata')

        logger.info(f'Downloading resource from: {resource_url}')

        # Download the file
        file_response = requests.get(resource_url, timeout=300, stream=True)
        file_response.raise_for_status()

        # Determine file extension from URL or content type
        parsed_url = urlparse(resource_url)
        file_extension = os.path.splitext(parsed_url.path)[1]

        if not file_extension:
            content_type = file_response.headers.get('content-type', '')
            if 'csv' in content_type:
                file_extension = '.csv'
            elif 'excel' in content_type or 'spreadsheet' in content_type:
                file_extension = '.xlsx'
            else:
                file_extension = '.csv'  # Default to CSV

        # Create safe filename
        safe_name = ''.join(c for c in resource_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_name = safe_name.replace(' ', '_')
        filename = f'{safe_name}_{resource_id}{file_extension}'
        file_path = os.path.join(output_dir, filename)

        # Save file
        with open(file_path, 'wb') as f:
            for chunk in file_response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f'Successfully downloaded resource to: {file_path}')
        return file_path

    except requests.exceptions.RequestException as e:
        logger.error(f'Network error downloading resource {resource_id}: {e}')
        raise Exception(f'Failed to download resource {resource_id}: {e}')
    except Exception as e:
        logger.error(f'Error downloading resource {resource_id}: {e}')
        raise


def get_resource_metadata(resource_id: str) -> dict:
    """
    Get metadata for a resource from HDX API.

    Args:
        resource_id: The HDX resource ID

    Returns:
        dict: Resource metadata

    Raises:
        Exception: If API call fails
    """
    try:
        resource_url = f'{HDX_API_BASE_URL}/resource_show?id={resource_id}'
        logger.info(f'Fetching resource metadata from: {resource_url}')

        response = requests.get(resource_url, timeout=30)
        response.raise_for_status()

        resource_data = response.json()

        if not resource_data.get('success'):
            raise Exception(f"HDX API returned error: {resource_data.get('error', 'Unknown error')}")

        return resource_data.get('result', {})

    except requests.exceptions.RequestException as e:
        logger.error(f'Network error fetching resource metadata {resource_id}: {e}')
        raise Exception(f'Failed to fetch resource metadata {resource_id}: {e}')
    except Exception as e:
        logger.error(f'Error fetching resource metadata {resource_id}: {e}')
        raise
