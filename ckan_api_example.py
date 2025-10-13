import logging
import logging.config
import os
import requests

logging.config.fileConfig('logging.conf')

logger = logging.getLogger(__name__)

# Read configuration from environment variables
CKAN_URL = os.getenv('CKAN_URL', 'https://data.humdata.org')
CKAN_API_TOKEN = os.getenv('CKAN_API_TOKEN')

# Hard-coded IDs for the example
PACKAGE_ID = os.getenv('PACKAGE_ID')
RESOURCE_ID = os.getenv('RESOURCE_ID')


def package_show(package_id):
    """
    Fetch details about a package (dataset).

    Args:
        package_id: The ID or name of the package

    Returns:
        The package data dictionary
    """
    url = f'{CKAN_URL}/api/3/action/package_show'
    params = {'id': package_id}

    logger.info(f'Fetching package: {package_id}')
    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()
    if data['success']:
        logger.info(f'Successfully retrieved package: {data["result"]["name"]}')
        return data['result']
    else:
        logger.error(f'Failed to retrieve package: {data.get("error")}')
        return None


def resource_show(resource_id):
    """
    Fetch details about a resource.

    Args:
        resource_id: The ID of the resource

    Returns:
        The resource data dictionary
    """
    url = f'{CKAN_URL}/api/3/action/resource_show'
    params = {'id': resource_id}

    logger.info(f'Fetching resource: {resource_id}')
    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()
    if data['success']:
        logger.info(f'Successfully retrieved resource: {data["result"]["name"]}')
        return data['result']
    else:
        logger.error(f'Failed to retrieve resource: {data.get("error")}')
        return None


def resource_patch(resource_id, new_description):
    """
    Update the description of a resource.

    Args:
        resource_id: The ID of the resource to update
        new_description: The new description text

    Returns:
        The updated resource data dictionary
    """
    url = f'{CKAN_URL}/api/3/action/resource_patch'
    headers = {'Authorization': CKAN_API_TOKEN}
    payload = {'id': resource_id, 'description': new_description}

    logger.info(f'Updating resource description: {resource_id}')
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()

    data = response.json()
    if data['success']:
        logger.info(f'Successfully updated resource: {data["result"]["name"]}')
        return data['result']
    else:
        logger.error(f'Failed to update resource: {data.get("error")}')
        return None


if __name__ == '__main__':
    logger.info('Starting CKAN API demonstration')

    # Example 1: Show package details
    package = package_show(PACKAGE_ID)
    if package:
        logger.info(f'Package title: {package.get("title")}')
        logger.info(f'Number of resources: {len(package.get("resources", []))}')

    # Example 2: Show resource details
    resource = resource_show(RESOURCE_ID)
    if resource:
        logger.info(f'Resource name: {resource.get("name")}')
        logger.info(f'Current description: {resource.get("description")}')

    # Example 3: Update resource description
    new_description = 'This is an updated description for the resource.'
    updated_resource = resource_patch(RESOURCE_ID, new_description)
    if updated_resource:
        logger.info(f'New description: {updated_resource.get("description")}')

    logger.info('CKAN API demonstration completed')
