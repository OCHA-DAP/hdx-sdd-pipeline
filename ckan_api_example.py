"""ckan_api_example.py: Example of using the CKAN API."""

import logging
import logging.config
import os
import requests

logging.config.fileConfig('logging.conf')

logger = logging.getLogger(__name__)

# Read configuration from environment variables
CKAN_URL = os.getenv('CKAN_URL')
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

    logger.info('Fetching package: %s', package_id)
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    if data['success']:
        logger.info('Successfully retrieved package: %s', data['result']['name'])
        return data['result']
    else:
        logger.error('Failed to retrieve package: %s', data.get('error'))
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

    logger.info('Fetching resource: %s', resource_id)
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    if data['success']:
        logger.info('Successfully retrieved resource: %s', data['result']['name'])
        return data['result']
    else:
        logger.error('Failed to retrieve resource: %s', data.get('error'))
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

    logger.info('Updating resource description: %s', resource_id)
    response = requests.post(url, json=payload, headers=headers, timeout=30)
    response.raise_for_status()

    data = response.json()
    if data['success']:
        logger.info('Successfully updated resource: %s', data['result']['name'])
        return data['result']
    else:
        logger.error('Failed to update resource: %s', data.get('error'))
        return None


if __name__ == '__main__':
    logger.info('Starting CKAN API demonstration')

    # Example 1: Show package details
    package = package_show(PACKAGE_ID)
    if package:
        logger.info('Package title: %s', package.get('title'))
        logger.info('Number of resources: %s', len(package.get('resources', [])))

    # Example 2: Show resource details
    resource = resource_show(RESOURCE_ID)
    if resource:
        logger.info('Resource name: %s', resource.get('name'))
        logger.info('Current description: %s', resource.get('description'))
        # Logg all keys of resource
        logger.info('Resource keys: %s', resource.keys())

    # Example 3: Update resource description
    NEW_DESCRIPTION = 'This is an updated description for the resource.'
    updated_resource = resource_patch(RESOURCE_ID, NEW_DESCRIPTION)
    if updated_resource:
        logger.info('New description: %s', updated_resource.get('description'))

    # Check if resource description is updated
    resource = resource_show(RESOURCE_ID)
    if resource.get('description') == NEW_DESCRIPTION:
        logger.info('Resource description is updated')
    else:
        logger.info('Resource description is not updated')

    logger.info('CKAN API demonstration completed')
