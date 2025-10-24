"""test/unit/test_ckan_utils.py: Unit tests for utils/ckan.py."""

import pytest
from utils.ckan import CKANClient
import dotenv
import os
import logging
import pathlib

dotenv.load_dotenv()


def test_invalid_resource_id_type():
    """Test CKANClient raises ValueError when resource_id is not a string."""
    with pytest.raises(ValueError, match='fields must be a dictionary'):
        CKANClient().update_resource_fields(None, 'not-a-dict')
    with pytest.raises(ValueError, match='resource_id must be a string'):
        CKANClient().update_resource_fields(None, {'sensitive': True})


test_resource_id = '651aec8f-5c7c-4539-a0c8-3235a8dfde76'


def test_update_resource_fields_success():
    CKAN_URL = os.getenv('CKAN_URL')
    CKAN_API_TOKEN = os.getenv('CKAN_API_TOKEN')

    ckan = CKANClient(base_url=CKAN_URL, api_token=CKAN_API_TOKEN, logger=None)

    resource = ckan.resource_show(test_resource_id)
    assert resource is not None
    assert resource['download_url'] is not None

    ckan.remove_resource_field(test_resource_id, 'sensitive')
    new_resource = ckan.resource_show(test_resource_id)
    assert new_resource is not None
    assert new_resource.get('sensitive') is None

    ckan.download_resource(test_resource_id, output_dir=(pathlib.Path('test/unit/downloads')))
    filename = pathlib.Path(ckan.resource_show(test_resource_id).get('download_url')).name
    assert filename is not None
    assert filename.endswith('.csv')
    assert os.path.exists(pathlib.Path('test/unit/downloads') / filename)
    os.remove(pathlib.Path('test/unit/downloads') / filename)
