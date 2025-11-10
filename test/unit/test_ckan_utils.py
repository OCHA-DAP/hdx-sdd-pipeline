"""test/unit/test_ckan_utils.py: Unit tests for utils/ckan.py."""

import pytest
from utils.ckan import CKANClient
import dotenv

dotenv.load_dotenv()


def test_invalid_resource_id_type():
    """Test CKANClient raises ValueError when resource_id is not a string."""
    with pytest.raises(ValueError, match='fields must be a dictionary'):
        CKANClient().update_resource_fields(None, 'not-a-dict')
    with pytest.raises(ValueError, match='resource_id must be a string'):
        CKANClient().update_resource_fields(None, {'sensitive': True})
