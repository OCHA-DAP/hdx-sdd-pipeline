"""Unit tests for PIIEntityType value object."""

from src.domain.value_objects.entity_type import PIIEntityType


class TestPIIEntityType:
    """Test suite for PIIEntityType value object."""

    def test_from_string_exact_match(self):
        """Test parsing with exact enum value."""
        result = PIIEntityType.from_string('EMAIL_ADDRESS')
        assert result == PIIEntityType.EMAIL_ADDRESS

    def test_from_string_lowercase(self):
        """Test parsing with lowercase input."""
        result = PIIEntityType.from_string('email_address')
        assert result == PIIEntityType.EMAIL_ADDRESS

    def test_from_string_with_spaces(self):
        """Test parsing with spaces."""
        result = PIIEntityType.from_string('PERSON NAME')
        assert result == PIIEntityType.PERSON_NAME

    def test_from_string_with_hyphens(self):
        """Test parsing with hyphens."""
        result = PIIEntityType.from_string('PHONE-NUMBER')
        assert result == PIIEntityType.PHONE_NUMBER

    def test_from_string_none_values(self):
        """Test parsing None-like values."""
        assert PIIEntityType.from_string('none') == PIIEntityType.NONE
        assert PIIEntityType.from_string('None') == PIIEntityType.NONE
        assert PIIEntityType.from_string('null') == PIIEntityType.NONE
        assert PIIEntityType.from_string('') == PIIEntityType.NONE
        assert PIIEntityType.from_string(None) == PIIEntityType.NONE

    def test_from_string_invalid(self):
        """Test parsing invalid value."""
        result = PIIEntityType.from_string('INVALID_TYPE')
        assert result == PIIEntityType.UNDETERMINED

    def test_is_pii_true(self):
        """Test is_pii returns True for actual PII types."""
        assert PIIEntityType.EMAIL_ADDRESS.is_pii() is True
        assert PIIEntityType.PERSON_NAME.is_pii() is True
        assert PIIEntityType.PHONE_NUMBER.is_pii() is True
        assert PIIEntityType.ADDRESS.is_pii() is True

    def test_is_pii_false(self):
        """Test is_pii returns False for non-PII types."""
        assert PIIEntityType.NONE.is_pii() is False
        assert PIIEntityType.UNDETERMINED.is_pii() is False

    def test_str_representation(self):
        """Test string representation."""
        assert str(PIIEntityType.EMAIL_ADDRESS) == 'EMAIL_ADDRESS'
        assert str(PIIEntityType.NONE) == 'None'

    def test_all_entity_types_parseable(self):
        """Test that all entity types can be parsed back from their string representation."""
        for entity_type in PIIEntityType:
            if entity_type not in (PIIEntityType.NONE, PIIEntityType.UNDETERMINED):
                parsed = PIIEntityType.from_string(str(entity_type))
                assert parsed == entity_type
