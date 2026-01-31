"""Unit tests for Column entity."""

import pytest
from src.domain.entities.column import Column, PIIClassification
from src.domain.value_objects.entity_type import PIIEntityType


class TestColumn:
    """Test suite for Column entity."""

    def test_create_column_basic(self):
        """Test creating a basic column."""
        column = Column(name='email', sample_values=['test@example.com', 'user@test.com'])
        assert column.name == 'email'
        assert len(column.sample_values) == 2
        assert column.pii_classification is not None
        assert column.pii_classification.entity_type == PIIEntityType.NONE

    def test_create_column_empty_name_raises_error(self):
        """Test that empty column name raises ValueError."""
        with pytest.raises(ValueError, match='Column name cannot be empty'):
            Column(name='', sample_values=[])

    def test_has_pii_true(self):
        """Test has_pii returns True when column has PII."""
        pii_class = PIIClassification(entity_type=PIIEntityType.EMAIL_ADDRESS, sensitive=True)
        column = Column(name='email', sample_values=['test@example.com'], pii_classification=pii_class)
        assert column.has_pii() is True

    def test_has_pii_false(self):
        """Test has_pii returns False when column has no PII."""
        pii_class = PIIClassification(entity_type=PIIEntityType.NONE, sensitive=False)
        column = Column(name='count', sample_values=['1', '2', '3'], pii_classification=pii_class)
        assert column.has_pii() is False

    def test_is_sensitive_true(self):
        """Test is_sensitive returns True when classified as sensitive."""
        pii_class = PIIClassification(entity_type=PIIEntityType.EMAIL_ADDRESS, sensitive=True)
        column = Column(name='email', sample_values=['test@example.com'], pii_classification=pii_class)
        assert column.is_sensitive() is True

    def test_is_sensitive_false(self):
        """Test is_sensitive returns False when not sensitive."""
        pii_class = PIIClassification(entity_type=PIIEntityType.EMAIL_ADDRESS, sensitive=False)
        column = Column(name='email', sample_values=['test@example.com'], pii_classification=pii_class)
        assert column.is_sensitive() is False

    def test_has_valid_samples_true(self):
        """Test has_valid_samples returns True for valid samples."""
        column = Column(name='name', sample_values=['John', 'Jane', 'Bob'])
        assert column.has_valid_samples() is True

    def test_has_valid_samples_false_empty(self):
        """Test has_valid_samples returns False for empty samples."""
        column = Column(name='name', sample_values=[])
        assert column.has_valid_samples() is False

    def test_has_valid_samples_false_all_empty_strings(self):
        """Test has_valid_samples returns False when all values are empty."""
        column = Column(name='name', sample_values=['', '', ''])
        assert column.has_valid_samples() is False

    def test_has_valid_samples_mixed(self):
        """Test has_valid_samples returns True when some values are valid."""
        column = Column(name='name', sample_values=['John', '', None, 'Jane'])
        assert column.has_valid_samples() is True

    def test_to_dict(self):
        """Test converting column to dictionary."""
        pii_class = PIIClassification(entity_type=PIIEntityType.EMAIL_ADDRESS, sensitive=True)
        column = Column(name='email', sample_values=['test@example.com'], pii_classification=pii_class)
        result = column.to_dict()

        assert result['column_name'] == 'email'
        assert result['sample_values'] == ['test@example.com']
        assert result['personal_data']['entity_type'] == 'EMAIL_ADDRESS'
        assert result['personal_data']['sensitive'] is True

    def test_from_dict(self):
        """Test creating column from dictionary."""
        data = {
            'column_name': 'email',
            'sample_values': ['test@example.com', 'user@test.com'],
            'pii': {'entity_type': 'EMAIL_ADDRESS', 'sensitive': True},
        }
        column = Column.from_dict(data)

        assert column.name == 'email'
        assert len(column.sample_values) == 2
        assert column.pii_classification.entity_type == PIIEntityType.EMAIL_ADDRESS
        assert column.pii_classification.sensitive is True

    def test_round_trip_serialization(self):
        """Test that to_dict and from_dict are inverses."""
        original = Column(
            name='phone',
            sample_values=['+1234567890', '+0987654321'],
            pii_classification=PIIClassification(
                entity_type=PIIEntityType.PHONE_NUMBER, sensitive=True, explanation='Contains phone numbers'
            ),
        )

        # Convert to dict and back
        data = original.to_dict()
        restored = Column.from_dict(data)

        assert restored.name == original.name
        assert restored.sample_values == original.sample_values
        assert restored.pii_classification.entity_type == original.pii_classification.entity_type
        assert restored.pii_classification.sensitive == original.pii_classification.sensitive
