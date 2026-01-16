"""Unit tests for SensitivityLevel value object."""

import pytest
from src.domain.value_objects.sensitivity import SensitivityLevel


class TestSensitivityLevel:
    """Test suite for SensitivityLevel value object."""
    
    def test_from_string_exact_match(self):
        """Test parsing with exact enum value."""
        result = SensitivityLevel.from_string("HIGH_SENSITIVE")
        assert result == SensitivityLevel.HIGH_SENSITIVE
    
    def test_from_string_lowercase(self):
        """Test parsing with lowercase input."""
        result = SensitivityLevel.from_string("high")
        assert result == SensitivityLevel.HIGH_SENSITIVE
    
    def test_from_string_with_underscores(self):
        """Test parsing with underscores."""
        result = SensitivityLevel.from_string("moderate_sensitive")
        assert result == SensitivityLevel.MODERATE_SENSITIVE
    
    def test_from_string_with_hyphens(self):
        """Test parsing with hyphens."""
        result = SensitivityLevel.from_string("high-sensitive")
        assert result == SensitivityLevel.HIGH_SENSITIVE
    
    def test_from_string_simple_values(self):
        """Test parsing simple values like 'low', 'medium', 'high'."""
        assert SensitivityLevel.from_string("low") == SensitivityLevel.NON_SENSITIVE
        assert SensitivityLevel.from_string("medium") == SensitivityLevel.MEDIUM_SENSITIVE
        assert SensitivityLevel.from_string("high") == SensitivityLevel.HIGH_SENSITIVE
        assert SensitivityLevel.from_string("severe") == SensitivityLevel.SEVERE_SENSITIVE
    
    def test_from_string_empty(self):
        """Test parsing empty string."""
        result = SensitivityLevel.from_string("")
        assert result == SensitivityLevel.UNDETERMINED
    
    def test_from_string_none(self):
        """Test parsing None."""
        result = SensitivityLevel.from_string(None)
        assert result == SensitivityLevel.UNDETERMINED
    
    def test_from_string_invalid(self):
        """Test parsing invalid value."""
        result = SensitivityLevel.from_string("invalid_value")
        assert result == SensitivityLevel.UNDETERMINED
    
    def test_is_sensitive_true(self):
        """Test is_sensitive returns True for sensitive levels."""
        assert SensitivityLevel.MODERATE_SENSITIVE.is_sensitive() is True
        assert SensitivityLevel.MEDIUM_SENSITIVE.is_sensitive() is True
        assert SensitivityLevel.HIGH_SENSITIVE.is_sensitive() is True
        assert SensitivityLevel.SEVERE_SENSITIVE.is_sensitive() is True
    
    def test_is_sensitive_false(self):
        """Test is_sensitive returns False for non-sensitive levels."""
        assert SensitivityLevel.NON_SENSITIVE.is_sensitive() is False
        assert SensitivityLevel.UNDETERMINED.is_sensitive() is False
    
    def test_str_representation(self):
        """Test string representation."""
        assert str(SensitivityLevel.HIGH_SENSITIVE) == "HIGH_SENSITIVE"
        assert str(SensitivityLevel.NON_SENSITIVE) == "NON_SENSITIVE"
