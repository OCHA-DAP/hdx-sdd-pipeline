"""Sensitivity level value object."""

from enum import Enum


class SensitivityLevel(str, Enum):
    """Enumeration of sensitivity levels for data classification."""

    NON_SENSITIVE = 'NON_SENSITIVE'
    MEDIUM_SENSITIVE = 'MEDIUM_SENSITIVE'
    HIGH_SENSITIVE = 'HIGH_SENSITIVE'
    SEVERE_SENSITIVE = 'SEVERE_SENSITIVE'
    UNDETERMINED = 'UNDETERMINED'

    @classmethod
    def from_string(cls, value: str) -> 'SensitivityLevel':
        """
        Parse sensitivity level from string, handling various formats.

        Args:
            value: String representation of sensitivity level

        Returns:
            SensitivityLevel enum value

        Examples:
            >>> SensitivityLevel.from_string("high")
            SensitivityLevel.HIGH_SENSITIVE
            >>> SensitivityLevel.from_string("MEDIUM_SENSITIVE")
            SensitivityLevel.MEDIUM_SENSITIVE
        """
        if not value:
            return cls.UNDETERMINED

        value_lower = value.lower().strip()

        # Direct mapping
        mapping = {
            'non_sensitive': cls.NON_SENSITIVE,
            'non-sensitive': cls.NON_SENSITIVE,
            'low': cls.NON_SENSITIVE,

            'medium_sensitive': cls.MEDIUM_SENSITIVE,
            'medium-sensitive': cls.MEDIUM_SENSITIVE,
            'medium': cls.MEDIUM_SENSITIVE,
            'high_sensitive': cls.HIGH_SENSITIVE,
            'high-sensitive': cls.HIGH_SENSITIVE,
            'high': cls.HIGH_SENSITIVE,
            'severe_sensitive': cls.SEVERE_SENSITIVE,
            'severe-sensitive': cls.SEVERE_SENSITIVE,
            'severe': cls.SEVERE_SENSITIVE,
        }

        return mapping.get(value_lower, cls.UNDETERMINED)

    def is_sensitive(self) -> bool:
        """Check if this sensitivity level indicates sensitive data."""
        return self in {self.MEDIUM_SENSITIVE, self.MEDIUM_SENSITIVE, self.HIGH_SENSITIVE, self.SEVERE_SENSITIVE}

    def __str__(self) -> str:
        return self.value
