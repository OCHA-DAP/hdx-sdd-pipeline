"""PII Entity Type value object."""

from enum import Enum
from typing import Optional


class PIIEntityType(str, Enum):
    """Enumeration of PII entity types that can be detected."""

    # Personal identifiers
    PERSON_NAME = "PERSON_NAME"
    EMAIL_ADDRESS = "EMAIL_ADDRESS"
    PHONE_NUMBER = "PHONE_NUMBER"

    # Location data
    LOCATION = "LOCATION"
    ADDRESS = "ADDRESS"

    # Identification numbers
    ID_NUMBER = "ID_NUMBER"
    PASSPORT_NUMBER = "PASSPORT_NUMBER"
    SOCIAL_SECURITY_NUMBER = "SOCIAL_SECURITY_NUMBER"

    # Financial
    CREDIT_CARD = "CREDIT_CARD"
    BANK_ACCOUNT = "BANK_ACCOUNT"

    # Demographic
    AGE = "AGE"
    DATE_OF_BIRTH = "DATE_OF_BIRTH"
    GENDER = "GENDER"

    # Other
    IP_ADDRESS = "IP_ADDRESS"
    URL = "URL"

    # Special values
    NONE = "None"
    UNDETERMINED = "UNDETERMINED"

    @classmethod
    def from_string(cls, value: str) -> 'PIIEntityType':
        """
        Parse PII entity type from string.

        Args:
            value: String representation of entity type

        Returns:
            PIIEntityType enum value
        """
        if not value or value.lower() in ('none', 'null', ''):
            return cls.NONE

        value_upper = value.upper().strip()

        # Try direct match
        try:
            return cls(value_upper)
        except ValueError:
            pass

        # Try with underscores
        value_normalized = value_upper.replace('-', '_').replace(' ', '_')
        try:
            return cls(value_normalized)
        except ValueError:
            return cls.UNDETERMINED

    def is_pii(self) -> bool:
        """Check if this entity type represents actual PII."""
        return self not in {self.NONE, self.UNDETERMINED}

    def __str__(self) -> str:
        return self.value
