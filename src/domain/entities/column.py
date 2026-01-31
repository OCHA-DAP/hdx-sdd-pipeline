"""Column entity representing a single column in a dataset."""

from dataclasses import dataclass, field
from typing import List, Any, Optional

from ..value_objects.entity_type import PIIEntityType
from ...shared.utils.json_serializer import make_json_serializable


@dataclass
class PIIClassification:
    """PII classification result for a column."""

    entity_type: PIIEntityType = PIIEntityType.NONE
    sensitive: bool = False
    confidence: Optional[float] = None
    explanation: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        result = {
            'entity_type': str(self.entity_type),
            'sensitive': self.sensitive,
        }
        if self.confidence is not None:
            result['confidence'] = self.confidence
        if self.explanation:
            result['explanation'] = self.explanation
        return result

    @classmethod
    def from_dict(cls, data: dict) -> 'PIIClassification':
        """Create from dictionary representation."""
        return cls(
            entity_type=PIIEntityType.from_string(data.get('entity_type', 'None')),
            sensitive=data.get('sensitive', False),
            confidence=data.get('confidence'),
            explanation=data.get('explanation'),
        )


@dataclass
class Column:
    """
    Represents a single column in a dataset with its metadata and classifications.

    This is a domain entity that encapsulates all information about a column
    including its name, sample values, and classification results.
    """

    name: str
    sample_values: List[Any] = field(default_factory=list)
    pii_classification: PIIClassification = field(default_factory=PIIClassification)

    def __post_init__(self):
        """Validate column data after initialization."""
        if not self.name:
            raise ValueError('Column name cannot be empty')

        # Ensure sample_values is a list
        if not isinstance(self.sample_values, list):
            self.sample_values = list(self.sample_values)

    def has_pii(self) -> bool:
        """Check if this column contains PII."""
        return self.pii_classification.entity_type.is_pii()

    def is_sensitive(self) -> bool:
        """Check if this column is classified as sensitive."""
        return self.pii_classification.sensitive

    def has_valid_samples(self) -> bool:
        """Check if column has valid sample values."""
        if not self.sample_values:
            return False
        return any(v not in (None, '', 'nan', 'NaN') for v in self.sample_values)

    def to_dict(self) -> dict:
        """Convert column to dictionary representation."""
        return {
            'column_name': self.name,
            'sample_values': make_json_serializable(self.sample_values),
            'personal_data': self.pii_classification.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Column':
        """Create Column from dictionary representation."""
        # Support both old 'pii' and new 'personal_data' keys
        pii_data = data.get('personal_data', data.get('pii', {}))
        return cls(
            name=data.get('column_name', ''),
            sample_values=data.get('sample_values', []),
            pii_classification=PIIClassification.from_dict(pii_data),
        )
