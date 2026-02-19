"""PII Classification entity."""

from dataclasses import dataclass
from typing import Optional

from ..value_objects.entity_type import PIIEntityType


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
