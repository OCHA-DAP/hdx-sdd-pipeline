"""Personal Data Classification entity."""

from dataclasses import dataclass
from typing import Optional

from ..value_objects.sensitivity import SensitivityLevel


@dataclass
class PersonalDataClassification:
    """Personal data classification result for a sheet/table."""

    sensitivity: SensitivityLevel = SensitivityLevel.UNDETERMINED
    explanation: Optional[str] = None
    confidence: Optional[float] = None

    def is_sensitive(self) -> bool:
        """Check if classified as sensitive."""
        return self.sensitivity.is_sensitive()

    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        result = {
            'sensitivity': str(self.sensitivity),
        }
        if self.explanation:
            result['explanation'] = self.explanation
        if self.confidence is not None:
            result['confidence'] = self.confidence
        return result

    @classmethod
    def from_dict(cls, data: dict) -> 'PersonalDataClassification':
        """Create from dictionary representation."""
        return cls(
            sensitivity=SensitivityLevel.from_string(data.get('sensitivity', 'UNDETERMINED')),
            explanation=data.get('explanation'),
            confidence=data.get('confidence'),
        )
