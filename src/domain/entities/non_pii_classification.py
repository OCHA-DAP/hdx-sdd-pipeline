"""Non-PII Classification entity."""

from dataclasses import dataclass
from typing import Optional, List

from ..value_objects.sensitivity import SensitivityLevel


@dataclass
class NonPIIClassification:
    """Non-PII classification result for a sheet/table."""

    sensitivity: SensitivityLevel = SensitivityLevel.UNDETERMINED
    sensitive_columns: Optional[List[str]] = None
    cited_isp_rules: Optional[List[str]] = None
    explanation: Optional[str] = None
    confidence: Optional[float] = None
    isp_name: Optional[str] = None  # Name/title of the ISP used

    def is_sensitive(self) -> bool:
        """Check if classified as sensitive."""
        return self.sensitivity.is_sensitive()

    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        result = {
            'sensitivity': str(self.sensitivity),
        }
        if self.sensitive_columns:
            result['sensitive_columns'] = self.sensitive_columns
        if self.cited_isp_rules:
            result['cited_isp_rules'] = self.cited_isp_rules
        if self.explanation:
            result['explanation'] = self.explanation
        if self.confidence is not None:
            result['confidence'] = self.confidence
        if self.isp_name:
            result['isp_name'] = self.isp_name
        return result

    @classmethod
    def from_dict(cls, data: dict) -> 'NonPIIClassification':
        """Create from dictionary representation."""
        return cls(
            sensitivity=SensitivityLevel.from_string(data.get('sensitivity', 'UNDETERMINED')),
            sensitive_columns=data.get('sensitive_columns'),
            cited_isp_rules=data.get('cited_isp_rules'),
            explanation=data.get('explanation'),
            confidence=data.get('confidence'),
            isp_name=data.get('isp_name'),
        )
