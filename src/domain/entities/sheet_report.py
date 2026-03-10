"""Sheet Report entity representing a complete sheet/table analysis."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any

from .column import Column
from .non_pii_classification import NonPIIClassification
from .personal_data_classification import PersonalDataClassification


@dataclass
class SheetReport:
    """
    Represents a complete analysis report for a single sheet/table.

    This is the main aggregate root that contains all classification
    results for a dataset sheet.
    """

    # Identification
    resource_id: Optional[str] = None
    file_name: str = ''
    file_url: Optional[str] = None
    sheet_name: str = 'sheet1'

    # Metadata
    processing_timestamp: datetime = field(default_factory=datetime.now)
    processing_success: bool = True
    n_records: int = 0
    n_columns: int = 0

    # Token usage
    completion_tokens: int = 0
    prompt_tokens: int = 0

    # Model information
    pii_classifier_model: Optional[str] = None
    pii_reflection_model: Optional[str] = None
    non_pii_model: Optional[str] = None
    readme_model: Optional[str] = None

    # Classifications
    columns: List[Column] = field(default_factory=list)
    non_pii_classification: NonPIIClassification = field(default_factory=NonPIIClassification)
    personal_data_classification: PersonalDataClassification = field(default_factory=PersonalDataClassification)

    # Sensitivity flags
    personal_data_sensitive: bool = False
    non_personal_data_sensitive: bool = False

    # Error handling
    error_source: Optional[str] = None
    error_message: Optional[str] = None

    # Special cases
    is_readme: bool = False
    readme_content: Optional[str] = None
    readme_report: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        """Validate and normalize data after initialization."""
        if not self.file_name:
            raise ValueError('file_name is required')

        # Update n_columns if columns are provided
        if self.columns and self.n_columns == 0:
            self.n_columns = len(self.columns)

    def add_column(self, column: Column) -> None:
        """Add a column to the report."""
        self.columns.append(column)
        self.n_columns = len(self.columns)

    def has_pii_columns(self) -> bool:
        """Check if any column contains PII."""
        return any(col.has_pii() for col in self.columns)

    def has_sensitive_pii(self) -> bool:
        """Check if any column contains sensitive PII."""
        return any(col.is_sensitive() for col in self.columns)

    def update_pii_sensitivity(self) -> None:
        """Update the personal_data_sensitive flag based on column classifications."""
        self.personal_data_sensitive = self.has_sensitive_pii()

    def update_non_pii_sensitivity(self) -> None:
        """Update the non_personal_data_sensitive flag based on non-PII classification."""
        self.non_personal_data_sensitive = self.non_pii_classification.is_sensitive()

    def is_sensitive(self) -> bool:
        """Check if the sheet is sensitive (PII or non-PII)."""
        return self.personal_data_sensitive or self.non_personal_data_sensitive

    def total_tokens(self) -> int:
        """Calculate total tokens used."""
        return self.completion_tokens + self.prompt_tokens

    def has_error(self) -> bool:
        """Check if processing encountered an error."""
        return self.error_source is not None

    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary representation."""
        result = {
            'resource_id': self.resource_id,
            'file_name': self.file_name,
            'file_url': self.file_url,
            'sheet_name': self.sheet_name,
            'processing_timestamp': self.processing_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'processing_success': self.processing_success,
            'n_records': self.n_records,
            'n_columns': self.n_columns,
            'completion_tokens': self.completion_tokens,
            'prompt_tokens': self.prompt_tokens,
            'personal_data_sensitive': self.personal_data_sensitive,
            'non_personal_data_sensitive': self.non_personal_data_sensitive,
            'personal_data': self.personal_data_classification.to_dict(),
            'columns': [col.to_dict() for col in self.columns],
            'non_personal_data': self.non_pii_classification.to_dict(),
        }

        # Optional fields
        if self.pii_classifier_model:
            result['pii_classifier_model'] = self.pii_classifier_model
        if self.pii_reflection_model:
            result['pii_reflection_model'] = self.pii_reflection_model
        if self.non_pii_model:
            result['non_pii_model'] = self.non_pii_model
        if self.readme_model:
            result['readme_model'] = self.readme_model
        if self.error_source:
            result['error_source'] = self.error_source
        if self.error_message:
            result['error_message'] = self.error_message
        if self.is_readme:
            result['is_readme'] = self.is_readme
        if self.readme_report:
            result['readme_report'] = self.readme_report

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SheetReport':
        """Create SheetReport from dictionary representation."""
        # Parse timestamp
        timestamp_str = data.get('processing_timestamp')
        if isinstance(timestamp_str, str):
            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        else:
            timestamp = datetime.now()

        # Parse columns
        columns = [Column.from_dict(col) for col in data.get('columns', [])]

        # Parse non-PII classification (support both old and new keys)
        non_pii_data = data.get('non_personal_data', data.get('non_pii', {}))
        non_pii_classification = NonPIIClassification.from_dict(non_pii_data)

        # Parse personal data classification
        personal_data_data = data.get('personal_data', {})
        personal_data_classification = PersonalDataClassification.from_dict(personal_data_data)

        return cls(
            resource_id=data.get('resource_id'),
            file_name=data.get('file_name', ''),
            file_url=data.get('file_url'),
            sheet_name=data.get('sheet_name', 'sheet1'),
            processing_timestamp=timestamp,
            processing_success=data.get('processing_success', True),
            n_records=data.get('n_records', 0),
            n_columns=data.get('n_columns', 0),
            completion_tokens=data.get('completion_tokens', 0),
            prompt_tokens=data.get('prompt_tokens', 0),
            pii_classifier_model=data.get('pii_classifier_model'),
            pii_reflection_model=data.get('pii_reflection_model'),
            non_pii_model=data.get('non_pii_model'),
            readme_model=data.get('readme_model'),
            columns=columns,
            non_pii_classification=non_pii_classification,
            personal_data_classification=personal_data_classification,
            personal_data_sensitive=data.get('personal_data_sensitive', False),
            non_personal_data_sensitive=data.get('non_personal_data_sensitive', False),
            error_source=data.get('error_source'),
            error_message=data.get('error_message'),
            is_readme=data.get('is_readme', False),
            readme_content=data.get('readme'),
            readme_report=data.get('readme_report', data.get('readme_report')),
        )
