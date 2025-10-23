from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional, Union
import json
from datetime import datetime


# Entity Types for PII Classification
ENTITY_TYPES = [
    'person_name',
    'email_address',
    'phone_number',
    'address',
    'city',
    'country',
    'date',
    'product_name',
    'price',
    'unknown',
]


@dataclass
class PIIColumnReport:
    """Represents analysis details for a single column in the dataset."""

    column_name: str
    sample_values: List[str]
    pii: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class NonPIIReport:
    """Represents analysis details for the non-PII part of the dataset."""

    sensitivity: str
    explanation: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SDDReport:
    """Represents a full Sensitive Data Detection (SDD) report for one dataset."""

    resource_id: str
    file_name: str
    file_url: str
    processing_timestamp: str
    processing_success: bool
    n_records: int
    n_columns: int
    pii_classifier_model: Optional[str] = None
    pii_reflection_model: Optional[str] = None
    non_pii_classifier_model: Optional[str] = None
    pii_sensitive: bool = False
    non_pii_sensitive: bool = False
    columns: List[PIIColumnReport] = field(default_factory=list)
    non_pii: Optional[NonPIIReport] = None
    completion_tokens: int = 0
    prompt_tokens: int = 0

    def add_pii_column(self, column_report: PIIColumnReport):
        """Adds a new PII column report to the SDD."""
        self.columns.append(column_report)
        # If any column has sensitive PII, set the pii_sensitive flag to True
        if column_report.pii.get('sensitive', False):
            self.pii_sensitive = True

    def add_non_pii_report(self, non_pii_report: NonPIIReport):
        """Adds a new non-PII report to the SDD."""
        self.non_pii = non_pii_report
        # If the non-PII report mentions sensitivity, set the non_pii_sensitive flag to True
        if non_pii_report.sensitivity.lower() in ['high', 'high_sensitive']:
            self.non_pii_sensitive = True

    def update_pii_column(self, column_name: str, entity_type: str = None, sensitive: bool = None):
        """
        Update only the 'entity_type' or 'sensitive' fields for an existing PII column.
        If the column does not exist, nothing happens.
        """
        for column in self.columns:
            if column.column_name == column_name:
                if entity_type is not None:
                    column.pii['entity_type'] = entity_type
                if sensitive is not None:
                    column.pii['sensitive'] = sensitive
                    # Update the report-level flag if any column is sensitive
                    self.pii_sensitive = any(col.pii.get('sensitive', False) for col in self.columns)
                break

    def to_dict(self) -> Dict[str, Any]:
        """Convert the SDDReport to a nested dictionary."""
        return {
            'resource_id': self.resource_id,
            'file_name': self.file_name,
            'file_url': self.file_url,
            'processing_timestamp': self.processing_timestamp,
            'completion_tokens': self.completion_tokens,
            'prompt_tokens': self.prompt_tokens,
            'processing_success': self.processing_success,
            'n_records': self.n_records,
            'n_columns': self.n_columns,
            'pii_sensitive': self.pii_sensitive,
            'non_pii_sensitive': self.non_pii_sensitive,
            'columns': [column.to_dict() for column in self.columns],
            'non_pii': self.non_pii.to_dict() if self.non_pii else None,
        }

    def to_json(self, indent: int = 2) -> str:
        """Convert the SDDReport to a JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    @staticmethod
    def from_json(data: Union[str, dict]) -> 'SDDReport':
        """Create an SDDReport from a JSON string or dict."""
        if isinstance(data, str):
            data = json.loads(data)

        # Reconstruct PII columns
        columns = [
            PIIColumnReport(
                column_name=col['column_name'], sample_values=col.get('sample_values', []), pii=col.get('pii', {})
            )
            for col in data.get('columns', [])
        ]

        # Reconstruct non-PII report if present
        non_pii_data = data.get('non_pii')
        non_pii = None
        if non_pii_data:
            non_pii = NonPIIReport(
                sensitivity=non_pii_data.get('sensitivity', ''), explanation=non_pii_data.get('explanation', '')
            )

        # Build the SDDReport instance
        report = SDDReport(
            resource_id=data.get('resource_id', ''),
            file_name=data.get('file_name', ''),
            file_url=data.get('file_url', ''),
            processing_timestamp=data.get('processing_timestamp', ''),
            processing_success=data.get('processing_success', False),
            n_records=data.get('n_records', 0),
            n_columns=data.get('n_columns', 0),
            pii_sensitive=data.get('pii_sensitive', False),
            non_pii_sensitive=data.get('non_pii_sensitive', False),
            columns=columns,
            non_pii=non_pii,
            completion_tokens=data.get('completion_tokens', 0),
            prompt_tokens=data.get('prompt_tokens', 0),
        )

        return report


# ---------------------------
# Example usage
# ---------------------------
if __name__ == '__main__':
    # Sample metadata
    metadata = {'isp_used': 'default'}

    # Create the SDD report
    report = SDDReport(
        resource_id='1234567890',
        file_name='example.csv',
        file_url='https://example.com/example.csv',
        processing_timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        processing_success=True,
        n_records=100,
        n_columns=10,
    )

    # Add PII column reports
    report.add_pii_column(
        PIIColumnReport(
            column_name='email_address',
            sample_values=['john@example.com', 'jane@company.com'],
            pii={'entity_type': 'email_address', 'sensitive': True},
        )
    )

    # Add Non-PII report
    report.add_non_pii_report(
        NonPIIReport(
            sensitivity='LOW', explanation='The table contains email addresses, which are considered sensitive data.'
        )
    )

    # Print the JSON output
    print(report.to_json(indent=2))
