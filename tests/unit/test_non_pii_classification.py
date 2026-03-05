"""Unit tests for NonPIIClassification entity."""

from src.domain.entities.non_pii_classification import NonPIIClassification
from src.domain.value_objects.sensitivity import SensitivityLevel


class TestNonPIIClassification:
    """Test suite for NonPIIClassification."""

    def test_default_initialization(self):
        """Test default initialization."""
        classification = NonPIIClassification()

        assert classification.sensitivity == SensitivityLevel.UNDETERMINED
        assert classification.sensitive_columns is None
        assert classification.cited_isp_rules is None
        assert classification.explanation is None
        assert classification.confidence is None

    def test_initialization_with_values(self):
        """Test initialization with values."""
        classification = NonPIIClassification(
            sensitivity=SensitivityLevel.HIGH_SENSITIVE,
            sensitive_columns=['email', 'phone'],
            cited_isp_rules=['Personal data identifiers are HIGH_SENSITIVE'],
            explanation='Contains sensitive operational data',
            confidence=0.92,
        )

        assert classification.sensitivity == SensitivityLevel.HIGH_SENSITIVE
        assert classification.sensitive_columns == ['email', 'phone']
        assert classification.cited_isp_rules == ['Personal data identifiers are HIGH_SENSITIVE']
        assert classification.explanation == 'Contains sensitive operational data'
        assert classification.confidence == 0.92

    def test_is_sensitive_true(self):
        """Test is_sensitive returns True for sensitive levels."""
        classification = NonPIIClassification(sensitivity=SensitivityLevel.HIGH_SENSITIVE)

        assert classification.is_sensitive() is True

    def test_is_sensitive_false(self):
        """Test is_sensitive returns False for non-sensitive levels."""
        classification = NonPIIClassification(sensitivity=SensitivityLevel.NON_SENSITIVE)

        assert classification.is_sensitive() is False

    def test_to_dict_basic(self):
        """Test to_dict with basic fields."""
        classification = NonPIIClassification(sensitivity=SensitivityLevel.MODERATE_SENSITIVE)

        result = classification.to_dict()

        assert result['sensitivity'] == 'MODERATE_SENSITIVE'
        assert 'sensitive_columns' not in result
        assert 'cited_isp_rules' not in result
        assert 'explanation' not in result
        assert 'confidence' not in result

    def test_to_dict_with_explanation(self):
        """Test to_dict with explanation."""
        classification = NonPIIClassification(
            sensitivity=SensitivityLevel.HIGH_SENSITIVE, explanation='Operational security data'
        )

        result = classification.to_dict()

        assert result['explanation'] == 'Operational security data'

    def test_to_dict_with_confidence(self):
        """Test to_dict with confidence."""
        classification = NonPIIClassification(sensitivity=SensitivityLevel.SEVERE_SENSITIVE, confidence=0.98)

        result = classification.to_dict()

        assert result['confidence'] == 0.98

    def test_to_dict_complete(self):
        """Test to_dict with all fields."""
        classification = NonPIIClassification(
            sensitivity=SensitivityLevel.HIGH_SENSITIVE,
            sensitive_columns=['email', 'phone_number'],
            cited_isp_rules=['Personal identifiers are HIGH_SENSITIVE', 'Contact information is MODERATE_SENSITIVE'],
            explanation='Security-related information',
            confidence=0.95,
        )

        result = classification.to_dict()

        assert result['sensitivity'] == 'HIGH_SENSITIVE'
        assert result['sensitive_columns'] == ['email', 'phone_number']
        assert result['cited_isp_rules'] == [
            'Personal identifiers are HIGH_SENSITIVE',
            'Contact information is MODERATE_SENSITIVE',
        ]
        assert result['explanation'] == 'Security-related information'
        assert result['confidence'] == 0.95

    def test_from_dict_basic(self):
        """Test from_dict with basic fields."""
        data = {'sensitivity': 'MODERATE_SENSITIVE'}

        classification = NonPIIClassification.from_dict(data)

        assert classification.sensitivity == SensitivityLevel.MODERATE_SENSITIVE
        assert classification.sensitive_columns is None
        assert classification.cited_isp_rules is None
        assert classification.explanation is None
        assert classification.confidence is None

    def test_from_dict_with_explanation(self):
        """Test from_dict with explanation."""
        data = {'sensitivity': 'HIGH_SENSITIVE', 'explanation': 'Access control data'}

        classification = NonPIIClassification.from_dict(data)

        assert classification.explanation == 'Access control data'

    def test_from_dict_with_confidence(self):
        """Test from_dict with confidence."""
        data = {'sensitivity': 'SEVERE_SENSITIVE', 'confidence': 0.99}

        classification = NonPIIClassification.from_dict(data)

        assert classification.confidence == 0.99

    def test_from_dict_complete(self):
        """Test from_dict with all fields."""
        data = {
            'sensitivity': 'HIGH_SENSITIVE',
            'sensitive_columns': ['email', 'phone'],
            'cited_isp_rules': ['Personal identifiers are HIGH_SENSITIVE'],
            'explanation': 'Military operations data',
            'confidence': 0.97,
        }

        classification = NonPIIClassification.from_dict(data)

        assert classification.sensitivity == SensitivityLevel.HIGH_SENSITIVE
        assert classification.sensitive_columns == ['email', 'phone']
        assert classification.cited_isp_rules == ['Personal identifiers are HIGH_SENSITIVE']
        assert classification.explanation == 'Military operations data'
        assert classification.confidence == 0.97

    def test_from_dict_empty(self):
        """Test from_dict with empty dict."""
        classification = NonPIIClassification.from_dict({})

        assert classification.sensitivity == SensitivityLevel.UNDETERMINED
        assert classification.sensitive_columns is None
        assert classification.cited_isp_rules is None
        assert classification.explanation is None
        assert classification.confidence is None

    def test_round_trip_serialization(self):
        """Test that to_dict and from_dict are inverses."""
        original = NonPIIClassification(
            sensitivity=SensitivityLevel.SEVERE_SENSITIVE,
            sensitive_columns=['email', 'address'],
            cited_isp_rules=['Personal identifiers are SEVERE_SENSITIVE'],
            explanation='Highly sensitive operational data',
            confidence=0.96,
        )

        data = original.to_dict()
        restored = NonPIIClassification.from_dict(data)

        assert restored.sensitivity == original.sensitivity
        assert restored.sensitive_columns == original.sensitive_columns
        assert restored.cited_isp_rules == original.cited_isp_rules
        assert restored.explanation == original.explanation
        assert restored.confidence == original.confidence
