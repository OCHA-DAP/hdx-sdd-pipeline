"""Unit tests for PIIClassification entity."""

from src.domain.entities.pii_classification import PIIClassification
from src.domain.value_objects.entity_type import PIIEntityType


class TestPIIClassification:
    """Test suite for PIIClassification."""

    def test_default_initialization(self):
        """Test default initialization."""
        classification = PIIClassification()

        assert classification.entity_type == PIIEntityType.NONE
        assert classification.sensitive is False
        assert classification.confidence is None
        assert classification.explanation is None

    def test_initialization_with_values(self):
        """Test initialization with values."""
        classification = PIIClassification(
            entity_type=PIIEntityType.EMAIL_ADDRESS,
            sensitive=True,
            confidence=0.95,
            explanation='Contains email addresses',
        )

        assert classification.entity_type == PIIEntityType.EMAIL_ADDRESS
        assert classification.sensitive is True
        assert classification.confidence == 0.95
        assert classification.explanation == 'Contains email addresses'

    def test_to_dict_basic(self):
        """Test to_dict with basic fields."""
        classification = PIIClassification(entity_type=PIIEntityType.PERSON_NAME, sensitive=True)

        result = classification.to_dict()

        assert result['entity_type'] == 'PERSON_NAME'
        assert 'confidence' not in result
        assert 'explanation' not in result

    def test_to_dict_with_confidence(self):
        """Test to_dict with confidence."""
        classification = PIIClassification(entity_type=PIIEntityType.PHONE_NUMBER, sensitive=True, confidence=0.88)

        result = classification.to_dict()

        assert result['confidence'] == 0.88

    def test_to_dict_with_explanation(self):
        """Test to_dict with explanation."""
        classification = PIIClassification(
            entity_type=PIIEntityType.LOCATION, sensitive=False, explanation='General location data'
        )

        result = classification.to_dict()

        assert result['explanation'] == 'General location data'

    def test_to_dict_complete(self):
        """Test to_dict with all fields."""
        classification = PIIClassification(
            entity_type=PIIEntityType.EMAIL_ADDRESS,
            sensitive=True,
            confidence=0.99,
            explanation='Email column detected',
        )

        result = classification.to_dict()

        assert result['entity_type'] == 'EMAIL_ADDRESS'
        assert result['entity_type'] == 'EMAIL_ADDRESS'
        assert result['confidence'] == 0.99
        assert result['explanation'] == 'Email column detected'

    def test_from_dict_basic(self):
        """Test from_dict with basic fields."""
        data = {'entity_type': 'PERSON_NAME', 'sensitive': True}

        classification = PIIClassification.from_dict(data)

        assert classification.entity_type == PIIEntityType.PERSON_NAME
        assert classification.sensitive is True
        assert classification.confidence is None
        assert classification.explanation is None

    def test_from_dict_with_confidence(self):
        """Test from_dict with confidence."""
        data = {'entity_type': 'PHONE_NUMBER', 'sensitive': True, 'confidence': 0.92}

        classification = PIIClassification.from_dict(data)

        assert classification.confidence == 0.92

    def test_from_dict_with_explanation(self):
        """Test from_dict with explanation."""
        data = {'entity_type': 'LOCATION', 'sensitive': False, 'explanation': 'City names'}

        classification = PIIClassification.from_dict(data)

        assert classification.explanation == 'City names'

    def test_from_dict_complete(self):
        """Test from_dict with all fields."""
        data = {
            'entity_type': 'EMAIL_ADDRESS',
            'sensitive': True,
            'confidence': 0.98,
            'explanation': 'Email addresses detected',
        }

        classification = PIIClassification.from_dict(data)

        assert classification.entity_type == PIIEntityType.EMAIL_ADDRESS
        assert classification.sensitive is True
        assert classification.confidence == 0.98
        assert classification.explanation == 'Email addresses detected'

    def test_from_dict_empty(self):
        """Test from_dict with empty dict."""
        classification = PIIClassification.from_dict({})

        assert classification.entity_type == PIIEntityType.NONE
        assert classification.sensitive is False
        assert classification.confidence is None
        assert classification.explanation is None

    def test_round_trip_serialization(self):
        """Test that to_dict and from_dict are inverses."""
        original = PIIClassification(
            entity_type=PIIEntityType.PHONE_NUMBER, sensitive=True, confidence=0.95, explanation='Phone numbers found'
        )

        data = original.to_dict()
        restored = PIIClassification.from_dict(data)

        assert restored.entity_type == original.entity_type
        assert restored.sensitive == original.sensitive  # Sensitive flag is now properly serialized
        assert restored.confidence == original.confidence
        assert restored.explanation == original.explanation
