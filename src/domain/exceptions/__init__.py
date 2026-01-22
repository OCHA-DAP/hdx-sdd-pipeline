"""Domain exceptions for HDX SSD Pipeline."""


class DomainException(Exception):
    """Base exception for all domain-level errors."""

    pass


class ValidationError(DomainException):
    """Raised when domain validation fails."""

    pass


class ClassificationError(DomainException):
    """Raised when classification process fails."""

    pass


class DataProcessingError(DomainException):
    """Raised when data processing fails."""

    pass


class ConfigurationError(DomainException):
    """Raised when configuration is invalid."""

    pass


class ExternalServiceError(DomainException):
    """Raised when external service call fails."""

    pass


class LLMProviderError(ExternalServiceError):
    """Raised when LLM provider encounters an error."""

    pass
