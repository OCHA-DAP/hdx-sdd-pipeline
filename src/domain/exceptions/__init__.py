"""Domain exceptions for HDX SSD Pipeline."""


class DomainException(Exception):
    """Base exception for all domain-level errors."""

    pass


class DataProcessingError(DomainException):
    """Raised when data processing fails."""

    pass


class ExternalServiceError(DomainException):
    """Raised when external service call fails."""

    pass


class LLMProviderError(ExternalServiceError):
    """Raised when LLM provider encounters an error."""

    pass
