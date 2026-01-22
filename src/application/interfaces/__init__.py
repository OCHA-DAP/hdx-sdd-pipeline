"""Application layer interfaces (ports) for dependency inversion."""

from .llm_provider import ILLMProvider
from .data_loader import IDataLoader
from .report_repository import IReportRepository

__all__ = [
    'ILLMProvider',
    'IDataLoader',
    'IReportRepository',
]
