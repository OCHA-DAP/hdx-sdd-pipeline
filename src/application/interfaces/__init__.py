"""Application layer interfaces (ports) for dependency inversion."""

from .data_loader import IDataLoader
from .report_repository import IReportRepository

__all__ = [
    'IDataLoader',
    'IReportRepository',
]
