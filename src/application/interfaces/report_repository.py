"""Report Repository interface."""

from abc import ABC, abstractmethod
from typing import List, Optional

from ...domain.entities.sheet_report import SheetReport


class IReportRepository(ABC):
    """
    Interface for report persistence.

    This abstraction allows us to support different storage backends
    (CKAN, database, file system, etc.) without changing business logic.
    """

    @abstractmethod
    def exists(self, resource_id: str) -> bool:
        """
        Check if report exists for a resource.

        Args:
            resource_id: Resource identifier

        Returns:
            True if report exists
        """
        pass

    @abstractmethod
    def get(self, resource_id: str) -> Optional[List[SheetReport]]:
        """
        Retrieve reports for a resource.

        Args:
            resource_id: Resource identifier

        Returns:
            List of sheet reports if found, None otherwise
        """
        pass
