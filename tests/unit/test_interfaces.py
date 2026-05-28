"""Unit tests for application interfaces."""

import pytest
from typing import Dict, List, Optional
import pandas as pd

from src.application.interfaces.data_loader import IDataLoader
from src.application.interfaces.report_repository import IReportRepository
from src.domain.entities.sheet_report import SheetReport


class TestIDataLoader:
    """Test suite for IDataLoader interface."""

    def test_interface_cannot_be_instantiated(self):
        """Test that abstract interface cannot be instantiated directly."""
        with pytest.raises(TypeError):
            IDataLoader()

    def test_concrete_implementation_must_implement_methods(self):
        """Test that concrete implementations must implement all abstract methods."""

        # Create incomplete implementation
        class IncompleteLoader(IDataLoader):
            pass

        with pytest.raises(TypeError):
            IncompleteLoader()

    def test_valid_concrete_implementation(self):
        """Test that valid concrete implementation works."""

        class ValidLoader(IDataLoader):
            def load_from_url(self, url: str) -> Dict[str, pd.DataFrame]:
                return {}

            def sample_dataframe(self, df: pd.DataFrame, sample_size: int = 5) -> Dict[str, List]:
                return {}

            def validate_url(self, url: str) -> bool:
                return True

        # Should be able to instantiate
        loader = ValidLoader()
        assert isinstance(loader, IDataLoader)
        assert loader.validate_url('test.csv') is True


class TestIReportRepository:
    """Test suite for IReportRepository interface."""

    def test_interface_cannot_be_instantiated(self):
        """Test that abstract interface cannot be instantiated directly."""
        with pytest.raises(TypeError):
            IReportRepository()

    def test_concrete_implementation_must_implement_methods(self):
        """Test that concrete implementations must implement all abstract methods."""

        class IncompleteRepo(IReportRepository):
            pass

        with pytest.raises(TypeError):
            IncompleteRepo()

    def test_valid_concrete_implementation(self):
        """Test that valid concrete implementation works."""

        class ValidRepo(IReportRepository):
            def save(self, reports: List[SheetReport], resource_id: str) -> bool:
                return True

            def exists(self, resource_id: str) -> bool:
                return False

            def get(self, resource_id: str) -> Optional[List[SheetReport]]:
                return None

            def delete(self, resource_id: str) -> bool:
                return True

        # Should be able to instantiate
        repo = ValidRepo()
        assert isinstance(repo, IReportRepository)
        assert repo.save([], 'test-id') is True
        assert repo.exists('test-id') is False
