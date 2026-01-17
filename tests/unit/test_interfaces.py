"""Unit tests for application interfaces."""

import pytest
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd

from src.application.interfaces.data_loader import IDataLoader
from src.application.interfaces.llm_provider import ILLMProvider
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


class TestILLMProvider:
    """Test suite for ILLMProvider interface."""

    def test_interface_cannot_be_instantiated(self):
        """Test that abstract interface cannot be instantiated directly."""
        with pytest.raises(TypeError):
            ILLMProvider()

    def test_concrete_implementation_must_implement_methods(self):
        """Test that concrete implementations must implement all abstract methods."""

        class IncompleteLLM(ILLMProvider):
            pass

        with pytest.raises(TypeError):
            IncompleteLLM()

    def test_valid_concrete_implementation(self):
        """Test that valid concrete implementation works."""

        class ValidLLM(ILLMProvider):
            def generate(
                self, prompt: str, max_tokens: int = 256, temperature: float = 0.0, **kwargs
            ) -> Tuple[str, int, int]:
                return ('response', 10, 20)

            def generate_json(
                self, prompt: str, max_tokens: int = 256, temperature: float = 0.0, **kwargs
            ) -> Tuple[Dict[str, Any], int, int]:
                return ({'key': 'value'}, 10, 20)

            @property
            def model_name(self) -> str:
                return 'test-model'

        # Should be able to instantiate
        llm = ValidLLM()
        assert isinstance(llm, ILLMProvider)
        assert llm.model_name == 'test-model'
        result, comp, prompt = llm.generate('test')
        assert result == 'response'


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
