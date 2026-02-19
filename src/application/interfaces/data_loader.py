"""Data Loader interface."""

from abc import ABC, abstractmethod
from typing import Dict, List
import pandas as pd


class IDataLoader(ABC):
    """
    Interface for loading and sampling data from various sources.

    This abstraction allows us to support different data sources
    (URLs, local files, S3, etc.) without changing business logic.
    """

    @abstractmethod
    def load_from_url(self, url: str) -> Dict[str, pd.DataFrame]:
        """
        Load data from URL into dictionary of DataFrames.

        Args:
            url: URL to load data from

        Returns:
            Dictionary mapping sheet names to DataFrames
        """
        pass

    @abstractmethod
    def sample_dataframe(self, df: pd.DataFrame, sample_size: int = 5) -> Dict[str, List]:
        """
        Sample values from DataFrame.

        Args:
            df: DataFrame to sample from
            sample_size: Number of samples per column

        Returns:
            Dictionary mapping column names to sample values
        """
        pass

    @abstractmethod
    def _validate_url(self, url: str) -> bool:
        """
        Validate if URL is supported.

        Args:
            url: URL to validate

        Returns:
            True if URL is valid and supported
        """
        pass
