"""Data loader implementation with smart preprocessing."""

import logging
from typing import Dict, List, Any
from pathlib import Path
import pandas as pd
import requests

from ...application.interfaces.data_loader import IDataLoader
from ...domain.exceptions import DataProcessingError

logger = logging.getLogger(__name__)


class SmartDataLoader(IDataLoader):
    """
    Smart data loader with automatic preprocessing.

    Features:
    - Loads from URLs or local files
    - Handles CSV, Excel (XLS, XLSX)
    - Detects and concatenates multi-row headers
    - Handles multiple sheets
    - Smart sampling (most complete rows first)
    """

    SUPPORTED_EXTENSIONS = ('.csv', '.xls', '.xlsx')

    def __init__(self, max_rows: int = 1000):
        """
        Initialize data loader.

        Args:
            max_rows: Maximum rows to load per sheet
        """
        self.max_rows = max_rows

    def validate_url(self, url: str) -> bool:
        """
        Validate if URL is supported.

        Args:
            url: URL to validate

        Returns:
            True if URL is valid and supported
        """
        url_lower = url.lower()
        return any(url_lower.endswith(ext) for ext in self.SUPPORTED_EXTENSIONS)

    def load_from_url(self, url: str) -> Dict[str, pd.DataFrame]:
        """
        Load data from URL into dictionary of DataFrames.

        Args:
            url: URL to load data from

        Returns:
            Dictionary mapping sheet names to DataFrames

        Raises:
            DataProcessingError: If loading fails
        """
        if not self.validate_url(url):
            raise DataProcessingError(
                f"Unsupported file type. Only {', '.join(self.SUPPORTED_EXTENSIONS)} are supported."
            )

        try:
            if url.lower().endswith('.csv'):
                return self._load_csv(url)
            else:
                return self._load_excel(url)
        except Exception as e:
            logger.error(f"Failed to load data from {url}: {e}")
            raise DataProcessingError(f"Failed to load data: {e}")

    def load_from_file(self, file_path: str) -> Dict[str, pd.DataFrame]:
        """
        Load data from local file.

        Args:
            file_path: Path to local file

        Returns:
            Dictionary mapping sheet names to DataFrames
        """
        path = Path(file_path)
        if not path.exists():
            raise DataProcessingError(f"File not found: {file_path}")

        if not self.validate_url(str(path)):
            raise DataProcessingError(
                f"Unsupported file type. Only {', '.join(self.SUPPORTED_EXTENSIONS)} are supported."
            )

        try:
            if str(path).lower().endswith('.csv'):
                return self._load_csv(str(path))
            else:
                return self._load_excel(str(path))
        except Exception as e:
            logger.error(f"Failed to load file {file_path}: {e}")
            raise DataProcessingError(f"Failed to load file: {e}")

    def _load_csv(self, source: str) -> Dict[str, pd.DataFrame]:
        """Load CSV file."""
        df = pd.read_csv(source, header=None, nrows=self.max_rows)
        df = self._preprocess_dataframe(df)
        return {'sheet1': df}

    def _load_excel(self, source: str) -> Dict[str, pd.DataFrame]:
        """Load Excel file with multiple sheets."""
        df_dict = pd.read_excel(source, sheet_name=None, nrows=self.max_rows, header=None)
        return {sheet_name: self._preprocess_dataframe(df) for sheet_name, df in df_dict.items()}

    def _preprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess DataFrame with smart header detection.

        Handles:
        - Leading empty rows
        - Multi-row headers
        - Forward fill for merged cells

        Args:
            df: Raw DataFrame

        Returns:
            Preprocessed DataFrame with proper headers
        """
        if df.empty:
            return df

        # Remove leading all-NaN rows
        while not df.empty and df.iloc[0].isna().all():
            df = df.iloc[1:].reset_index(drop=True)

        if df.empty:
            return df

        # Detect header end (first complete row)
        header_end_row = 0
        for idx in range(min(10, len(df))):  # Check first 10 rows max
            row = df.iloc[idx]
            if row.notna().all():
                header_end_row = idx
                break

        # Extract and process header block
        header_block = df.iloc[: header_end_row + 1].fillna('').astype(str)

        # Forward fill within each row (for merged cells)
        header_block = header_block.apply(lambda row: row.replace('', None).ffill(), axis=1)

        # Forward fill down (for multi-row headers)
        header_block = header_block.replace('', None).ffill()

        # Combine multi-row headers
        final_columns = header_block.apply(lambda col: ' | '.join([v for v in col if v and v != 'None']), axis=0)

        # Data starts after header block
        data_df = df.iloc[header_end_row + 1 :].copy()
        data_df.columns = final_columns

        # Sort by completeness (rows with most non-null values first)
        data_df['__null_count__'] = data_df.isna().sum(axis=1)
        data_df = data_df.sort_values('__null_count__').drop(columns='__null_count__')

        return data_df.reset_index(drop=True)

    def sample_dataframe(self, df: pd.DataFrame, sample_size: int = 5) -> Dict[str, List[Any]]:
        """
        Sample values from DataFrame.

        Takes the most complete rows (fewest nulls) and samples from them.

        Args:
            df: DataFrame to sample from
            sample_size: Number of samples per column

        Returns:
            Dictionary mapping column names to sample values
        """
        if df.empty:
            return {}

        sample_dict = {}

        for col in df.columns:
            col_data = df[col]

            # Drop empty values
            non_empty = col_data.dropna()
            non_empty = non_empty[non_empty != '']

            # Take top N values
            values = non_empty.head(sample_size).values.ravel().tolist()

            # Pad to sample_size if needed
            while len(values) < sample_size:
                values.append('')

            sample_dict[str(col)] = values[:sample_size]

        return sample_dict
