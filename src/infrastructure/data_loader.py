"""Data loader implementation with smart preprocessing."""

import logging
import re
from typing import Dict, List, Any, Optional
from pathlib import Path
from contextlib import contextmanager
import tempfile
from urllib.parse import urlparse
import requests
import pandas as pd
import csv
from ..domain.exceptions import DataProcessingError

logger = logging.getLogger(__name__)

# Regex for number with comma as thousands separator (e.g. 1,234 or 1,234.56)
COMMA_NUMERIC_RE = re.compile(r'^[+-]?\d{1,3}(?:,\d{3})+(?:\.\d+)?$')

# Regex for number with space as thousands separator (e.g. 1 234 or 1 234.56, supporting multiple/Unicode spaces)
SPACE_NUMERIC_RE = re.compile(r'^[+-]?\d{1,3}(?:[\s\xa0\u202f]+\d{3})+(?:\.\d+)?$')


class SmartDataLoader:
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

    def __init__(
        self, max_rows: Optional[int] = None, user_agent: Optional[str] = None, hdx_base_url: Optional[str] = None
    ):
        """
        Initialize data loader.

        Args:
            max_rows: Optional maximum rows to load per sheet (None for all rows)
            user_agent: Optional default User-Agent header for outbound URL requests
            hdx_base_url: Optional HDX base URL used to scope Authorization header forwarding
        """
        self.max_rows = max_rows
        self.user_agent = user_agent
        self.hdx_base_url = hdx_base_url

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

    def load_from_url(self, url: str, http_headers: Dict[str, str] = None) -> Dict[str, pd.DataFrame]:
        """
        Load data from URL into dictionary of DataFrames.

        Args:
            url: URL to load data from
            http_headers: Optional HTTP headers for authentication

        Returns:
            Dictionary mapping sheet names to DataFrames

        Raises:
            DataProcessingError: If loading fails
        """
        logger.info(f'Loading data from URL: {url}')

        if not self.validate_url(url):
            logger.error(f'Unsupported file type for URL: {url}')
            raise DataProcessingError(
                f'Unsupported file type. Only {", ".join(self.SUPPORTED_EXTENSIONS)} are supported.'
            )

        if http_headers is None:
            http_headers = {}

        if self.user_agent:
            has_user_agent = any(k.lower() == 'user-agent' for k in http_headers)
            if not has_user_agent:
                http_headers = {**http_headers, 'User-Agent': self.user_agent}

        http_headers = self._sanitize_headers_for_url(url, http_headers)

        try:
            file_type = 'CSV' if url.lower().endswith('.csv') else 'Excel'
            logger.debug(f'Detected file type: {file_type}')

            suffix = '.csv' if url.lower().endswith('.csv') else '.xlsx'

            with self._download_to_tempfile(url, http_headers, suffix) as temp_file_path:
                logger.debug(f'Downloaded to temporary file: {temp_file_path}')

                if url.lower().endswith('.csv'):
                    result = self._load_csv(temp_file_path)
                else:
                    result = self._load_excel(temp_file_path)

            total_rows = sum(len(df) for df in result.values())
            total_cols = sum(len(df.columns) for df in result.values())
            logger.info(
                f'Successfully loaded {len(result)} sheet(s) from URL: '
                f'{total_rows} total rows, {total_cols} total columns'
            )
            return result

        except Exception as e:
            logger.error(f'Failed to load data from {url}: {e}', exc_info=True)
            raise DataProcessingError(f'Failed to load data: {e}')

    def load_from_file(self, file_path: str) -> Dict[str, pd.DataFrame]:
        """
        Load data from local file.

        Args:
            file_path: Path to local file

        Returns:
            Dictionary mapping sheet names to DataFrames
        """
        logger.info(f'Loading data from file: {file_path}')

        path = Path(file_path)
        if not path.exists():
            logger.error(f'File not found: {file_path}')
            raise DataProcessingError(f'File not found: {file_path}')

        if not self.validate_url(str(path)):
            logger.error(f'Unsupported file type: {file_path}')
            raise DataProcessingError(
                f'Unsupported file type. Only {", ".join(self.SUPPORTED_EXTENSIONS)} are supported.'
            )

        try:
            file_type = 'CSV' if str(path).lower().endswith('.csv') else 'Excel'
            try:
                file_size = path.stat().st_size / 1024  # KB
                logger.debug(f'File type: {file_type}, size: {file_size:.2f} KB')
            except OSError:
                logger.debug(f'File type: {file_type}')

            if str(path).lower().endswith('.csv'):
                result = self._load_csv(str(path))
            else:
                result = self._load_excel(str(path))

            total_rows = sum(len(df) for df in result.values())
            total_cols = sum(len(df.columns) for df in result.values())
            logger.info(
                f'Successfully loaded {len(result)} sheet(s) from file: '
                f'{total_rows} total rows, {total_cols} total columns'
            )
            return result

        except Exception as e:
            logger.error(f'Failed to load file {file_path}: {e}', exc_info=True)
            raise DataProcessingError(f'Failed to load file: {e}')

    def _sanitize_headers_for_url(self, url: str, http_headers: Dict[str, str]) -> Dict[str, str]:
        """Remove sensitive headers when destination is outside trusted HDX domains."""
        if not http_headers:
            return http_headers

        # HTTP header names are case-insensitive; detect any Authorization header regardless of casing.
        has_authorization = any(k.lower() == 'authorization' for k in http_headers.keys())
        if not has_authorization:
            return http_headers

        if self._is_hdx_domain(url):
            return http_headers

        # Remove all Authorization headers in a case-insensitive way.
        sanitized_headers = {k: v for k, v in http_headers.items() if k.lower() != 'authorization'}
        return sanitized_headers

    def _is_hdx_domain(self, url: str) -> bool:
        """Return True when URL host matches configured HDX host or one of its subdomains."""
        request_host = urlparse(url).hostname
        if not request_host or not self.hdx_base_url:
            return False

        hdx_host = urlparse(self.hdx_base_url).hostname
        if not hdx_host:
            return False

        request_host = request_host.lower()
        hdx_host = hdx_host.lower()
        return request_host == hdx_host or request_host.endswith(f'.{hdx_host}')

    def _detect_csv_delimiter(self, source: str) -> str:
        """Detect CSV delimiter, falling back to comma."""
        try:
            with open(source, 'r', encoding='utf-8', newline='') as f:
                sample = f.read(1024)
            dialect = csv.Sniffer().sniff(sample, delimiters=',;')
            logger.debug('Detected CSV delimiter: %r', dialect.delimiter)
            return dialect.delimiter
        except Exception as e:
            logger.debug('Could not detect delimiter, trying fallback: %s', e)

        try:
            with open(source, 'r', encoding='utf-8', newline='') as f:
                lines = [f.readline() for _ in range(5)]
            delimiter = ';' if sum(line.count(';') for line in lines) > sum(line.count(',') for line in lines) else ','
            logger.debug('Fallback delimiter detection: %r', delimiter)
            return delimiter
        except Exception as e:
            logger.debug('Fallback detection failed, defaulting to comma: %s', e)
            return ','

    @staticmethod
    def _convert_string_to_numeric(val: Any) -> Any:
        """Convert a string representation of a number to a real float or int."""
        if not isinstance(val, str):
            return val

        val_clean = val.strip()
        if not val_clean:
            return val

        # 1. Try direct integer conversion
        try:
            return int(val_clean)
        except ValueError:
            pass

        # 2. Try direct float conversion
        try:
            result = float(val_clean)
            if not (result != result or result == float('inf') or result == float('-inf')):
                return result
        except ValueError:
            pass

        # 3. Check for comma thousands separators (e.g., "3,466", "1,234,567.89")
        if COMMA_NUMERIC_RE.match(val_clean):
            val_no_comma = val_clean.replace(',', '')
            try:
                if '.' in val_no_comma:
                    return float(val_no_comma)
                else:
                    return int(val_no_comma)
            except ValueError:
                pass

        # 4. Check for space thousands separators (e.g., "3 466", "1 234 567.89")
        if SPACE_NUMERIC_RE.match(val_clean):
            val_no_space = re.sub(r'[\s\xa0\u202f]+', '', val_clean)
            try:
                if '.' in val_no_space:
                    return float(val_no_space)
                else:
                    return int(val_no_space)
            except ValueError:
                pass

        return val

    def _normalize_numeric_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert numeric strings in DataFrame to actual numbers."""
        if df.empty:
            return df
        if hasattr(df, 'map'):
            return df.map(self._convert_string_to_numeric)
        else:
            return df.applymap(self._convert_string_to_numeric)

    def _has_enough_unique_values(self, df_dict: Dict[str, pd.DataFrame], target_unique: int = 5) -> bool:
        """Check if all non-README sheets have at least target_unique unique values in every column."""
        for sheet_name, df in df_dict.items():
            normalized_name = sheet_name.lower().replace(' ', '')
            if any(k in normalized_name for k in ['readme', 'instructions', 'metadata', 'info']):
                continue
            if df.empty:
                continue
            for col in df.columns:
                col_data = df[col].dropna()
                if not col_data.empty:
                    str_mask = col_data.astype(str).str.strip().str.lower()
                    col_data = col_data[~str_mask.isin(['', 'nan', 'none'])]
                if col_data.nunique() < target_unique:
                    return False
        return True

    def _load_csv(self, source: str) -> Dict[str, pd.DataFrame]:
        """Load CSV file with chunked loading to find unique values."""
        logger.debug('Reading CSV file: %s (max_rows=%s)', source, self.max_rows)
        delimiter = self._detect_csv_delimiter(source)

        chunk_sizes = [100, 1000, 10000, 25000, 50000, 100000]
        if self.max_rows is not None:
            chunk_sizes = [c for c in chunk_sizes if c < self.max_rows] + [self.max_rows]

        final_df = None
        for chunk_size in chunk_sizes:
            logger.debug(f'Trying to load CSV with chunk size: {chunk_size}')
            try:
                df = pd.read_csv(source, header=None, nrows=chunk_size, delimiter=delimiter)
            except Exception as e:
                logger.error(f'Error reading CSV with chunk size {chunk_size}: {e}')
                if final_df is not None:
                    break
                raise

            raw_len = len(df)
            processed_df = self._preprocess_dataframe(df)
            processed_df = self._normalize_numeric_values(processed_df)
            final_df = processed_df

            # Check if we have enough unique values for all columns
            if self._has_enough_unique_values({'sheet1': processed_df}):
                logger.debug(f'Found enough unique values with chunk size {chunk_size}')
                break

            # If we reached the end of the file, we cannot load more rows
            if raw_len < chunk_size:
                logger.debug(f'Reached end of file at {raw_len} rows (chunk size was {chunk_size})')
                break

        if final_df is None:
            final_df = pd.DataFrame()

        return {'sheet1': final_df}

    def _load_excel(self, source: str) -> Dict[str, pd.DataFrame]:
        """Load Excel file with multiple sheets and chunked loading."""
        logger.debug(f'Reading Excel file: {source} (max_rows={self.max_rows})')

        chunk_sizes = [100, 1000, 10000, 25000, 50000, 100000]
        if self.max_rows is not None:
            chunk_sizes = [c for c in chunk_sizes if c < self.max_rows] + [self.max_rows]

        final_result = {}
        for chunk_size in chunk_sizes:
            logger.debug(f'Trying to load Excel with chunk size: {chunk_size}')
            try:
                df_dict = pd.read_excel(source, sheet_name=None, nrows=chunk_size, header=None)
            except Exception as e:
                logger.error(f'Error reading Excel with chunk size {chunk_size}: {e}')
                if final_result:
                    break
                raise

            processed_dict = {}
            max_raw_len = 0
            for sheet_name, df in df_dict.items():
                max_raw_len = max(max_raw_len, len(df))
                processed_df = self._preprocess_dataframe(df)
                processed_df = self._normalize_numeric_values(processed_df)
                processed_dict[sheet_name] = processed_df

            final_result = processed_dict

            # Check if all sheets/columns have enough unique values
            if self._has_enough_unique_values(processed_dict):
                logger.debug(f'Found enough unique values with chunk size {chunk_size}')
                break

            # If we reached the end of the file, stop
            if max_raw_len < chunk_size:
                logger.debug(f'Reached end of Excel file at {max_raw_len} rows (chunk size was {chunk_size})')
                break

        return final_result

    def _preprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess DataFrame with smart header detection and robust handling.

        Handles:
        - Leading empty rows
        - Multi-row hierarchical headers
        - Merged cells (forward fill)
        - Empty columns (filtered out)
        - Meaningful column naming

        Args:
            df: Raw DataFrame

        Returns:
            Preprocessed DataFrame with proper headers and cleaned data
        """
        if df.empty:
            return df

        # Step 1: Remove leading all-NaN rows
        while not df.empty and df.iloc[0].isna().all():
            df = df.iloc[1:].reset_index(drop=True)

        if df.empty:
            return df

        # Step 2: Detect header end using multiple heuristics
        header_end_row = self._detect_header_end(df)

        # Step 3: Extract and process header block
        header_block = df.iloc[: header_end_row + 1].copy()

        # Step 4: Build column names from multi-row headers
        final_columns = self._build_column_names(header_block)

        # Step 5: Extract data rows
        data_df = df.iloc[header_end_row + 1 :].copy()
        data_df.columns = final_columns

        # Step 6: Filter out completely empty columns
        data_df = self._filter_empty_columns(data_df)

        # Step 7: Sort by completeness (rows with most non-null values first)
        if not data_df.empty:
            data_df['__null_count__'] = data_df.isna().sum(axis=1)
            data_df = data_df.sort_values('__null_count__').drop(columns='__null_count__')

        return data_df.reset_index(drop=True)

    def _detect_header_end(self, df: pd.DataFrame) -> int:
        """
        Detect where the header ends and data begins.

        Uses multiple heuristics:
        1. First row where most cells are filled
        2. First row with consistent data types
        3. Row after which pattern repeats

        Args:
            df: Raw DataFrame

        Returns:
            Index of the last header row
        """
        max_check_rows = min(20, len(df))

        # Heuristic 1: Find first row with high fill rate (>70%)
        for idx in range(max_check_rows):
            row = df.iloc[idx]
            fill_rate = row.notna().sum() / len(row)

            # If this row is mostly filled and next few rows are also filled, it's likely data
            if fill_rate > 0.7 and idx < len(df) - 2:
                next_row_fill = df.iloc[idx + 1].notna().sum() / len(df.iloc[idx + 1])
                if next_row_fill > 0.7:
                    # Check if previous row looks like a header
                    if idx > 0:
                        return idx - 1
                    return idx

        # Heuristic 2: Look for rows with "Column" pattern (often metadata)
        for idx in range(max_check_rows):
            row = df.iloc[idx].astype(str)
            if any('column' in str(val).lower() for val in row if pd.notna(val)):
                # This is likely a metadata row, check next row
                if idx + 1 < len(df):
                    next_row = df.iloc[idx + 1]
                    if next_row.notna().sum() / len(next_row) > 0.7:
                        return idx

        # Fallback: First row where all cells are filled
        for idx in range(max_check_rows):
            row = df.iloc[idx]
            if row.notna().all():
                return idx

        # Last resort: assume first 3 rows are headers
        return min(2, len(df) - 1)

    def _build_column_names(self, header_block: pd.DataFrame) -> list:
        """
        Build meaningful column names from multi-row header block.

        Handles hierarchical headers by combining parent and child labels.

        Args:
            header_block: DataFrame containing header rows

        Returns:
            List of column names
        """
        if header_block.empty:
            return []

        # Convert to string and handle NaN
        header_block = header_block.fillna('').astype(str)

        # Forward fill within each row (for merged cells horizontally)
        header_block = header_block.apply(lambda row: row.replace('', pd.NA).ffill().fillna(''), axis=1)

        final_columns = []

        for col_idx in range(len(header_block.columns)):
            # Collect all non-empty values in this column across header rows
            col_values = []

            for row_idx in range(len(header_block)):
                val = header_block.iloc[row_idx, col_idx]

                # Clean the value
                val = str(val).strip()

                # Skip empty, 'None', 'nan', or 'Column' metadata
                if val and val.lower() not in ['none', 'nan', ''] and not val.lower().startswith('column'):
                    # Avoid duplicates (e.g., parent header repeated)
                    if not col_values or val != col_values[-1]:
                        col_values.append(val)

            # Build column name
            if col_values:
                # Join hierarchical parts with separator
                col_name = ' - '.join(col_values)
            else:
                col_name = f'Unnamed_Column_{col_idx}'

            final_columns.append(col_name)

        # Ensure unique column names
        final_columns = self._ensure_unique_names(final_columns)

        return final_columns

    def _ensure_unique_names(self, names: list) -> list:
        """
        Ensure all column names are unique by appending numbers to duplicates.

        Args:
            names: List of column names

        Returns:
            List of unique column names
        """
        seen = {}
        unique_names = []

        for name in names:
            if name not in seen:
                seen[name] = 0
                unique_names.append(name)
            else:
                seen[name] += 1
                unique_names.append(f'{name}_{seen[name]}')

        return unique_names

    def _filter_empty_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter out columns that are completely empty or have only whitespace.

        Args:
            df: DataFrame to filter

        Returns:
            DataFrame with empty columns removed
        """
        if df.empty:
            return df

        # Identify columns to keep
        cols_to_keep = []

        for col in df.columns:
            col_data = df[col]

            # Check if column has any non-null, non-empty values
            has_data = False
            for val in col_data:
                if pd.notna(val):
                    val_str = str(val).strip()
                    if val_str and val_str.lower() not in ['nan', 'none', '']:
                        has_data = True
                        break

            if has_data:
                cols_to_keep.append(col)

        # Return filtered DataFrame
        if cols_to_keep:
            return df[cols_to_keep].copy()
        else:
            # If all columns are empty, return empty DataFrame
            return pd.DataFrame()

    def sample_dataframe(self, df: pd.DataFrame, sample_size: int = 5) -> Dict[str, List[Any]]:
        """
        Sample unique values from DataFrame.

        Randomly samples unique non-empty/non-null values for each column using a random seed of 42.

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

            # Clean and get unique values
            clean_col = col_data.dropna()
            if not clean_col.empty:
                str_mask = clean_col.astype(str).str.strip().str.lower()
                clean_col = clean_col[~str_mask.isin(['', 'nan', 'none'])]

            # Drop duplicates to get unique values
            unique_series = clean_col.drop_duplicates()
            unique_list = unique_series.tolist()

            # Randomly sample from unique values if we have enough
            if len(unique_list) >= sample_size:
                sampled_series = pd.Series(unique_list).sample(n=sample_size, random_state=42)
                values = sampled_series.tolist()
            else:
                values = unique_list

            # Pad to sample_size if needed
            while len(values) < sample_size:
                values.append('')

            sample_dict[str(col)] = values[:sample_size]

        return sample_dict

    @contextmanager
    def _download_to_tempfile(self, url: str, http_headers: Dict[str, str], suffix: str):
        """Context manager that downloads a file to a temporary path using requests.

        Uses requests.get to handle authentication and redirects properly
        (the Authorization header is not forwarded to redirect targets,
        avoiding 400 errors from cloud storage).
        Yields the path to the temporary file and deletes it on exit.
        """
        response = requests.get(url, headers=http_headers, timeout=60)
        response.raise_for_status()

        with tempfile.NamedTemporaryFile(suffix=suffix, delete=True) as tmp_file:
            tmp_file.write(response.content)
            tmp_file.flush()
            yield tmp_file.name
