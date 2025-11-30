# utils/processing.py
from typing import Dict
import pandas as pd

from utils.exception_handler import handle_exception


class DataSampler:
    """
    Utility class to load a dataset (CSV/XLS/XLSX) directly from a URL and sample random records.
    """

    SUPPORTED_EXTENSIONS = ('.csv', '.xls', '.xlsx')

    @handle_exception
    def _validate_url(self, url: str) -> str:
        """Check if the URL points to a supported file type."""
        url_lower = url.lower()
        if not any(url_lower.endswith(ext) for ext in self.SUPPORTED_EXTENSIONS):
            raise ValueError(f'Unsupported file type. Only {", ".join(self.SUPPORTED_EXTENSIONS)} are supported.')
        return url_lower

    @handle_exception
    def _load_from_url(self, url: str) -> Dict[str, pd.DataFrame]:
        """Load CSV/XLS/XLSX from a URL into a dictionary of DataFrames keyed by sheet name."""
        url = self._validate_url(url)
        if url.endswith('.csv'):
            df = pd.read_csv(url, header=None, nrows=200)
            df = self._concatenate_header(df)
            return {'sheet1': df}

        # Excel files: can contain multiple sheets
        df_dict = pd.read_excel(url, sheet_name=None, nrows=200, header=None)
        return {sheet_name: self._concatenate_header(df) for sheet_name, df in df_dict.items()}

    @handle_exception
    def _concatenate_header(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Attempts to detect header rows and flatten them into single column names.
        If no header pattern is found, uses the first row as header.
        """
        header_end_row = None
        for idx, row in df.iterrows():
            if row.notna().all():
                header_end_row = idx
                break

        if header_end_row is None:
            # Fallback: treat first row as header
            header_end_row = 0

        # Extract header block and fill missing cells
        header_block = df.iloc[: header_end_row + 1].fillna('').astype(str)
        header_block = header_block.apply(lambda row: row.replace('', None).ffill(), axis=1)
        header_block = header_block.replace('', None).ffill()

        # Combine multi-row headers
        final_columns = header_block.apply(lambda col: ' | '.join([v for v in col if v]), axis=0)

        cleaned_df = df.iloc[header_end_row + 1 :].copy()
        cleaned_df.columns = final_columns
        return cleaned_df.reset_index(drop=True)

    @handle_exception
    def _sample_dataframe(self, df: pd.DataFrame, sample_size: int = 20) -> pd.DataFrame:
        """Return a random sample of rows, preferring complete rows if available."""
        if df.empty:
            return df

        n = min(sample_size, len(df))
        complete_rows = df[df.notna().all(axis=1)]
        incomplete_rows = df[df.isna().any(axis=1)]

        if len(complete_rows) >= n:
            sample = complete_rows.sample(n=n, random_state=42)
        else:
            needed = n - len(complete_rows)
            incomplete_rows = incomplete_rows.assign(null_count=incomplete_rows.isna().sum(axis=1))
            incomplete_rows = incomplete_rows.sort_values('null_count')
            fallback_rows = incomplete_rows.drop(columns='null_count').head(needed)
            sample = pd.concat([complete_rows, fallback_rows]).sample(frac=1, random_state=42)

        return sample.reset_index(drop=True)

    @handle_exception
    def sample(self, url: str, sample_size: int = 20) -> Dict[str, pd.DataFrame]:
        """Main entrypoint: load and sample dataset(s) from a URL."""
        sheets = self._load_from_url(url)
        return {name: self._sample_dataframe(df, sample_size) for name, df in sheets.items()}
