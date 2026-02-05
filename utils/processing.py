# utils/processing.py
from typing import Dict
import pandas as pd
from utils.exception_handler import handle_exception_wrap
import datetime
import logging

logger = logging.getLogger(__name__)


def is_readme_sheet(sheet_name: str) -> bool:
    normalized = sheet_name.lower().replace(' ', '')
    return 'readme' in normalized


def create_report(url: str, resource_id: str = None, download_url: str = None, sample_size: int = 5):
    sampler = DataSampler()
    sheets = sampler.load_from_url(url)
    new_sample_dict = {}
    for name, df in sheets.items():
        logger.debug(f'Processing sheet: {name}')
        if not is_readme_sheet(name):
            new_sample_dict[name] = sampler.sample_dataframe(df, sample_size)
    reports = []
    for sheet_name, column_dict_with_sample_values in new_sample_dict.items():
        sdd_report = {
            'resource_id': resource_id,
            'file_name': url,
            'file_url': download_url,
            'sheet_name': sheet_name,
            'processing_timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'processing_success': True,
            'n_records': len(sheets[sheet_name]),
            'n_columns': len(sheets[sheet_name].columns),
            'completion_tokens': 0,
            'prompt_tokens': 0,
            'personal_data_sensitive': False,
            'non_personal_data_sensitive': False,
            'columns': [],
        }
        if 'readme' in sheet_name.lower().replace(' ', ''):
            reports.append(sdd_report)
            continue

        # Add to columns the pii_entity TODO and sensitive False
        for col, item in column_dict_with_sample_values.items():
            sdd_report['columns'].append(
                {
                    'column_name': col,
                    'sample_values': item,
                    'personal_data': {'entity_type': 'TODO', 'sensitive': False},
                }
            )
        reports.append(sdd_report)
    logger.debug(f'Reports: {reports}')
    return reports


class DataSampler:
    """
    Utility class to load a dataset (CSV/XLS/XLSX) directly from a URL and sample random records.
    """

    SUPPORTED_EXTENSIONS = ('.csv', '.xls', '.xlsx')

    @handle_exception_wrap()
    def _validate_url(self, url: str) -> str:
        """Check if the URL points to a supported file type."""
        url_lower = url.lower()
        if not any(url_lower.endswith(ext) for ext in self.SUPPORTED_EXTENSIONS):
            raise ValueError(f'Unsupported file type. Only {", ".join(self.SUPPORTED_EXTENSIONS)} are supported.')
        return url_lower

    @handle_exception_wrap()
    def load_from_url(self, url: str) -> Dict[str, pd.DataFrame]:
        """Load CSV/XLS/XLSX from a URL into a dictionary of DataFrames keyed by sheet name."""
        url = self._validate_url(url)
        if url.endswith('.csv'):
            df = pd.read_csv(url, header=None, nrows=200)

            df = self._concatenate_header(df)
            # Put the most complete rows to the top
            df_sorted = (
                df.assign(num_nans=df.isna().sum(axis=1))
                .sort_values('num_nans', ascending=True)
                .drop(columns='num_nans')
            )
            return {'sheet1': df_sorted}

        # Excel files: can contain multiple sheets
        df_dict = pd.read_excel(url, sheet_name=None, nrows=1000, header=None)
        return {sheet_name: self._concatenate_header(df) for sheet_name, df in df_dict.items()}

    @handle_exception_wrap()
    def _concatenate_header(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Attempts to detect header rows and flatten them into single column names.
        Treats row 0 as the header row and concatenates all header rows including it.
        """
        # If dict, get first sheet
        if isinstance(df, dict):
            df = df[list(df.keys())[0]]

        # Skip leading all-NaN rows
        while not df.empty and df.iloc[0].isna().all():
            df = df.iloc[1:].reset_index(drop=True)

        if df.empty:
            return df

        # Find the end of the header block (first complete row starting from index 0)
        # Header row itself is at index 0, so we look for the last header row
        header_end_row = 0
        for idx in range(len(df)):
            row = df.iloc[idx]
            if row.notna().all():
                header_end_row = idx
                break

        # Extract header block (rows 0 to header_end_row, inclusive)
        # This includes the header row at index 0
        header_block = df.iloc[: header_end_row + 1].fillna('').astype(str)

        # Forward fill missing values within each row, then across rows
        header_block = header_block.apply(lambda row: row.replace('', None).ffill(), axis=1)
        header_block = header_block.replace('', None).ffill()

        # Combine multi-row headers into single column names
        final_columns = header_block.apply(lambda col: ' | '.join([v for v in col if v]), axis=0)
        # Data starts after the header block (header_end_row + 1)
        cleaned_df = df.iloc[header_end_row + 1 :].copy()
        cleaned_df.columns = final_columns
        return cleaned_df.reset_index(drop=True)

    @handle_exception_wrap()
    def sample_dataframe(self, df: pd.DataFrame, sample_size: int = 10) -> dict:
        """Return a dict of column -> sample values, using rows with the most non-null values first."""

        if df.empty:
            return {}

        # Work on a copy to avoid mutating original DF
        df_sorted = df.copy()

        # Add completeness score
        df_sorted['__null_count__'] = df_sorted.isna().sum(axis=1)

        # Sort so best rows (fewest nulls) appear at the top
        df_sorted = df_sorted.sort_values('__null_count__')

        sample_dict = {}

        for col in df.columns:  # only iterate real columns
            col_data = df_sorted[col]

            # Drop empty values (NaN or empty string)
            non_empty = col_data.dropna()
            non_empty = non_empty[non_empty != '']

            # Take the top N most complete values
            values = non_empty.head(sample_size).values.ravel().tolist()

            # If the column has no usable values
            if not values:
                values = [''] * sample_size  # or: df[col].unique()[:sample_size]

            # Pad to sample_size
            while len(values) < sample_size:
                values.append('')

            sample_dict[col] = values

        return sample_dict

    @handle_exception_wrap()
    def sample(self, url: str, sample_size: int = 5) -> Dict[str, pd.DataFrame]:
        """Main entrypoint: load and sample dataset(s) from a URL."""
        sheets = self.load_from_url(url)

        new_sample_dict = {}
        for name, df in sheets.items():
            if not is_readme_sheet(name):
                new_sample_dict[name] = self.sample_dataframe(df, sample_size)
        return new_sample_dict
