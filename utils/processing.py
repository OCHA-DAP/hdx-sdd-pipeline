# utils/data_sampler.py
import logging
import logging.config
from pathlib import Path
from typing import Union, Dict
import pandas as pd
import requests


class DataSampler:
    """
    Utility class to download a dataset (CSV/XLS/XLSX) and sample random records.
    """

    def __init__(self, output_dir: Union[str, Path] = 'downloads', logging_conf: str = 'logging.conf'):
        # --- Setup logging ---
        if Path(logging_conf).exists():
            logging.config.fileConfig(logging_conf)
        self.logger = logging.getLogger(__name__)

        # --- Setup directories ---
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.logger.debug('Initialized DataSampler with output_dir=%s', self.output_dir)

    def _download_file(self, url: str) -> Path:
        filename = Path(url).name
        file_path = self.output_dir / filename
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            file_path.write_bytes(response.content)
        except requests.RequestException as e:
            self.logger.error('Download failed: %s', e)
            raise RuntimeError(f'Failed to download file from {url}') from e

        return file_path

    def _load_file(self, file_path: Union[str, Path]) -> Dict[str, pd.DataFrame]:
        """
        Load CSV/XLS/XLSX file into a dictionary of DataFrames keyed by sheet name.
        CSV files return {'sheet1': df}.
        """
        file_path = Path(file_path)
        ext = file_path.suffix.lower()
        self.logger.debug('Loading file: %s', file_path)

        if ext == '.csv':
            df = pd.read_csv(file_path)
            return {'sheet1': df}
        elif ext in ['.xls', '.xlsx']:
            # Load all sheets with a sample size of 200 rows (to prevenet memory issues)
            all_sheets = pd.read_excel(file_path, sheet_name=None, nrows=200)

            # Log info
            total_rows = sum(len(df) for df in all_sheets.values())
            total_columns = max(df.shape[1] for df in all_sheets.values())
            print(f'Loaded dataset with {total_rows} rows and {total_columns} columns across {len(all_sheets)} sheets')

            # Return dictionary of DataFrames
            return all_sheets
        else:
            raise ValueError(f'Unsupported file type: {ext}')

    def _sample_dataframe(self, df: pd.DataFrame, sample_size: int = 20) -> pd.DataFrame:
        if df.empty:
            self.logger.debug('Empty DataFrame — returning as is.')
            return df

        n = min(sample_size, len(df))
        complete_rows = df[df.notna().all(axis=1)]
        incomplete_rows = df[df.isna().any(axis=1)]

        if len(complete_rows) >= n:
            sample = complete_rows.sample(n=n, random_state=42)
        else:
            needed = n - len(complete_rows)
            incomplete_rows['null_count'] = incomplete_rows.isna().sum(axis=1)
            incomplete_rows = incomplete_rows.sort_values('null_count')
            fallback_rows = incomplete_rows.drop(columns='null_count').head(needed)
            sample = pd.concat([complete_rows, fallback_rows]).sample(frac=1, random_state=42)

        self.logger.debug(
            'Sampled %d records (%d complete, %d with nulls)',
            len(sample),
            sample.notna().all(axis=1).sum(),
            sample.isna().any(axis=1).sum(),
        )
        return sample.reset_index(drop=True)

    def sample_from_url(self, url: str, sample_size: int = 20) -> Dict[str, pd.DataFrame]:
        """
        Download a dataset from URL, load it, and return sampled DataFrames by sheet.
        """
        file_path = self._download_file(url)

        return self._load_file(file_path)  # Dictionary of DataFrames by sheet name

    def sample_from_local(self, file_path: Union[str, Path], sample_size: int = 20) -> Dict[str, pd.DataFrame]:
        sheets = self._load_file(file_path)
        return {sheet_name: self._sample_dataframe(df, sample_size) for sheet_name, df in sheets.items()}
