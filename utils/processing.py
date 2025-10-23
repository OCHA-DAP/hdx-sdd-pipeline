"""utils/data_sampler.py: Download and sample datasets with pandas."""

import logging
import logging.config
from pathlib import Path
from typing import Union
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
        """
        Download a file from a URL and save it locally.
        """
        filename = Path(url).name
        file_path = self.output_dir / filename

        self.logger.info('Downloading file: %s', url)
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            file_path.write_bytes(response.content)
            self.logger.info('File saved to: %s', file_path)
        except requests.RequestException as e:
            self.logger.error('Download failed: %s', e)
            raise RuntimeError(f'Failed to download file from {url}') from e

        return file_path

    def _load_file(self, file_path: Union[str, Path]) -> pd.DataFrame:
        """
        Load a CSV/XLS/XLSX file into a pandas DataFrame.
        """
        file_path = Path(file_path)
        ext = file_path.suffix.lower()

        self.logger.debug('Loading file: %s', file_path)

        if ext == '.csv':
            df = pd.read_csv(file_path)
        elif ext in ['.xls', '.xlsx']:
            df = pd.read_excel(file_path)
        else:
            raise ValueError(f'Unsupported file type: {ext}')

        self.logger.info('Loaded dataset with %d rows and %d columns', len(df), len(df.columns))
        return df

    def _sample_dataframe(self, df: pd.DataFrame, sample_size: int = 20) -> pd.DataFrame:
        """
        Randomly sample records from the DataFrame.
        """
        n = min(sample_size, len(df))
        sample = df.sample(n=n, random_state=42)
        self.logger.debug('Sampled %d records', n)
        return sample

    def sample_from_url(self, url: str, sample_size: int = 20) -> pd.DataFrame:
        """
        Download a dataset from URL, load it, and return a random sample.
        """
        file_path = self._download_file(url)
        df = self._load_file(file_path)
        return self._sample_dataframe(df, sample_size)

    def sample_from_local(self, file_path: Union[str, Path], sample_size: int = 20) -> pd.DataFrame:
        """
        Load a local dataset and return a random sample.
        """
        df = self._load_file(file_path)
        return self._sample_dataframe(df, sample_size)
