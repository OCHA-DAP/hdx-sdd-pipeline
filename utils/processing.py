# utils/data_sampler.py
from pathlib import Path
from typing import Union, Dict
import pandas as pd
import requests


class DataSampler:
    """
    Utility class to download a dataset (CSV/XLS/XLSX) and sample random records.
    """

    def __init__(self, output_dir: Union[str, Path] = 'downloads'):
        # Setup directories
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _download_file(self, url: str) -> Path:
        filename = Path(url).name
        file_path = self.output_dir / filename
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            file_path.write_bytes(response.content)
        except requests.RequestException as e:
            raise RuntimeError(f'Failed to download file from {url}') from e

        return file_path

    def _load_file(self, file_path: Union[str, Path]) -> Dict[str, pd.DataFrame]:
        """
        Load CSV/XLS/XLSX file into a dictionary of DataFrames keyed by sheet name.
        CSV files return {'sheet1': df}.
        """
        file_path = Path(file_path)
        ext = file_path.suffix.lower()

        if ext == '.csv':
            df = pd.read_csv(file_path, header=None)
            df = self._concatenate_header(df)
            return {'sheet1': df}
        elif ext in ['.xls', '.xlsx']:
            # Load all sheets with a sample size of 200 rows (to prevent memory issues)
            df = pd.read_excel(file_path, sheet_name=None, nrows=200, header=None)
            return {sheet_name: self._concatenate_header(df[sheet_name]) for sheet_name in df.keys()}
        else:
            raise ValueError(f'Unsupported file type: {ext}')

    def _concatenate_header(self, df: pd.DataFrame, numeric_threshold: float = 0.8) -> pd.DataFrame:
        """
        Combined header concatenation:
        1. First attempts to find the first row without NaN (fully populated row).
        2. If none found, falls back to detecting numeric columns.
        3. Fills missing header cells horizontally.
        4. Concatenates the header rows into single column names.
        """

        # ---- STEP 1: try first fully populated row ---- #
        header_end_row = None
        for idx, row in df.iterrows():
            if row.notna().all():
                header_end_row = idx
                break

        # ---- STEP 2: fallback to numeric detection if no full row found ---- #
        if header_end_row is None:
            numeric_ratio = df.apply(lambda col: pd.to_numeric(col, errors='coerce').notna().mean())
            numeric_cols = numeric_ratio[numeric_ratio >= numeric_threshold].index.tolist()

            if numeric_cols:
                for idx, row in df.iterrows():
                    if row[numeric_cols].apply(lambda x: pd.to_numeric(x, errors='coerce')).notna().any():
                        header_end_row = idx
                        break
            else:
                # no numeric columns detected, return original df
                return df

        # ---- STEP 3: extract header block ---- #
        header_block = df.iloc[: header_end_row + 1].copy()
        header_block = header_block.fillna("").astype(str)

        # ---- STEP 4: fill horizontally missing cells ---- #
        header_block = header_block.apply(lambda row: row.replace("", None).ffill(), axis=1)
        header_block = header_block.replace("", None).ffill()  # vertical fill

        # ---- STEP 5: concatenate header rows ---- #
        final_columns = header_block.apply(lambda col: " | ".join([v for v in col if v]), axis=0)

        # ---- STEP 6: assign as header ---- #
        cleaned_df = df.iloc[header_end_row + 1 :].copy()
        cleaned_df.columns = final_columns

        return cleaned_df.reset_index(drop=True)

    def _sample_dataframe(self, df: pd.DataFrame, sample_size: int = 20) -> pd.DataFrame:
        if df.empty:
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

        return sample.reset_index(drop=True)

    def sample_from_url(self, url: str, sample_size: int = 20) -> Dict[str, pd.DataFrame]:
        file_path = self._download_file(url)
        return self._load_file(file_path)

    def sample_from_local(self, file_path: Union[str, Path], sample_size: int = 20) -> Dict[str, pd.DataFrame]:
        sheets = self._load_file(file_path)
        return {sheet_name: self._sample_dataframe(df, sample_size) for sheet_name, df in sheets.items()}
