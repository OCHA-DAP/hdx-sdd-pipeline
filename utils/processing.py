# utils/processing.py
from typing import Dict
import pandas as pd
from models.sdd_report import SDDReport, PIIColumnReport
from utils.exception_handler import handle_exception_wrap
import datetime


def create_report(url: str, resource_id: str = None, download_url: str = None, sample_size: int = 5):
    sampler = DataSampler()
    sheets = sampler.load_from_url(url)
    sample_dict = {name: sampler._sample_dataframe(df, sample_size) for name, df in sheets.items()}

    reports = []
    for sheet_name, column_dict_with_sample_values in sample_dict.items():
        sdd_report = SDDReport(
            resource_id=resource_id,
            file_name=url,
            file_url=download_url,
            sheet_name=sheet_name,
            processing_timestamp=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            processing_success=True,
            n_records=len(sheets[sheet_name]),
            n_columns=len(sheets[sheet_name].columns),
        )
        if 'readme' in sheet_name.lower().replace(' ', ''):
            reports.append(sdd_report.to_dict())
            continue

        # Add to columns the pii_entity TODO and sensitive False
        column_sdd_report = []
        for col, item in column_dict_with_sample_values.items():
            column_sdd_report.append(
                PIIColumnReport(
                    column_name=col,
                    sample_values=item,
                    pii={'entity_type': 'TODO', 'sensitive': False},
                )
            )
        sdd_report.columns = column_sdd_report
        reports.append(sdd_report.to_dict())
    return reports


# def table_markdown(report: SDDReport) -> str:
#     """Generate a markdown table from the report sample columns."""
#     column_samples = {}
#     for col in report.columns:
#         key = (
#             f'{col.column_name} - {col.pii.get("entity_type", "None")}'
#             if col.pii.get('entity_type') != 'None'
#             else col.column_name
#         )
#         column_samples[key] = col.sample_values
#     if not column_samples:
#         return ''
#     max_len = max(len(values) for values in column_samples.values())
#     for key, values in column_samples.items():
#         column_samples[key] = values + [''] * (max_len - len(values))

#     return pd.DataFrame(column_samples).to_markdown(index=False) or ''


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

    @handle_exception_wrap()
    def _sample_dataframe(self, df: pd.DataFrame, sample_size: int = 10) -> dict:
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
            values = non_empty.head(sample_size).tolist()

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

        sample_dict = {name: self._sample_dataframe(df, sample_size) for name, df in sheets.items()}
        return sample_dict


if __name__ == '__main__':
    sampler = DataSampler()
    # sheets = sampler.sample(
    #     'https://dev.data-humdata-org.ahconu.org/dataset/a4256b92-dfee-4856-b28e-81abcf1da882/resource/496d920a-56e5-4093-9588-26fbd1ea46b7/download/multicolumn_sample.xlsx'
    # )

    sheets = sampler.sample('research/data/panama.xlsx')
    sdd_report = create_report('research/data/panama.xlsx')
    print(sdd_report)
