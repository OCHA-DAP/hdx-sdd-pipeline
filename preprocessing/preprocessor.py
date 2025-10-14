"""preprocessing/preprocessor.py: Data preprocessor for the SSD Pipeline."""

import logging
import os
import pandas as pd
from typing import Any, Dict, Union
from .analyze_xlsx import analyze_excel_file

logger = logging.getLogger(__name__)


class TablePreprocessor:
    """
    Preprocesses tabular data files for classification.
    """

    def __init__(self):
        self.supported_formats = ['.csv', '.xlsx', '.xls']

    def process_file(self, file_path: str) -> Dict[str, Any]:
        """
        Process a file and extract table structure and context.

        Args:
            file_path: Path to the file to process

        Returns:
            dict: Structured data with columns, table_context, and metadata
        """
        try:
            # Validate file exists and get extension
            if not os.path.exists(file_path):
                raise FileNotFoundError(f'File not found: {file_path}')

            file_ext = os.path.splitext(file_path)[1].lower()
            if file_ext not in self.supported_formats:
                raise ValueError(f'Unsupported file format: {file_ext}')

            logger.info('Processing file: %s', file_path)

            # Load data based on file type
            if file_ext == '.csv':
                df = self._load_csv(file_path)
                excel_analysis = None
            else:  # Excel files
                df, excel_analysis = self._load_excel(file_path)

            # Extract table structure
            columns_data = self._extract_columns_data(df)

            # Generate table context
            table_context = self._generate_table_context(df, columns_data, excel_analysis)

            # Create metadata
            metadata = {
                'file_path': file_path,
                'file_name': os.path.basename(file_path),
                'file_size': os.path.getsize(file_path),
                'num_rows': len(df),
                'num_columns': len(df.columns),
                'excel_analysis': excel_analysis,
                'processing_timestamp': pd.Timestamp.now().isoformat(),
            }

            result = {'columns': columns_data, 'table_context': table_context, 'metadata': metadata}

            logger.info('Successfully processed file: %s', file_path)
            return result

        except Exception as e:
            logger.error('Error processing file %s: %s', file_path, e)
            raise

    def _load_csv(self, file_path: str) -> pd.DataFrame:
        """Load CSV file into DataFrame."""
        try:
            # Try different encodings
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    logger.info('Successfully loaded CSV with encoding: %s', encoding)
                    return df
                except UnicodeDecodeError:
                    continue

            # If all encodings fail, use utf-8 with errors='replace'
            df = pd.read_csv(file_path, encoding='utf-8', errors='replace')
            logger.warning('Loaded CSV with encoding errors replaced')
            return df

        except Exception as e:
            logger.error('Error loading CSV file %s: %s', file_path, e)
            raise

    def _load_excel(self, file_path: str) -> tuple[pd.DataFrame, Dict[str, Any]]:
        """Load Excel file into DataFrame and get analysis."""
        try:
            # Analyze Excel file structure
            excel_analysis = analyze_excel_file(file_path)

            # Load the first sheet (or main data sheet)
            df = pd.read_excel(file_path, sheet_name=0)

            logger.info('Successfully loaded Excel file with analysis: %s', excel_analysis)
            return df, excel_analysis

        except Exception as e:
            logger.error('Error loading Excel file %s: %s', file_path, e)
            raise

    def _extract_columns_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Extract column information and sample values."""
        columns_data = {}

        for col_name in df.columns:
            # Get sample values (first 5 non-null values)
            sample_values = df[col_name].dropna().head(5).tolist()

            # Get data type
            dtype = str(df[col_name].dtype)

            # Get basic statistics for numeric columns
            stats = {}
            if pd.api.types.is_numeric_dtype(df[col_name]):
                stats = {
                    'min': df[col_name].min(),
                    'max': df[col_name].max(),
                    'mean': df[col_name].mean(),
                    'null_count': df[col_name].isnull().sum(),
                    'unique_count': df[col_name].nunique(),
                }

            columns_data[col_name] = {
                'sample_values': sample_values,
                'dtype': dtype,
                'stats': stats,
                'null_count': df[col_name].isnull().sum(),
                'unique_count': df[col_name].nunique(),
            }

        return columns_data

    def _generate_table_context(
        self, df: pd.DataFrame, columns_data: Dict[str, Any], excel_analysis: Union[Dict[str, Any], None] = None
    ) -> str:
        """Generate markdown-formatted table context."""
        context_parts = []

        # Table overview
        context_parts.append('## Table Overview')
        context_parts.append(f'- **Rows**: {len(df)}')
        context_parts.append(f'- **Columns**: {len(df.columns)}')

        if excel_analysis:
            context_parts.append(f"- **Multiple Sheets**: {excel_analysis.get('multiple_sheets', False)}")
            context_parts.append(f"- **Metadata Sheet**: {excel_analysis.get('metadatasheet', False)}")

        context_parts.append('')

        # Column schema
        context_parts.append('## Column Schema')
        for col_name, col_data in columns_data.items():
            context_parts.append(f'### {col_name}')
            context_parts.append(f"- **Type**: {col_data['dtype']}")
            context_parts.append(f"- **Null Count**: {col_data['null_count']}")
            context_parts.append(f"- **Unique Values**: {col_data['unique_count']}")

            if col_data['sample_values']:
                sample_str = ', '.join([str(val) for val in col_data['sample_values'][:3]])
                context_parts.append(f'- **Sample Values**: {sample_str}')

            # Add statistics for numeric columns
            if col_data['stats']:
                stats = col_data['stats']
                if 'min' in stats and 'max' in stats:
                    context_parts.append(f"- **Range**: {stats['min']} to {stats['max']}")
                if 'mean' in stats:
                    context_parts.append(f"- **Mean**: {stats['mean']:.2f}")

            context_parts.append('')

        # Sample data preview
        context_parts.append('## Sample Data Preview')
        context_parts.append('```')
        context_parts.append(df.head(3).to_string())
        context_parts.append('```')

        return '\n'.join(context_parts)
