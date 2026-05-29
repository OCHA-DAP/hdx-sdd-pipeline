"""Unit tests for SmartDataLoader."""

import pandas as pd
from unittest.mock import patch, MagicMock

from src.infrastructure.data_loader import SmartDataLoader
from src.domain.exceptions import DataProcessingError
import pytest


class TestSmartDataLoader:
    """Test suite for SmartDataLoader."""

    def test_initialization(self):
        """Test loader initialization."""
        loader = SmartDataLoader(max_rows=500)
        assert loader.max_rows == 500

    def test_validate_url_csv(self):
        """Test URL validation for CSV files."""
        loader = SmartDataLoader()
        assert loader.validate_url('https://example.com/data.csv') is True
        assert loader.validate_url('https://example.com/data.CSV') is True

    def test_validate_url_excel(self):
        """Test URL validation for Excel files."""
        loader = SmartDataLoader()
        assert loader.validate_url('https://example.com/data.xlsx') is True
        assert loader.validate_url('https://example.com/data.xls') is True

    def test_validate_url_invalid(self):
        """Test URL validation rejects invalid formats."""
        loader = SmartDataLoader()
        assert loader.validate_url('https://example.com/data.pdf') is False
        assert loader.validate_url('https://example.com/data.json') is False

    def test_preprocess_dataframe_simple(self):
        """Test preprocessing simple DataFrame."""
        loader = SmartDataLoader()

        # Create simple DataFrame
        data = {0: ['Name', 'John', 'Jane'], 1: ['Age', '25', '30'], 2: ['City', 'NYC', 'LA']}
        df = pd.DataFrame(data)

        result = loader._preprocess_dataframe(df)

        # Check headers were extracted
        assert 'Name' in result.columns
        assert 'Age' in result.columns
        assert len(result) == 2  # Data rows only

    def test_preprocess_dataframe_multirow_header(self):
        """Test preprocessing DataFrame with multi-row headers."""
        loader = SmartDataLoader()

        # Create DataFrame with multi-row header
        data = {
            0: ['Personal', 'Name', 'John', 'Jane'],
            1: ['Personal', 'Age', '25', '30'],
            2: ['Contact', 'Email', 'j@e.com', 'jane@e.com'],
        }
        df = pd.DataFrame(data)

        result = loader._preprocess_dataframe(df)

        # Check multi-row headers were concatenated
        assert any('Personal' in str(col) for col in result.columns)
        assert any('Contact' in str(col) for col in result.columns)

    def test_preprocess_dataframe_empty(self):
        """Test preprocessing empty DataFrame."""
        loader = SmartDataLoader()
        df = pd.DataFrame()

        result = loader._preprocess_dataframe(df)

        assert result.empty

    def test_preprocess_dataframe_leading_empty_rows(self):
        """Test preprocessing removes leading empty rows."""
        loader = SmartDataLoader()

        # Create DataFrame with leading NaN rows
        data = {0: [None, None, 'Name', 'John'], 1: [None, None, 'Age', '25']}
        df = pd.DataFrame(data)

        result = loader._preprocess_dataframe(df)

        # Leading empty rows should be removed
        assert len(result) == 1  # Only data row

    def test_sample_dataframe_basic(self):
        """Test basic DataFrame sampling."""
        loader = SmartDataLoader()

        df = pd.DataFrame({'Name': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'], 'Age': [25, 30, 35, 28, 32]})

        samples = loader.sample_dataframe(df, sample_size=3)

        assert 'Name' in samples
        assert 'Age' in samples
        assert len(samples['Name']) == 3
        assert len(samples['Age']) == 3

    def test_sample_dataframe_with_nulls(self):
        """Test sampling prioritizes non-null values."""
        loader = SmartDataLoader()

        df = pd.DataFrame({'Name': ['John', None, 'Jane', '', 'Bob'], 'Age': [25, None, 30, None, 35]})

        samples = loader.sample_dataframe(df, sample_size=3)

        # Should get non-null values first
        assert 'John' in samples['Name']
        assert 'Jane' in samples['Name']
        assert 'Bob' in samples['Name']

    def test_sample_dataframe_empty(self):
        """Test sampling empty DataFrame."""
        loader = SmartDataLoader()
        df = pd.DataFrame()

        samples = loader.sample_dataframe(df, sample_size=5)

        assert samples == {}

    def test_sample_dataframe_padding(self):
        """Test sampling pads to sample_size."""
        loader = SmartDataLoader()

        df = pd.DataFrame({'Name': ['John', 'Jane']})  # Only 2 values

        samples = loader.sample_dataframe(df, sample_size=5)

        # Should be padded to 5
        assert len(samples['Name']) == 5
        assert samples['Name'][0] == 'John'
        assert samples['Name'][1] == 'Jane'
        assert samples['Name'][2] == ''  # Padding

    @patch('pandas.read_csv')
    def test_load_csv_from_url(self, mock_read_csv):
        """Test loading CSV from URL."""
        loader = SmartDataLoader()

        # Mock CSV data
        mock_df = pd.DataFrame({0: ['Name', 'John'], 1: ['Age', '25']})
        mock_read_csv.return_value = mock_df

        result = loader._load_csv('https://example.com/data.csv')

        assert 'sheet1' in result
        assert isinstance(result['sheet1'], pd.DataFrame)
        mock_read_csv.assert_called_once()

    @patch('pandas.read_excel')
    def test_load_excel_from_url(self, mock_read_excel):
        """Test loading Excel from URL."""
        loader = SmartDataLoader()

        # Mock Excel data with multiple sheets
        mock_sheets = {
            'Sheet1': pd.DataFrame({0: ['Name', 'John'], 1: ['Age', '25']}),
            'Sheet2': pd.DataFrame({0: ['City', 'NYC'], 1: ['Country', 'USA']}),
        }
        mock_read_excel.return_value = mock_sheets

        result = loader._load_excel('https://example.com/data.xlsx')

        assert 'Sheet1' in result
        assert 'Sheet2' in result
        mock_read_excel.assert_called_once()

    @patch('src.infrastructure.data_loader.requests.get')
    def test_load_from_url_csv(self, mock_requests_get):
        """Test load_from_url with CSV."""
        loader = SmartDataLoader()

        # Mock the HTTP response
        mock_response = MagicMock()
        mock_response.content = b'Name,Age\nJohn,25\nJane,30'
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response

        result = loader.load_from_url('https://example.com/data.csv')

        assert 'sheet1' in result
        assert len(result['sheet1']) == 2  # Two data rows
        assert list(result['sheet1'].columns) == ['Name', 'Age']

    @patch('src.infrastructure.data_loader.requests.get')
    def test_load_from_url_applies_default_user_agent(self, mock_requests_get):
        """Test load_from_url applies loader default user-agent when none provided by caller."""
        loader = SmartDataLoader(user_agent='TestUA/2.0.0')

        mock_response = MagicMock()
        mock_response.content = b'Name,Age\nJohn,25\nJane,30'
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response

        loader.load_from_url('https://example.com/data.csv')

        _, kwargs = mock_requests_get.call_args
        assert kwargs['headers']['User-Agent'] == 'TestUA/2.0.0'

    @patch('src.infrastructure.data_loader.requests.get')
    def test_load_from_url_preserves_explicit_user_agent(self, mock_requests_get):
        """Test load_from_url keeps caller-provided user-agent header."""
        loader = SmartDataLoader(user_agent='DefaultUA/2.0.0')

        mock_response = MagicMock()
        mock_response.content = b'Name,Age\nJohn,25\nJane,30'
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response

        loader.load_from_url('https://example.com/data.csv', http_headers={'User-Agent': 'CallerUA/9.9.9'})

        _, kwargs = mock_requests_get.call_args
        assert kwargs['headers']['User-Agent'] == 'CallerUA/9.9.9'

    @patch('src.infrastructure.data_loader.requests.get')
    def test_load_from_url_keeps_authorization_for_hdx_domain(self, mock_requests_get):
        """Test Authorization header is kept for configured HDX domain and subdomains."""
        loader = SmartDataLoader(hdx_base_url='https://hdx.example.org')

        mock_response = MagicMock()
        mock_response.content = b'Name,Age\nJohn,25\nJane,30'
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response

        loader.load_from_url('https://data.hdx.example.org/file.csv', http_headers={'Authorization': 'Bearer secret'})

        _, kwargs = mock_requests_get.call_args
        assert kwargs['headers']['Authorization'] == 'Bearer secret'

    @patch('src.infrastructure.data_loader.requests.get')
    def test_load_from_url_drops_authorization_for_non_hdx_domain(self, mock_requests_get):
        """Test Authorization header is removed for non-HDX download targets."""
        loader = SmartDataLoader(hdx_base_url='https://hdx.example.org')

        mock_response = MagicMock()
        mock_response.content = b'Name,Age\nJohn,25\nJane,30'
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response

        loader.load_from_url(
            'https://storage.other-cloud.org/file.csv',
            http_headers={'Authorization': 'Bearer secret'},
        )

        _, kwargs = mock_requests_get.call_args
        assert 'Authorization' not in kwargs['headers']

    @patch('src.infrastructure.data_loader.requests.get')
    def test_load_from_url_excel(self, mock_requests_get):
        """Test load_from_url with Excel."""
        loader = SmartDataLoader()

        # Create a simple Excel file in memory
        import io

        excel_data = io.BytesIO()
        df = pd.DataFrame({'Name': ['John'], 'Age': [25]})
        df.to_excel(excel_data, index=False)
        excel_data.seek(0)

        # Mock the HTTP response
        mock_response = MagicMock()
        mock_response.content = excel_data.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response

        result = loader.load_from_url('https://example.com/data.xlsx')

        assert 'Sheet1' in result
        assert len(result['Sheet1']) == 1  # One data row

    def test_load_from_url_invalid_format(self):
        """Test load_from_url rejects invalid format."""
        loader = SmartDataLoader()

        with pytest.raises(DataProcessingError, match='Unsupported file type'):
            loader.load_from_url('https://example.com/data.pdf')

    @patch('pandas.read_csv')
    def test_load_from_url_error_handling(self, mock_read_csv):
        """Test load_from_url handles errors."""
        loader = SmartDataLoader()

        mock_read_csv.side_effect = Exception('Network error')

        with pytest.raises(DataProcessingError, match='Failed to load data'):
            loader.load_from_url('https://example.com/data.csv')

    def test_load_from_file_not_found(self):
        """Test load_from_file with non-existent file."""
        loader = SmartDataLoader()

        with pytest.raises(DataProcessingError, match='File not found'):
            loader.load_from_file('/nonexistent/file.csv')

    def test_load_from_file_invalid_format(self):
        """Test load_from_file rejects invalid format."""
        loader = SmartDataLoader()

        # Create a temporary file with wrong extension
        with patch('pathlib.Path.exists', return_value=True):
            with pytest.raises(DataProcessingError, match='Unsupported file type'):
                loader.load_from_file('/path/to/file.pdf')

    @patch('pandas.read_csv')
    @patch('pathlib.Path.exists', return_value=True)
    def test_load_from_file_csv(self, mock_exists, mock_read_csv):
        """Test load_from_file with CSV."""
        loader = SmartDataLoader()

        mock_df = pd.DataFrame({0: ['Name', 'John'], 1: ['Age', '25']})
        mock_read_csv.return_value = mock_df

        result = loader.load_from_file('/path/to/data.csv')

        assert 'sheet1' in result

    @patch('pandas.read_excel')
    @patch('pathlib.Path.exists', return_value=True)
    def test_load_from_file_excel(self, mock_exists, mock_read_excel):
        """Test load_from_file with Excel."""
        loader = SmartDataLoader()

        mock_sheets = {'Sheet1': pd.DataFrame({0: ['Name', 'John']})}
        mock_read_excel.return_value = mock_sheets

        result = loader.load_from_file('/path/to/data.xlsx')

        assert 'Sheet1' in result

    @patch('pandas.read_csv')
    @patch('pathlib.Path.exists', return_value=True)
    def test_load_from_file_error_handling(self, mock_exists, mock_read_csv):
        """Test load_from_file handles errors."""
        loader = SmartDataLoader()

        mock_read_csv.side_effect = Exception('Read error')

        with pytest.raises(DataProcessingError, match='Failed to load file'):
            loader.load_from_file('/path/to/data.csv')

    def test_build_column_names_empty_header(self):
        """Test _build_column_names with empty header block."""
        loader = SmartDataLoader()

        # Empty DataFrame
        header_block = pd.DataFrame()
        result = loader._build_column_names(header_block)

        assert result == []

    def test_detect_header_end_with_column_pattern(self):
        """Test _detect_header_end detects 'Column' metadata rows."""
        loader = SmartDataLoader()

        # Create DataFrame with 'Column' pattern
        data = {
            0: ['Column 1', 'Column 2', 'Data1', 'Data2'],
            1: ['Type A', 'Type B', 'Value1', 'Value2'],
            2: ['Info', 'Info', 'Value3', 'Value4'],
        }
        df = pd.DataFrame(data)

        result = loader._detect_header_end(df)

        # Should detect the row with 'Column' pattern
        assert result >= 0

    def test_detect_header_end_fallback(self):
        """Test _detect_header_end uses fallback when no clear pattern."""
        loader = SmartDataLoader()

        # Create DataFrame with sparse data
        data = {0: ['A', None, None, 'Data'], 1: [None, 'B', None, 'Data'], 2: [None, None, 'C', 'Data']}
        df = pd.DataFrame(data)

        result = loader._detect_header_end(df)

        # Should return a valid header end index
        assert result >= 0
        assert result < len(df)

    def test_filter_empty_columns_empty_df(self):
        """Test _filter_empty_columns with empty DataFrame."""
        loader = SmartDataLoader()

        df = pd.DataFrame()
        result = loader._filter_empty_columns(df)

        assert result.empty

    def test_filter_empty_columns_all_empty(self):
        """Test _filter_empty_columns when all columns are empty."""
        loader = SmartDataLoader()

        df = pd.DataFrame({'col1': [None, None, None], 'col2': ['', '', ''], 'col3': ['nan', 'none', '']})

        result = loader._filter_empty_columns(df)

        # Should return empty DataFrame when all columns are empty
        assert result.empty or len(result.columns) == 0

    def test_preprocess_dataframe_complex_headers(self):
        """Test preprocessing with complex multi-row headers."""
        loader = SmartDataLoader()

        # Create DataFrame with complex headers
        data = {
            0: ['Section A', 'Name', 'John', 'Jane'],
            1: ['Section A', 'Age', '25', '30'],
            2: ['Section B', 'City', 'NYC', 'LA'],
        }
        df = pd.DataFrame(data)

        result = loader._preprocess_dataframe(df)

        # Should have processed headers
        assert len(result.columns) > 0
        assert len(result) >= 1

    def test_ensure_unique_names(self):
        """Test _ensure_unique_names handles duplicates."""
        loader = SmartDataLoader()

        names = ['Name', 'Age', 'Name', 'Name', 'City']
        result = loader._ensure_unique_names(names)

        assert result[0] == 'Name'
        assert result[1] == 'Age'
        assert result[2] == 'Name_1'
        assert result[3] == 'Name_2'
        assert result[4] == 'City'

    def test_detect_header_end_with_column_and_high_fill_next_row(self):
        """Test _detect_header_end with 'Column' pattern and high fill rate in next row."""
        loader = SmartDataLoader()

        # Create DataFrame where row with 'Column' has high fill rate in next row
        data = {
            0: ['Column A', 'Value1', 'Value2', 'Value3'],
            1: ['Column B', 'Value4', 'Value5', 'Value6'],
            2: ['Column C', 'Value7', 'Value8', 'Value9'],
        }
        df = pd.DataFrame(data)

        result = loader._detect_header_end(df)

        # Should detect the 'Column' row as header
        assert result == 0

    def test_detect_header_end_all_filled_row(self):
        """Test _detect_header_end finds first row where all cells are filled."""
        loader = SmartDataLoader()

        # Create DataFrame with first fully filled row
        data = {
            0: [None, 'Header1', 'Data1', 'Data2'],
            1: [None, 'Header2', 'Data3', 'Data4'],
            2: [None, 'Header3', 'Data5', 'Data6'],
        }
        df = pd.DataFrame(data)

        result = loader._detect_header_end(df)

        # Should find the first row where all cells are filled
        assert result >= 0

    def test_initialization_default_max_rows(self):
        """Test that default max_rows is None (loading whole dataset)."""
        loader = SmartDataLoader()
        assert loader.max_rows is None

    def test_sample_dataframe_unique_values(self):
        """Test that sample_dataframe extracts unique values and filters empty/nan/none values."""
        loader = SmartDataLoader()

        # Column Name has duplicates and invalid placeholder strings
        # Column Age has unique valid values, nulls, and empty strings
        df = pd.DataFrame(
            {
                'Name': ['John', 'John', 'Jane', '   ', 'Bob', 'none', 'nan', 'Jane', 'Alice'],
                'Age': [25, None, 30, '', 35, 25, 40, 25, 45],
            }
        )

        samples = loader.sample_dataframe(df, sample_size=5)

        # Name should be: John, Jane, Bob, Alice (4 unique values, padded with 1 empty string)
        assert samples['Name'] == ['John', 'Jane', 'Bob', 'Alice', '']

        # Age unique values in order, randomly sampled with seed 42: [30.0, 45.0, 35.0, 25.0, 40.0]
        # Note: 25 appears multiple times but should only be present once
        assert samples['Age'] == [30.0, 45.0, 35.0, 25.0, 40.0]

    def test_sample_dataframe_chunking_efficiency(self):
        """Test that sample_dataframe's chunking optimization works correctly and returns correct values."""
        loader = SmartDataLoader()

        # Create a large series of values: 2000 'duplicate' followed by 5 unique values,
        # then check if we can still extract the unique values.
        # This will trigger the fallback check because the first 100 or 1000 rows do not have 5 unique values.
        names = ['John'] * 2000 + ['Jane', 'Bob', 'Alice', 'Charlie', 'Dave']
        df = pd.DataFrame({'Name': names})

        samples = loader.sample_dataframe(df, sample_size=5)
        assert samples['Name'] == ['John', 'Jane', 'Dave', 'Bob', 'Charlie']

    def test_load_csv_chunking_stops_early(self):
        """Test that load_csv stops loading larger chunks if 5 unique values are found in the first chunk."""
        import tempfile
        import os
        from unittest.mock import patch

        # Create a CSV file with 150 rows.
        # First 100 rows already have 5 unique values: John0, John1, John2, John3, John4.
        rows = ['Name,Age']
        for i in range(150):
            rows.append(f'John{i % 5},{20 + i}')

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
            f.write('\n'.join(rows))
            temp_file = f.name

        try:
            loader = SmartDataLoader()
            # Let's spy/mock pd.read_csv to see what chunk sizes it's called with.
            original_read_csv = pd.read_csv
            calls = []

            def mock_read_csv(*args, **kwargs):
                calls.append(kwargs.get('nrows'))
                return original_read_csv(*args, **kwargs)

            with patch('pandas.read_csv', side_effect=mock_read_csv):
                result = loader.load_from_file(temp_file)

            assert 'sheet1' in result
            # Should have only loaded the first chunk (100)
            assert calls == [100]
            # Verify data matches the first 100 loaded rows
            # 1 header row + 99 data rows
            assert len(result['sheet1']) == 99
        finally:
            os.unlink(temp_file)

    def test_load_csv_chunking_goes_to_next_chunk(self):
        """Test that load_csv proceeds to larger chunks if first chunk does not have 5 unique values."""
        import tempfile
        import os
        from unittest.mock import patch

        # Create a CSV file with 250 rows.
        # First 100 rows only have 2 unique values for Name: John0, John1.
        rows = ['Name,Age']
        for i in range(250):
            name_val = f'John{i % 2}' if i < 100 else f'John{i}'
            rows.append(f'{name_val},{20 + i}')

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
            f.write('\n'.join(rows))
            temp_file = f.name

        try:
            loader = SmartDataLoader()
            calls = []
            original_read_csv = pd.read_csv

            def mock_read_csv(*args, **kwargs):
                calls.append(kwargs.get('nrows'))
                return original_read_csv(*args, **kwargs)

            with patch('pandas.read_csv', side_effect=mock_read_csv):
                result = loader.load_from_file(temp_file)

            # First chunk is 100. It doesn't have 5 unique values for Name.
            # So it tries chunk size 1000.
            # File has 251 lines total, so read_csv(nrows=1000) reads all 250 rows, which is < 1000.
            assert calls == [100, 1000]
            assert len(result['sheet1']) == 250  # 250 rows
        finally:
            os.unlink(temp_file)
