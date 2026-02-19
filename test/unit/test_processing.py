import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from utils.processing import DataSampler
from utils.utils import table_markdown
import utils.exception_handler


def test_basic_markdown_output():
    report = {
        'columns': [
            {'column_name': 'name', 'sample_values': ['Alice', 'Bob'], 'personal_data': {'entity_type': 'person_name'}},
            {'column_name': 'age', 'sample_values': ['30', '40'], 'personal_data': {'entity_type': 'None'}},
        ]
    }
    md = table_markdown(report)
    # Should include "name - person_name" in header
    assert 'name - person_name' in md
    # age should not include "- None"
    assert 'age |' in md
    # markdown must contain table pipes
    assert '|' in md


def test_padding_of_shorter_column():
    report = {
        'columns': [
            {'column_name': 'city', 'sample_values': ['NYC'], 'personal_data': {'entity_type': 'None'}},
            {'column_name': 'zip', 'sample_values': ['10001', '20002'], 'personal_data': {'entity_type': 'None'}},
        ]
    }
    md = table_markdown(report)

    # Row 1 of "city" should be padded with empty string
    assert 'city' in md
    assert 'NYC' in md
    assert '10001' in md
    assert '20002' in md


def test_entity_type_none_handling():
    report = {
        'columns': [
            {
                'column_name': 'email',
                'sample_values': ['a@example.com'],
                'personal_data': {'entity_type': 'email_address'}
            },
            {'column_name': 'status', 'sample_values': ['active'], 'personal_data': {'entity_type': 'None'}},
        ]
    }
    md = table_markdown(report)

    assert 'email - email_address' in md
    assert 'status - None' not in md  # must not append "- None"


def test_markdown_with_irregular_sample_lengths():
    report = {
        'columns': [
            {'column_name': 'col1', 'sample_values': ['A', 'B', 'C'], 'personal_data': {'entity_type': 'None'}},
            {'column_name': 'col2', 'sample_values': ['1'], 'personal_data': {'entity_type': 'None'}},
        ]
    }
    md = table_markdown(report)
    assert 'col1' in md
    assert 'A' in md
    assert 'B' in md
    assert 'C' in md
    assert '1' in md

    # col2 should have empty strings filling up row 1 and 2
    assert 'col2' in md
    assert '1' in md
    assert '' in md
    assert '' in md


def test_markdown_output_matches_dataframe_to_markdown_format():
    report = {
        'columns': [
            {
                'column_name': 'product',
                'sample_values': ['Book', 'Pen'],
                'personal_data': {'entity_type': 'product_name'}
            },
            {'column_name': 'price', 'sample_values': ['10', '2'], 'personal_data': {'entity_type': 'price'}},
        ]
    }
    md = table_markdown(report)

    assert 'product - product_name' in md
    assert 'price - price' in md
    assert 'Book' in md
    assert '2' in md


sampler = DataSampler()
test_url_csv = 'https://dev.data-humdata-org.ahconu.org/dataset/a87f96f8-16e6-4d51-872c-cfa54a8251ec/resource/4ef001d1-7888-4f5d-98ce-0ca8006787f7/download/gdacs_rss_information.csv'
test_file_path = 'test/unit/downloads/gdacs_rss_information.csv'


def test_init_datasampler():
    sampler = DataSampler()
    assert sampler is not None


def test_download_file():
    dfs = sampler.sample(test_url_csv)
    assert dfs is not None
    assert len(dfs) == 1
    assert dfs['sheet1'] is not None
    # assert len(dfs['sheet1']) == 20


def test_concatenate_header():
    df = pd.read_excel('test/unit/downloads/multicolumn_sample.xlsx', header=None)
    sampler = DataSampler()
    df = sampler._concatenate_header(df)
    columns = df.columns.tolist()
    assert df is not None
    assert 'test | test header 1 | test subheader 1' in columns
    assert 'test | test header 1 | test subheader 2' in columns
    assert 'test | test header 2 | test subheader 3' in columns


def test_unsupported_file_type():
    with pytest.raises(utils.exception_handler.ContextualError):
        sampler.sample('test/unit/downloads/unsupported.txt')


def test_empty_dataframe():
    df = pd.DataFrame()
    assert sampler.sample_dataframe(df) is not None
    assert len(sampler.sample_dataframe(df)) == 0


def test_load_from_url_error():
    with pytest.raises(utils.exception_handler.ContextualError):
        sampler.load_from_url('test/unit/downloads/nonexistent.csv')


@patch('utils.processing.requests.get')
def test_load_from_url_passes_http_headers_to_csv(mock_requests_get):
    sampler = DataSampler()
    mock_response = MagicMock()
    mock_response.content = b'h1,h2\nv1,v2\n'
    mock_requests_get.return_value = mock_response

    sampler.load_from_url('https://example.com/file.csv', http_headers={'Authorization': 'Bearer token'})

    mock_requests_get.assert_called_once_with(
        'https://example.com/file.csv', headers={'Authorization': 'Bearer token'}, timeout=60
    )


@patch('utils.processing.requests.get')
def test_load_from_url_passes_http_headers_to_excel(mock_requests_get):
    sampler = DataSampler()
    # Create a real Excel file in memory to avoid parse errors
    from io import BytesIO

    excel_buffer = BytesIO()
    pd.DataFrame([['h1', 'h2'], ['v1', 'v2']]).to_excel(excel_buffer, index=False, header=False)

    mock_response = MagicMock()
    mock_response.content = excel_buffer.getvalue()
    mock_requests_get.return_value = mock_response

    sampler.load_from_url('https://example.com/file.xlsx', http_headers={'Authorization': 'Bearer token'})

    mock_requests_get.assert_called_once_with(
        'https://example.com/file.xlsx', headers={'Authorization': 'Bearer token'}, timeout=60
    )
