import pandas as pd
import pytest
from models.sdd_report import SDDReport, PIIColumnReport
from utils.processing import DataSampler, table_markdown


def make_report(columns):
    """Utility to create SDDReport quickly."""
    return SDDReport(
        resource_id='r1',
        file_name='file.csv',
        file_url='http://example.com',
        processing_timestamp='2024-01-01',
        processing_success=True,
        n_records=10,
        n_columns=len(columns),
        columns=columns,
    )


def test_basic_markdown_output():
    columns = [
        PIIColumnReport('name', ['Alice', 'Bob'], {'entity_type': 'person_name'}),
        PIIColumnReport('age', ['30', '40'], {'entity_type': 'None'}),
    ]
    report = make_report(columns)

    md = table_markdown(report)

    # Should include "name - person_name" in header
    assert 'name - person_name' in md
    # age should not include "- None"
    assert 'age |' in md
    # markdown must contain table pipes
    assert '|' in md


def test_padding_of_shorter_column():
    columns = [
        PIIColumnReport('city', ['NYC'], {'entity_type': 'None'}),
        PIIColumnReport('zip', ['10001', '20002'], {'entity_type': 'None'}),
    ]
    report = make_report(columns)

    md = table_markdown(report)

    # Row 1 of "city" should be padded with empty string
    assert 'city' in md
    assert 'NYC' in md
    assert '10001' in md
    assert '20002' in md


def test_entity_type_none_handling():
    columns = [
        PIIColumnReport('email', ['a@example.com'], {'entity_type': 'email_address'}),
        PIIColumnReport('status', ['active'], {'entity_type': 'None'}),
    ]
    report = make_report(columns)

    md = table_markdown(report)

    assert 'email - email_address' in md
    assert 'status - None' not in md  # must not append "- None"


def test_markdown_with_irregular_sample_lengths():
    columns = [
        PIIColumnReport('col1', ['A', 'B', 'C'], {'entity_type': 'None'}),
        PIIColumnReport('col2', ['1'], {'entity_type': 'None'}),
    ]
    report = make_report(columns)

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
    columns = [
        PIIColumnReport('product', ['Book', 'Pen'], {'entity_type': 'product_name'}),
        PIIColumnReport('price', ['10', '2'], {'entity_type': 'price'}),
    ]
    report = make_report(columns)

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
    assert len(dfs['sheet1']) == 20


def test_concatenate_header():
    df = pd.read_excel('test/unit/downloads/multicolumn_sample.xlsx', header=None)
    sampler = DataSampler()
    df = sampler._concatenate_header(df)
    columns = df.columns.tolist()
    assert df is not None
    assert 'test | test header 1 | test subheader 1' in columns
    assert 'test | test header 1 | test subheader 2' in columns
    assert 'test | test header 2 | test subheader 3' in columns


def test_concatenate_header_nan():
    dfs = sampler.sample('test/unit/downloads/multicolumn_sample_nan.xlsx')
    assert dfs.get('Sheet1') is not None
    columns = dfs.get('Sheet1').columns.tolist()
    assert 'test | test header 1 | test subheader 1' in columns
    assert 'test | test header 1 | test subheader 2' in columns
    assert 'test | test header 2 | test subheader 3' in columns


def test_unsupported_file_type():
    with pytest.raises(ValueError):
        sampler.sample('test/unit/downloads/unsupported.txt')


def test_empty_dataframe():
    df = pd.DataFrame()
    assert sampler._sample_dataframe(df) is not None
    assert len(sampler._sample_dataframe(df)) == 0
