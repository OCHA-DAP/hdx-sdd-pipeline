from utils.processing import DataSampler
import pandas as pd
import pytest

sampler = DataSampler()
test_url_csv = 'https://dev.data-humdata-org.ahconu.org/dataset/a87f96f8-16e6-4d51-872c-cfa54a8251ec/resource/4ef001d1-7888-4f5d-98ce-0ca8006787f7/download/gdacs_rss_information.csv'
test_file_path = 'test/unit/downloads/gdacs_rss_information.csv'


def test_init_datasampler():
    sampler = DataSampler()
    assert sampler is not None


def test_download_file():
    dfs = sampler.sample_from_url(test_url_csv)
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
    dfs = sampler.sample_from_url('test/unit/downloads/multicolumn_sample_nan.xlsx')
    assert dfs.get('Sheet1') is not None
    columns = dfs.get('Sheet1').columns.tolist()
    assert 'test | test header 1 | test subheader 1' in columns
    assert 'test | test header 1 | test subheader 2' in columns
    assert 'test | test header 2 | test subheader 3' in columns


def test_concatenate_header_multitable():
    with pytest.raises(ValueError):
        sampler.sample_from_url('test/unit/downloads/multitable.xlsx')


def test_unsupported_file_type():
    with pytest.raises(ValueError):
        sampler.sample_from_url('test/unit/downloads/unsupported.txt')


def test_empty_dataframe():
    df = pd.DataFrame()
    assert sampler._sample_dataframe(df) is not None
    assert len(sampler._sample_dataframe(df)) == 0
