from utils.processing import DataSampler
import pandas as pd
import pytest

sampler = DataSampler()
test_url_csv = 'https://dev.data-humdata-org.ahconu.org/dataset/a87f96f8-16e6-4d51-872c-cfa54a8251ec/resource/4ef001d1-7888-4f5d-98ce-0ca8006787f7/download/gdacs_rss_information.csv'
test_file_path = 'test/unit/downloads/gdacs_rss_information.csv'
test_file_xlsx = 'test/unit/downloads/Country Profiles Oct 14 2025.xlsx'


def test_init_datasampler():
    sampler = DataSampler()
    assert sampler is not None


def test_download_file():
    dfs = sampler.sample_from_url(test_url_csv)
    assert dfs is not None
    assert len(dfs) == 1
    assert dfs['sheet1'] is not None
    assert len(dfs['sheet1']) == 20


def test_load_file_xlsx():
    dfs = sampler.sample_from_url(test_file_xlsx)
    assert dfs is not None
    assert len(dfs) == 2
    assert dfs.get('Sheet1') is None
    assert dfs.get('EM-DAT (2025-10-14)') is not None
    assert len(dfs.get('EM-DAT (2025-10-14)')) == 20
    assert dfs.get('test') is not None


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
