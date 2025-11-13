from utils.processing import DataSampler
import pandas as pd
import os
import numpy as np
import pytest

sampler = DataSampler(download_dir='test/unit/downloads')
test_url_csv = 'https://dev.data-humdata-org.ahconu.org/dataset/a87f96f8-16e6-4d51-872c-cfa54a8251ec/resource/4ef001d1-7888-4f5d-98ce-0ca8006787f7/download/gdacs_rss_information.csv'
test_file_path = 'test/unit/downloads/gdacs_rss_information.csv'
test_file_xlsx = 'test/unit/downloads/Country Profiles Oct 14 2025.xlsx'


def test_init_datasampler():
    sampler = DataSampler()
    assert sampler is not None


def test_download_file():
    file_path = sampler._download_file(test_url_csv)
    assert file_path is not None
    assert file_path.exists()


def test_load_file_csv():
    sheets = sampler._load_file(test_file_path)
    assert sheets is not None
    assert len(sheets) == 1
    assert sheets['sheet1'] is not None


def test_load_file_xlsx():
    sheets = sampler._load_file(test_file_xlsx)
    assert sheets is not None
    assert len(sheets) == 2
    assert sheets.get('Sheet1') is None
    assert sheets.get('EM-DAT (2025-10-14)') is not None
    assert len(sheets.get('EM-DAT (2025-10-14)')) == 199
    assert sheets.get('test') is not None


def test_sample_dataframe():
    sheets = sampler._load_file(test_file_xlsx)
    df = sampler._sample_dataframe(sheets.get('EM-DAT (2025-10-14)'))
    assert df is not None
    # Check that the dataframe is sampled
    assert len(df) == 20
    assert len(df.columns) == 13


def test_sample_from_url():
    # Remove test_file_path if it exists
    if os.path.exists(test_file_path):
        os.remove(test_file_path)
    sheets = sampler.sample_from_url(test_url_csv)
    assert sheets is not None
    assert len(sheets) == 1
    assert sheets['sheet1'] is not None
    if os.path.exists(test_file_path):
        os.remove(test_file_path)


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
    df = pd.read_excel('test/unit/downloads/multicolumn_sample_nan.xlsx', header=None)
    sampler = DataSampler()
    df = sampler._concatenate_header(df)
    columns = df.columns.tolist()
    assert df is not None
    assert 'test | test header 1 | test subheader 1' in columns
    assert 'test | test header 1 | test subheader 2' in columns
    assert 'test | test header 2 | test subheader 3' in columns


def test_concatenate_header_multitable():
    df = pd.read_excel('test/unit/downloads/multitable.xlsx', header=None)
    sampler = DataSampler()
    df = sampler._concatenate_header(df)
    columns = df.columns.tolist()
    expected = [
        'Table 1 col 1',
        'Table 1 col 2',
        'Table 1 col 3',
        'Table 1 col 4',
        'Table 1 col 5',
        'Table 1 col 6',
        np.nan,
        'Table 2 col 1',
        'Table 2 col 2',
        'Table 2 col 3',
        'Table 2 col 4',
    ]

    assert columns == pytest.approx(expected, nan_ok=True)
