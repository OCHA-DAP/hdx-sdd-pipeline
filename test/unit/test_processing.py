from utils.processing import DataSampler
import os

sampler = DataSampler(output_dir='test/unit/downloads')
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
    assert len(sheets.get('EM-DAT (2025-10-14)')) == 200
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
