import pytest
import pandas as pd
from unittest.mock import MagicMock

import main


# ----------------------------------------------------------------------
# Helper Fixtures
# ----------------------------------------------------------------------
@pytest.fixture
def fake_config(mocker):
    cfg = MagicMock()
    cfg.PII_DETECT_MODEL = 'pii-model'
    cfg.PII_REFLECT_MODEL = 'pii-reflect'
    cfg.NON_PII_DETECT_MODEL = 'non-pii-model'
    cfg.README_SCAN_MODEL = 'readme-model'
    cfg.HDX_URL = 'https://example.com'
    cfg.HDX_KEY = '123'
    cfg.RERUN = True
    mocker.patch('main.config', cfg)
    return cfg


# ----------------------------------------------------------------------
# process_sheet
# ----------------------------------------------------------------------
def test_process_sheet(mocker, fake_config):
    # Mock classifiers
    pii = mocker.patch('main.PIIClassifier').return_value
    pii.classify_df.return_value = MagicMock(to_dict=lambda: {'pii_sensitive': True}, __str__=lambda x: 'pii_report')

    reflection = mocker.patch('main.PIIReflectionClassifier').return_value
    reflection.classify_df.return_value = pii.classify_df.return_value

    non_pii = mocker.patch('main.NonPIIClassifier').return_value
    non_pii.classify.return_value = pii.classify_df.return_value

    mocker.patch('main.table_markdown', return_value='markdown-table')

    df = pd.DataFrame({'a': [1, 2]})
    isp = {'default': {'country': 'test'}}

    res = main.process_sheet(
        df=df,
        sheet_name='Sheet1',
        file_name='file.csv',
        download_url='http://example.com/file',
        resource_id='rid',
        isp=isp,
    )

    assert isinstance(res, dict)
    assert res.get('pii_sensitive') is True


# ----------------------------------------------------------------------
# determine_sensitivity
# ----------------------------------------------------------------------
@pytest.mark.parametrize(
    'reports, expected',
    [
        ([{'pii_sensitive': True, 'non_pii_sensitive': True}], 'sensitive-pii-and-non-pii'),
        ([{'pii_sensitive': True, 'non_pii_sensitive': False}], 'sensitive-pii'),
        ([{'pii_sensitive': False, 'non_pii_sensitive': True}], 'sensitive-non-pii'),
        ([{'pii_sensitive': False, 'non_pii_sensitive': False}], 'not-sensitive'),
    ],
)
def test_determine_sensitivity(reports, expected):
    assert main.determine_sensitivity(reports) == expected


# ----------------------------------------------------------------------
# event_processor: missing resource_id
# ----------------------------------------------------------------------
def test_event_processor_missing_resource_id(fake_config):
    ok, msg = main.event_processor({})
    assert ok is False
    assert msg == 'Missing resource_id'


# ----------------------------------------------------------------------
# event_processor: README sheet path
# ----------------------------------------------------------------------
def test_event_processor_readme_path(mocker, fake_config):
    # Mock CKAN resource
    ckan = mocker.patch('main.CKANClient').return_value
    ckan.resource_show.return_value = {
        'download_url': 'http://example.com/file',
        'name': 'dataset.xlsx',
        'sdd_report': None,
    }

    ckan.update_resource_fields.return_value = True

    # Mock DataSampler returning a README sheet
    df = pd.DataFrame({'x': [1]})
    sampler = mocker.patch('main.DataSampler').return_value
    sampler.sample.return_value = {'readme': df}

    # Mock README Classifier
    readme_mock = mocker.patch('main.ReadMeScanClassifier').return_value
    readme_mock.classify_readme.return_value = ({'contains_pii': True}, 50, 100)

    mocker.patch('main.load_isp_info', return_value={'default': {}})

    ok, msg = main.event_processor({'resource_id': 'RID123'})
    assert ok is True
    assert 'Processed successfully' in msg

    # verify CKAN update called
    assert ckan.update_resource_fields.called


# ----------------------------------------------------------------------
# event_processor: standard non-readme sheet path
# ----------------------------------------------------------------------
def test_event_processor_normal_sheet(mocker, fake_config):
    ckan = mocker.patch('main.CKANClient').return_value
    ckan.resource_show.return_value = {
        'download_url': 'http://example.com/file',
        'name': 'dataset.csv',
        'sdd_report': None,
    }

    ckan.update_resource_fields.return_value = True

    df = pd.DataFrame({'a': [1]})
    sampler = mocker.patch('main.DataSampler').return_value
    sampler.sample.return_value = {'sheet1': df}

    mocker.patch('main.load_isp_info', return_value={'default': {}})

    # Mock process_sheet directly to simplify
    mocker.patch('main.process_sheet', return_value={'pii_sensitive': False, 'non_pii_sensitive': False})

    ok, msg = main.event_processor({'resource_id': 'RID789'})
    assert ok is True
    assert 'Processed successfully' in msg
    assert ckan.update_resource_fields.called
