# tests/test_sdd_report.py
import pytest
import json
from datetime import datetime

from models.sdd_report import SDDReport, PIIColumnReport, NonPIIReport


# ------------------------------------------------------------
# FIXTURES
# ------------------------------------------------------------
@pytest.fixture
def sample_pii_column():
    return PIIColumnReport(
        column_name='email',
        sample_values=['a@example.com', 'b@example.com'],
        pii={'entity_type': 'email_address', 'sensitive': True},
    )


@pytest.fixture
def sample_non_pii():
    return NonPIIReport(
        model_name='gpt-test',
        isp_used='default',
        sensitivity='HIGH',
        explanation='Test explanation',
        sensitive_columns=['email'],
        cited_isp_rules=['email is sensitive'],
    )


@pytest.fixture
def sdd_report(sample_pii_column):
    return SDDReport(
        resource_id='1',
        file_name='file.csv',
        file_url='http://example.com',
        processing_timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        processing_success=True,
        n_records=10,
        n_columns=2,
        columns=[sample_pii_column],
        completion_tokens=0,
        prompt_tokens=0,
    )


# ------------------------------------------------------------
# TEST: add_pii_column updates columns and pii_sensitive flag
# ------------------------------------------------------------
def test_add_pii_column_updates_flag():
    report = SDDReport(
        resource_id='1',
        file_name='file.csv',
        file_url='url',
        processing_timestamp='now',
        processing_success=True,
        n_records=0,
        n_columns=0,
    )

    col = PIIColumnReport(column_name='phone', sample_values=['123'], pii={'entity_type': 'phone', 'sensitive': True})
    report.add_pii_column(col)

    assert len(report.columns) == 1
    assert report.pii_sensitive is True


def test_add_pii_column_non_sensitive():
    report = SDDReport(
        resource_id='1',
        file_name='file.csv',
        file_url='url',
        processing_timestamp='now',
        processing_success=True,
        n_records=0,
        n_columns=0,
    )

    col = PIIColumnReport(column_name='id', sample_values=['1'], pii={'entity_type': 'None'})
    report.add_pii_column(col)

    assert len(report.columns) == 1
    assert report.pii_sensitive is False


# ------------------------------------------------------------
# TEST: add_non_pii_report sets non_pii_sensitive correctly
# ------------------------------------------------------------
def test_add_non_pii_report_high_sensitivity(sample_non_pii):
    report = SDDReport(
        resource_id='1',
        file_name='file.csv',
        file_url='url',
        processing_timestamp='now',
        processing_success=True,
        n_records=0,
        n_columns=0,
    )

    report.add_non_pii_report(sample_non_pii)
    assert report.non_pii_sensitive is True
    assert report.non_pii.model_name == 'gpt-test'


def test_add_non_pii_report_low_sensitivity():
    report = SDDReport(
        resource_id='1',
        file_name='file.csv',
        file_url='url',
        processing_timestamp='now',
        processing_success=True,
        n_records=0,
        n_columns=0,
    )
    non_pii = NonPIIReport(
        model_name='m',
        isp_used='default',
        sensitivity='LOW',
        explanation='none',
        sensitive_columns=[],
        cited_isp_rules=[],
    )
    report.add_non_pii_report(non_pii)
    assert report.non_pii_sensitive is False


# ------------------------------------------------------------
# TEST: update_pii_column updates entity_type and sensitive
# ------------------------------------------------------------
def test_update_pii_column_updates_fields(sdd_report):
    sdd_report.update_pii_column('email', entity_type='NAME', sensitive=False)
    col = sdd_report.columns[0]
    assert col.pii['entity_type'] == 'NAME'
    assert col.pii['sensitive'] is False
    # Report-level flag recomputed
    assert sdd_report.pii_sensitive is False


def test_update_nonexistent_column_does_nothing(sdd_report):
    sdd_report.update_pii_column('nonexistent', entity_type='NAME', sensitive=True)
    # Original column unchanged
    col = sdd_report.columns[0]
    assert col.pii['entity_type'] == 'email_address'


# ------------------------------------------------------------
# TEST: to_dict / to_json / from_json
# ------------------------------------------------------------
def test_serialization_roundtrip(sdd_report):
    json_str = sdd_report.to_json()
    data_dict = json.loads(json_str)

    assert data_dict['resource_id'] == sdd_report.resource_id
    assert 'columns' in data_dict
    assert data_dict['columns'][0]['column_name'] == 'email'

    # Recreate from JSON string
    report_copy = SDDReport.from_json(json_str)
    assert isinstance(report_copy, SDDReport)
    assert report_copy.columns[0].column_name == 'email'
    assert report_copy.non_pii is None

    # Recreate from dict
    report_copy2 = SDDReport.from_json(data_dict)
    assert report_copy2.columns[0].column_name == 'email'


# ------------------------------------------------------------
# TEST: from_json handles non_pii correctly
# ------------------------------------------------------------
def test_from_json_with_non_pii(sample_non_pii):
    data = {
        'resource_id': '1',
        'file_name': 'file.csv',
        'file_url': 'url',
        'processing_timestamp': 'now',
        'processing_success': True,
        'n_records': 0,
        'n_columns': 0,
        'columns': [],
        'non_pii': sample_non_pii.to_dict(),
    }

    report = SDDReport.from_json(data)
    assert report.non_pii.sensitivity == 'HIGH'
    assert isinstance(report.non_pii, NonPIIReport)


def test_pii_column_report_to_dict():
    report = PIIColumnReport(
        column_name='email',
        sample_values=['a@example.com', 'b@example.com'],
        pii={'entity_type': 'email_address', 'sensitive': True},
    )
    assert report.to_dict() is not None
    assert report.to_dict()['column_name'] == 'email'
    assert report.to_dict()['sample_values'] == ['a@example.com', 'b@example.com']
    assert report.to_dict()['pii'] == {'entity_type': 'email_address', 'sensitive': True}
