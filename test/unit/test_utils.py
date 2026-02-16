import pytest

from utils.utils import determine_sensitivity, table_markdown


@pytest.mark.parametrize(
    'reports, expected',
    [
        ([{'personal_data_sensitive': True, 'non_personal_data_sensitive': False}], 'sensitive-pd'),
        ([{'personal_data_sensitive': False, 'non_personal_data_sensitive': True}], 'sensitive-non-pd'),
        ([{'personal_data_sensitive': True, 'non_personal_data_sensitive': True}],
         'sensitive-pd-and-non-pd'),
        ([{'personal_data_sensitive': False, 'non_personal_data_sensitive': False}], 'not-sensitive'),
    ],
)
def test_determine_sensitivity(reports, expected):
    assert determine_sensitivity(reports) == expected


def test_table_markdown(sample_report):
    assert table_markdown(sample_report) is not None
    assert 'Alice' in table_markdown(sample_report)
    assert 'Bob' in table_markdown(sample_report)
    assert 'Charlie' in table_markdown(sample_report)
    assert '25' in table_markdown(sample_report)
    assert '30' in table_markdown(sample_report)
    assert '35' in table_markdown(sample_report)
    assert 'US' in table_markdown(sample_report)
    assert 'UK' in table_markdown(sample_report)
    assert 'DE' in table_markdown(sample_report)
    assert 'name - PERSON_NAME' in table_markdown(sample_report)
    assert 'age - AGE' in table_markdown(sample_report)
    assert 'country - STREET_ADDRESS' in table_markdown(sample_report)
