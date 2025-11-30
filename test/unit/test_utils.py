import pytest

from utils.utils import report_exists_in_ckan, determine_sensitivity, table_markdown

@pytest.mark.parametrize('reports, expected', [
    ([{'pii_sensitive': True, 'non_pii_sensitive': False}], 'sensitive-pii'),
    ([{'pii_sensitive': False, 'non_pii_sensitive': True}], 'sensitive-non-pii'),
    ([{'pii_sensitive': True, 'non_pii_sensitive': True}], 'sensitive-pii-and-non-pii'),
    ([{'pii_sensitive': False, 'non_pii_sensitive': False}], 'not-sensitive'),
])
def test_determine_sensitivity(reports, expected):
    assert determine_sensitivity(reports) == expected


def test_table_markdown(sample_report):
    assert table_markdown(sample_report) is not None
    assert "Alice" in table_markdown(sample_report)
    assert "Bob" in table_markdown(sample_report)
    assert "Charlie" in table_markdown(sample_report)
    assert "25" in table_markdown(sample_report)
    assert "30" in table_markdown(sample_report)
    assert "35" in table_markdown(sample_report)
    assert "US" in table_markdown(sample_report)
    assert "UK" in table_markdown(sample_report)
    assert "DE" in table_markdown(sample_report)
    assert "name - PERSON_NAME" in table_markdown(sample_report)
    assert "age - AGE" in table_markdown(sample_report)
    assert "country - STREET_ADDRESS" in table_markdown(sample_report)