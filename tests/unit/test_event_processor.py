import pytest
from unittest.mock import MagicMock, patch, mock_open
from src.event_processor import EventProcessor
from src.domain.entities.sheet_report import SheetReport


@pytest.fixture
def mock_config():
    with patch('src.event_processor.get_config') as mock:
        config = MagicMock()
        config.CKAN_UPDATE = True
        config.HDX_URL = 'http://test-hdx'
        config.HDX_KEY = 'test-key'
        mock.return_value = config
        yield config


@pytest.fixture
def mock_pipeline_factory():
    with patch('src.event_processor.PipelineFactory') as mock:
        yield mock


@pytest.fixture
def mock_ckan_client():
    with patch('src.event_processor.CKANClient') as mock:
        yield mock


def test_init_with_ckan(mock_config, mock_pipeline_factory, mock_ckan_client):
    mock_config.CKAN_UPDATE = True
    processor = EventProcessor()
    assert processor.ckan is not None
    mock_ckan_client.assert_called_once()


def test_init_without_ckan(mock_config, mock_pipeline_factory, mock_ckan_client):
    mock_config.CKAN_UPDATE = False
    processor = EventProcessor()
    assert processor.ckan is None
    mock_ckan_client.assert_not_called()


def test_process_event_missing_resource_id(mock_config, mock_pipeline_factory):
    processor = EventProcessor()
    success, message = processor.process_event({})
    assert not success
    assert 'Missing resource_id' in message


def test_process_event_report_exists(mock_config, mock_pipeline_factory, mock_ckan_client):
    processor = EventProcessor()
    processor.ckan.resource_show.return_value = {'sdd_report': 'some report'}

    event = {'resource_id': '123'}
    success, message = processor.process_event(event)
    assert success
    assert 'Already processed' in message


def test_process_event_no_download_url(mock_config, mock_pipeline_factory, mock_ckan_client):
    processor = EventProcessor()
    processor.ckan.resource_show.return_value = {}  # No download_url inside CKAN resource

    event = {'resource_id': '123'}
    success, message = processor.process_event(event)
    assert not success
    assert 'No download URL' in message


def test_process_event_success(mock_config, mock_pipeline_factory, mock_ckan_client):
    processor = EventProcessor()
    processor.ckan.resource_show.return_value = {'download_url': 'http://example.com/data.csv'}
    processor.isp_retriever.get_isp_rules = MagicMock(return_value={})
    processor.pipeline.execute.return_value = []
    processor._save_to_ckan = MagicMock()

    event = {'resource_id': '123', 'package_id': 'pkg123'}
    success, message = processor.process_event(event)

    assert success
    assert 'Processed successfully' in message
    processor.pipeline.execute.assert_called_once()
    processor._save_to_ckan.assert_called_once()


def test_process_event_uses_dataset_id_for_isp_lookup(mock_config, mock_pipeline_factory, mock_ckan_client):
    processor = EventProcessor()
    processor.ckan.resource_show.return_value = {'download_url': 'http://example.com/data.csv', 'name': 'data.csv'}
    processor.isp_retriever.get_isp_rules = MagicMock(return_value={})
    processor.pipeline.execute.return_value = []
    processor._save_to_ckan = MagicMock()

    event = {'resource_id': '123', 'dataset_id': 'ds-123'}
    success, _ = processor.process_event(event)

    assert success
    processor.isp_retriever.get_isp_rules.assert_called_once_with('ds-123', 'data.csv', processor.ckan)


def test_process_event_exception(mock_config, mock_pipeline_factory):
    processor = EventProcessor()
    processor.ckan = MagicMock()
    processor.ckan.resource_show.side_effect = Exception('CKAN Error')
    processor.slack = MagicMock()

    event = {'resource_id': '123'}
    success, message = processor.process_event(event)
    assert not success
    assert 'Processing failed' in message
    processor.slack.post_to_slack_channel.assert_called_once()



def test_determine_sensitivity_sensitive(mock_config, mock_pipeline_factory):
    processor = EventProcessor()
    report = MagicMock(spec=SheetReport)
    report.personal_data_sensitive = True
    report.non_personal_data_sensitive = False
    assert processor._determine_sensitivity([report]) == 'sensitive-pd'


def test_determine_sensitivity_non_sensitive(mock_config, mock_pipeline_factory):
    processor = EventProcessor()
    report = MagicMock(spec=SheetReport)
    report.personal_data_sensitive = False
    report.non_personal_data_sensitive = False
    assert processor._determine_sensitivity([report]) == 'not-sensitive'


def test_save_to_ckan(mock_config, mock_pipeline_factory, mock_ckan_client):
    processor = EventProcessor()
    processor._save_to_ckan('123', [], 'sensitive')
    processor.ckan.update_resource_fields.assert_called_once()


def test_save_to_local_file(mock_config, mock_pipeline_factory, mock_ckan_client):
    # Setup for local save
    mock_config.CKAN_UPDATE = False
    processor = EventProcessor()

    with patch('builtins.open', mock_open()) as mock_file:
        with patch('pathlib.Path.mkdir') as mock_mkdir:
            processor._save_to_ckan('123', [], 'sensitive')

    # Verify file was opened for writing
    mock_file.assert_called_once()
    mock_mkdir.assert_called_once()


def test_report_exists_no_ckan(mock_config, mock_pipeline_factory):
    mock_config.CKAN_UPDATE = False
    processor = EventProcessor()
    assert processor._report_exists('123') is False


def test_report_exists_exception(mock_config, mock_pipeline_factory, mock_ckan_client):
    processor = EventProcessor()
    processor.ckan.resource_show.side_effect = Exception('DB Error')
    with pytest.raises(Exception, match='DB Error'):
        processor._report_exists('123')
