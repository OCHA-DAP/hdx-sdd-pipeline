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


# def test_process_event_report_exists(mock_config, mock_pipeline_factory, mock_ckan_client):
#     processor = EventProcessor()
#     processor.ckan.resource_show.return_value = {'sdd_report': 'some report'}

#     event = {'resource_id': '123'}
#     success, message = processor.process_event(event)
#     assert success
#     assert 'Already processed' in message


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


# def test_process_event_exception(mock_config, mock_pipeline_factory):
#     processor = EventProcessor()
#     processor.ckan = MagicMock()
#     processor.ckan.resource_show.side_effect = Exception('CKAN Error')
#     processor.slack = MagicMock()

#     event = {'resource_id': '123'}
#     success, message = processor.process_event(event)
#     assert not success
#     assert 'Processing failed' in message
#     processor.slack.post_to_slack_channel.assert_called_once()


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


def test_determine_sensitivity_level_success(mock_config, mock_pipeline_factory):
    processor = EventProcessor()

    # Using SheetReports
    report1 = MagicMock(spec=SheetReport)
    report1.personal_data_risk_level = 1
    report1.non_personal_data_risk_level = 2

    report2 = MagicMock(spec=SheetReport)
    report2.personal_data_risk_level = 3
    report2.non_personal_data_risk_level = 0

    assert processor._determine_sensitivity_level([report1, report2]) == 3

    # Using dicts
    dict_reports = [
        {'personal_data_risk_level': 1, 'non_personal_data_risk_level': 2},
        {'personal_data_risk_level': 3, 'non_personal_data_risk_level': 0},
    ]
    assert processor._determine_sensitivity_level(dict_reports) == 3


def test_save_to_ckan_includes_sensitivity_level(mock_config, mock_pipeline_factory, mock_ckan_client):
    processor = EventProcessor()
    processor._save_to_ckan('123', [], 'sensitive', 3)
    processor.ckan.update_resource_fields.assert_called_once_with(
        '123', {'sdd_report': '[]', 'sensitive': 'sensitive', 'sensitivity_level': 3}
    )


def test_process_event_metadata_extraction(mock_config, mock_pipeline_factory, mock_ckan_client):
    processor = EventProcessor()

    # Mock CKAN calls
    resource_data = {
        'download_url': 'http://example.com/data.csv',
        'name': 'resource_name_from_ckan.csv',
        'description': 'resource_description_from_ckan',
        'package_id': 'pkg-123',
    }
    package_data = {
        'title': 'dataset_title_from_ckan',
        'notes': 'dataset_notes_from_ckan',
        'dataset_source': 'dataset_source_from_ckan',
        'groups': [{'name': 'afg', 'title': 'Afghanistan'}],
        'organization': {'name': 'ocha', 'title': 'OCHA Office'},
    }

    processor.ckan.resource_show.return_value = resource_data
    processor.ckan.package_show.return_value = package_data
    processor.isp_retriever.get_isp_rules = MagicMock(return_value={})
    processor.pipeline.execute.return_value = []
    processor._save_to_ckan = MagicMock()

    event = {
        'resource_id': 'res-123',
        'dataset_id': 'ds-123',
        'file_name': 'ignored.csv',
        'dataset_title': 'ignored_title',
    }

    success, _ = processor.process_event(event)
    assert success

    # Verify metadata passed to execute
    processor.pipeline.execute.assert_called_once()
    _, kwargs = processor.pipeline.execute.call_args
    assert 'metadata' in kwargs
    metadata = kwargs['metadata']

    assert metadata['resource_name'] == 'resource_name_from_ckan.csv'
    assert metadata['resource_description'] == 'resource_description_from_ckan'
    assert metadata['dataset_title'] == 'dataset_title_from_ckan'
    assert metadata['dataset_description'] == 'dataset_notes_from_ckan'
    assert metadata['dataset_source'] == 'dataset_source_from_ckan'
    assert metadata['dataset_location'] == 'Afghanistan'
    assert metadata['organization_title'] == 'OCHA Office'


def test_process_event_metadata_extraction_no_ckan(mock_config, mock_pipeline_factory, mock_ckan_client):
    mock_config.CKAN_UPDATE = False
    processor = EventProcessor()
    processor.isp_retriever.get_isp_rules = MagicMock(return_value={})
    processor.pipeline.execute.return_value = []
    processor._save_to_local_file = MagicMock()

    event = {
        'resource_id': 'res-123',
        'download_url': 'http://example.com/data.csv',
        'file_name': 'event_file.csv',
        'resource_description': 'event_res_desc',
        'dataset_title': 'event_ds_title',
        'dataset_description': 'event_ds_desc',
        'dataset_source': 'event_source',
        'dataset_location': 'event_loc',
        'organization_title': 'event_org',
    }

    success, _ = processor.process_event(event)
    assert success

    # Verify metadata passed to execute
    processor.pipeline.execute.assert_called_once()
    _, kwargs = processor.pipeline.execute.call_args
    assert 'metadata' in kwargs
    metadata = kwargs['metadata']

    assert metadata['resource_name'] == 'event_file.csv'
    assert metadata['resource_description'] == 'event_res_desc'
    assert metadata['dataset_title'] == 'event_ds_title'
    assert metadata['dataset_description'] == 'event_ds_desc'
    assert metadata['dataset_source'] == 'event_source'
    assert metadata['dataset_location'] == 'event_loc'
    assert metadata['organization_title'] == 'event_org'


def test_process_event_metadata_local_json_folder(mock_config, mock_pipeline_factory, mock_ckan_client):
    mock_config.CKAN_UPDATE = False
    processor = EventProcessor()
    processor.isp_retriever.get_isp_rules = MagicMock(return_value={})
    processor.pipeline.execute.return_value = []
    processor._save_to_local_file = MagicMock()

    event = {
        'resource_id': 'res-123',
        'download_url': 'http://example.com/event_file.csv',
        'file_name': 'event_file.csv',
    }

    mock_json_content = (
        '{"dataset_title": "local_json_title", '
        '"dataset_description": "local_json_desc", '
        '"dataset_source": "local_json_source", '
        '"dataset_location": "local_json_location", '
        '"organization_title": "local_json_org", '
        '"resource_name": "local_json_res_name", '
        '"resource_description": "local_json_res_desc"}'
    )

    with (
        patch('pathlib.Path.exists', return_value=True),
        patch('builtins.open', mock_open(read_data=mock_json_content)),
    ):
        success, _ = processor.process_event(event)
        assert success

    processor.pipeline.execute.assert_called_once()
    _, kwargs = processor.pipeline.execute.call_args
    metadata = kwargs['metadata']

    assert metadata['dataset_title'] == 'local_json_title'
    assert metadata['dataset_description'] == 'local_json_desc'
    assert metadata['dataset_source'] == 'local_json_source'
    assert metadata['dataset_location'] == 'local_json_location'
    assert metadata['organization_title'] == 'local_json_org'
    assert metadata['resource_name'] == 'local_json_res_name'
    assert metadata['resource_description'] == 'local_json_res_desc'


def test_process_event_metadata_truncation(mock_config, mock_pipeline_factory, mock_ckan_client):
    mock_config.CKAN_UPDATE = False
    processor = EventProcessor()
    processor.isp_retriever.get_isp_rules = MagicMock(return_value={})
    processor.pipeline.execute.return_value = []
    processor._save_to_local_file = MagicMock()

    long_desc = "a" * 1050
    event = {
        'resource_id': 'res-123',
        'download_url': 'http://example.com/data.csv',
        'file_name': 'event_file.csv',
        'resource_description': long_desc,
        'dataset_title': 'event_ds_title',
        'dataset_description': long_desc,
    }

    success, _ = processor.process_event(event)
    assert success

    processor.pipeline.execute.assert_called_once()
    _, kwargs = processor.pipeline.execute.call_args
    metadata = kwargs['metadata']

    assert len(metadata['resource_description']) == 1000
    assert metadata['resource_description'] == "a" * 1000
    assert len(metadata['dataset_description']) == 1000
    assert metadata['dataset_description'] == "a" * 1000


def test_clean_dataset_location():
    from src.event_processor import clean_dataset_location
    # Test string with <= 5 locations
    assert clean_dataset_location("Nepal, Netherlands, Nicaragua") == "Nepal, Netherlands, Nicaragua"
    # Test string with > 5 locations
    assert clean_dataset_location("Nepal, Netherlands, Nicaragua, New Zealand, Sudan, Yemen") is None
    # Test list with <= 5 locations
    assert clean_dataset_location(["Nepal", "Netherlands", "Nicaragua"]) == "Nepal, Netherlands, Nicaragua"
    # Test list with > 5 locations
    assert clean_dataset_location(["Nepal", "Netherlands", "Nicaragua", "New Zealand", "Sudan", "Yemen"]) is None
    # Test invalid / empty inputs
    assert clean_dataset_location(None) is None
    assert clean_dataset_location([]) is None


def test_process_event_metadata_location_cleaning(mock_config, mock_pipeline_factory, mock_ckan_client):
    processor = EventProcessor()

    # Test event with too many locations
    processor.ckan.resource_show.return_value = {'download_url': 'http://example.com/data.csv', 'name': 'data.csv'}
    # Mock package metadata with > 5 groups (locations)
    groups = [
        {'title': 'Nepal'},
        {'title': 'Netherlands'},
        {'title': 'Nicaragua'},
        {'title': 'New Zealand'},
        {'title': 'Sudan'},
        {'title': 'Yemen'}
    ]
    processor.ckan.package_show.return_value = {
        'title': 'Test Dataset',
        'groups': groups
    }
    processor.isp_retriever.get_isp_rules = MagicMock(return_value={})
    processor.pipeline.execute.return_value = []
    processor._save_to_ckan = MagicMock()

    event = {'resource_id': 'res-123', 'dataset_id': 'ds-123'}
    success, _ = processor.process_event(event)
    assert success

    processor.pipeline.execute.assert_called_once()
    _, kwargs = processor.pipeline.execute.call_args
    metadata = kwargs['metadata']
    # locations should be omitted (None) because count > 5
    assert metadata['dataset_location'] is None




def test_process_event_package_show_with_resources(mock_config, mock_pipeline_factory, mock_ckan_client):
    processor = EventProcessor()
    
    # Package has resources list
    package_data = {
        "resources": [
            {"id": "other-123"},
            {"id": "res-123", "download_url": "http://example.com/res.csv", "name": "res.csv"}
        ]
    }
    processor.ckan.package_show.return_value = package_data
    processor.isp_retriever.get_isp_rules = MagicMock(return_value={})
    processor.pipeline.execute.return_value = []
    processor._save_to_ckan = MagicMock()
    processor._report_exists = MagicMock(return_value=False)
    
    event = {"resource_id": "res-123", "dataset_id": "ds-123"}
    success, _ = processor.process_event(event)
    
    assert success
    # Should not fall back to resource_show
    processor.ckan.resource_show.assert_not_called()
    processor.pipeline.execute.assert_called_once()
    assert processor.pipeline.execute.call_args[1]["metadata"]["resource_name"] == "res.csv"


def test_process_event_package_show_exception(mock_config, mock_pipeline_factory, mock_ckan_client):
    processor = EventProcessor()
    processor._report_exists = MagicMock(return_value=False)
    
    processor.ckan.package_show.side_effect = Exception("Package error")
    processor.ckan.resource_show.return_value = {
        "download_url": "http://example.com/res.csv", 
        "name": "res.csv",
        "package_id": "ds-123"
    }
    processor.isp_retriever.get_isp_rules = MagicMock(return_value={})
    processor.pipeline.execute.return_value = []
    processor._save_to_ckan = MagicMock()
    
    event = {"resource_id": "res-123", "dataset_id": "ds-123"}
    success, _ = processor.process_event(event)
    
    assert success
    processor.ckan.resource_show.assert_called_once()


def test_process_event_local_metadata_exception(mock_config, mock_pipeline_factory, mock_ckan_client):
    mock_config.CKAN_UPDATE = False
    processor = EventProcessor()
    processor.isp_retriever.get_isp_rules = MagicMock(return_value={})
    processor.pipeline.execute.return_value = []
    processor._save_to_local_file = MagicMock()

    event = {
        "resource_id": "res-123",
        "download_url": "http://example.com/event_file.csv",
        "file_name": "event_file.csv",
    }

    with patch("pathlib.Path.exists", return_value=True):
        with patch("builtins.open", side_effect=Exception("Read error")):
            success, _ = processor.process_event(event)
            assert success

def test_determine_sensitivity_sensitive_non_pd(mock_config, mock_pipeline_factory):
    processor = EventProcessor()
    report = MagicMock(spec=SheetReport)
    report.personal_data_sensitive = False
    report.non_personal_data_sensitive = True
    assert processor._determine_sensitivity([report]) == "sensitive-non-pd"

def test_determine_sensitivity_sensitive_pd_and_non_pd(mock_config, mock_pipeline_factory):
    processor = EventProcessor()
    report = MagicMock(spec=SheetReport)
    report.personal_data_sensitive = True
    report.non_personal_data_sensitive = True
    assert processor._determine_sensitivity([report]) == "sensitive-pd-and-non-pd"

def test_save_to_local_file_custom_output_dir(mock_config, mock_pipeline_factory, mock_ckan_client):
    mock_config.CKAN_UPDATE = False
    from pathlib import Path
    processor = EventProcessor(custom_output_path=str(Path("/tmp/custom_dir")))
    
    with patch("pathlib.Path.is_dir", return_value=True):
        with patch("pathlib.Path.mkdir"):
            with patch("builtins.open", mock_open()) as mock_file:
                processor._save_to_ckan("123", [], "sensitive")
                mock_file.assert_called_once()

def test_save_to_local_file_custom_output_file(mock_config, mock_pipeline_factory, mock_ckan_client):
    mock_config.CKAN_UPDATE = False
    from pathlib import Path
    processor = EventProcessor(custom_output_path=str(Path("/tmp/custom_file.json")))
    
    with patch("pathlib.Path.is_dir", return_value=False):
        with patch("pathlib.Path.mkdir"):
            with patch("builtins.open", mock_open()) as mock_file:
                processor._save_to_ckan("123", [], "sensitive")
                mock_file.assert_called_once()

def test_main_execution(mock_config, mock_pipeline_factory, mock_ckan_client):
    from unittest.mock import patch
    with patch("src.event_processor.EventProcessor.process_event") as mock_process:
        mock_process.return_value = (True, "Success")
        from src.event_processor import main
        main()
        mock_process.assert_called_once()

def test_main_execution_failure(mock_config, mock_pipeline_factory, mock_ckan_client):
    from unittest.mock import patch
    with patch("src.event_processor.EventProcessor.process_event") as mock_process:
        mock_process.return_value = (False, "Failed")
        from src.event_processor import main
        main()
        mock_process.assert_called_once()
