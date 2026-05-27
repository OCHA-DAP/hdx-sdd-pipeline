"""
Tests for research/run_sdd_hdx.py
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers — import the module under test after patching sys.path manipulation
# so tests don't depend on project layout.
# ---------------------------------------------------------------------------

# Provide stub modules for the local imports used inside the lazy functions
# (config, src.*).  We inject them before importing the script so that
# Python resolves them from sys.modules rather than the file system.


@pytest.fixture(autouse=True)
def stub_local_modules():
    """Inject lightweight stubs for all project-internal imports."""
    fake_config_module = MagicMock()
    fake_event_processor_module = MagicMock()
    fake_ckan_module = MagicMock()

    with patch.dict(
        sys.modules,
        {
            'config': fake_config_module,
            'src': MagicMock(),
            'src.event_processor': fake_event_processor_module,
            'src.shared': MagicMock(),
            'src.shared.utils': MagicMock(),
            'src.shared.utils.ckan': fake_ckan_module,
            'dotenv': MagicMock(),
        },
    ):
        yield {
            'config': fake_config_module,
            'event_processor': fake_event_processor_module,
            'ckan': fake_ckan_module,
        }


# Import the module under test *inside* a function so the autouse fixture
# above is always applied first.
@pytest.fixture()
def sut(stub_local_modules):
    import importlib
    import research.run_sdd_hdx as m

    importlib.reload(m)
    return m


# ---------------------------------------------------------------------------
# BatchResult
# ---------------------------------------------------------------------------


class TestBatchResult:
    def test_initial_counts_are_zero(self, sut):
        r = sut.BatchResult(total=5)
        assert r.successful == 0
        assert r.skipped == 0
        assert r.failed == 0

    def test_record_increments_correct_field(self, sut):
        r = sut.BatchResult(total=3)
        r.record('successful')
        r.record('successful')
        r.record('failed')
        assert r.successful == 2
        assert r.failed == 1
        assert r.skipped == 0

    def test_record_unknown_outcome_raises(self, sut):
        r = sut.BatchResult(total=1)
        with pytest.raises(AttributeError):
            r.record('nonexistent')

    def test_print_summary_contains_counts(self, sut, capsys):
        r = sut.BatchResult(total=10, successful=7, skipped=2, failed=1)
        r.print_summary(Path('some/dir'))
        out = capsys.readouterr().out
        assert '10' in out
        assert '7' in out
        assert '2' in out
        assert '1' in out
        assert 'some/dir' in out


# ---------------------------------------------------------------------------
# _bootstrap
# ---------------------------------------------------------------------------


class TestBootstrap:
    def test_appends_cwd_to_syspath(self, sut):
        import os

        cwd = os.getcwd()
        original_path = sys.path.copy()
        try:
            sut._bootstrap()
            assert cwd in sys.path
        finally:
            sys.path[:] = original_path

    def test_calls_load_dotenv(self, sut):
        import dotenv

        sut._bootstrap()
        dotenv.load_dotenv.assert_called()


# ---------------------------------------------------------------------------
# build_event_processor
# ---------------------------------------------------------------------------


class TestBuildEventProcessor:
    def test_sets_model_on_all_tasks(self, sut, stub_local_modules):
        fake_config = MagicMock()
        stub_local_modules['config'].get_config.return_value = fake_config

        sut.build_event_processor('my-model', Path('/tmp/out'))

        assert fake_config.PII_DETECT_MODEL == 'my-model'
        assert fake_config.PII_REFLECT_MODEL == 'my-model'
        assert fake_config.NON_PII_DETECT_MODEL == 'my-model'

    def test_enables_detection_steps(self, sut, stub_local_modules):
        fake_config = MagicMock()
        stub_local_modules['config'].get_config.return_value = fake_config

        sut.build_event_processor('some-model', Path('/tmp/out'))

        assert fake_config.PERSONAL_DATA_DETECTION is True
        assert fake_config.PERSONAL_DATA_REFLECTION is True
        assert fake_config.NON_PERSONAL_DATA_DETECTION is True

    def test_disables_ckan_update(self, sut, stub_local_modules):
        fake_config = MagicMock()
        stub_local_modules['config'].get_config.return_value = fake_config

        sut.build_event_processor('some-model', Path('/tmp/out'))

        assert fake_config.CKAN_UPDATE is False

    def test_passes_output_path_to_event_processor(self, sut, stub_local_modules):
        fake_config = MagicMock()
        stub_local_modules['config'].get_config.return_value = fake_config
        fake_ep_cls = stub_local_modules['event_processor'].EventProcessor

        sut.build_event_processor('m', Path('/my/output'))

        fake_ep_cls.assert_called_once_with(custom_output_path='/my/output', config=fake_config)

    def test_returns_event_processor_instance(self, sut, stub_local_modules):
        stub_local_modules['config'].get_config.return_value = MagicMock()
        fake_instance = MagicMock()
        stub_local_modules['event_processor'].EventProcessor.return_value = fake_instance

        result = sut.build_event_processor('m', Path('/out'))

        assert result is fake_instance


# ---------------------------------------------------------------------------
# build_ckan_client
# ---------------------------------------------------------------------------


class TestBuildCkanClient:
    def test_passes_config_values_to_ckan(self, sut, stub_local_modules):
        fake_config = MagicMock()
        fake_config.HDX_URL_PROD = 'https://hdx.example.com'
        fake_config.HDX_KEY_PROD = 'secret-key'
        fake_config.SDD_USER_AGENT = 'my-agent'
        stub_local_modules['config'].get_config.return_value = fake_config
        fake_ckan_cls = stub_local_modules['ckan'].CKANClient

        sut.build_ckan_client()

        fake_ckan_cls.assert_called_once_with(
            base_url='https://hdx.example.com',
            api_token='secret-key',
            user_agent='my-agent',
        )

    def test_returns_ckan_instance(self, sut, stub_local_modules):
        stub_local_modules['config'].get_config.return_value = MagicMock()
        fake_instance = MagicMock()
        stub_local_modules['ckan'].CKANClient.return_value = fake_instance

        result = sut.build_ckan_client()

        assert result is fake_instance


# ---------------------------------------------------------------------------
# fetch_event
# ---------------------------------------------------------------------------


class TestFetchEvent:
    def _make_ckan(self, resource: dict | None) -> MagicMock:
        ckan = MagicMock()
        ckan.resource_show.return_value = resource
        return ckan

    def test_returns_event_dict_on_success(self, sut):
        ckan = self._make_ckan(
            {
                'download_url': 'https://example.com/file.csv',
                'package_id': 'pkg-123',
                'name': 'my-file.csv',
            }
        )
        event = sut.fetch_event(ckan, 'res-abc')
        assert event == {
            'resource_id': 'res-abc',
            'dataset_id': 'pkg-123',
            'download_url': 'https://example.com/file.csv',
            'file_name': 'my-file.csv',
            'event_type': 'batch-processing',
        }

    def test_returns_none_when_resource_not_found(self, sut):
        ckan = self._make_ckan(None)
        assert sut.fetch_event(ckan, 'missing-id') is None

    def test_returns_none_when_no_download_url(self, sut):
        ckan = self._make_ckan({'package_id': 'pkg-1', 'name': 'f.csv'})
        assert sut.fetch_event(ckan, 'res-1') is None

    def test_falls_back_to_default_filename(self, sut):
        ckan = self._make_ckan(
            {
                'download_url': 'https://example.com/f.csv',
                'package_id': 'pkg-1',
                # no 'name' key
            }
        )
        event = sut.fetch_event(ckan, 'res-xyz')
        assert event['file_name'] == 'res-xyz.csv'


# ---------------------------------------------------------------------------
# process_resource
# ---------------------------------------------------------------------------


class TestProcessResource:
    def _make_processor(self, success: bool = True, message: str = 'ok') -> MagicMock:
        p = MagicMock()
        p.process_event.return_value = (success, message)
        return p

    def _make_ckan(self, resource: dict | None = None) -> MagicMock:
        c = MagicMock()
        c.resource_show.return_value = resource or {
            'download_url': 'https://example.com/f.csv',
            'package_id': 'pkg-1',
            'name': 'f.csv',
        }
        return c

    def test_returns_successful_on_success(self, sut, tmp_path):
        result = sut.process_resource('res-1', self._make_ckan(), self._make_processor(True), tmp_path, False)
        assert result == 'successful'

    def test_returns_failed_on_processor_failure(self, sut, tmp_path):
        result = sut.process_resource('res-1', self._make_ckan(), self._make_processor(False, 'boom'), tmp_path, False)
        assert result == 'failed'

    def test_returns_skipped_when_file_exists_and_skip_enabled(self, sut, tmp_path):
        (tmp_path / 'res-1.json').write_text('{}')
        result = sut.process_resource('res-1', self._make_ckan(), self._make_processor(), tmp_path, skip_existing=True)
        assert result == 'skipped'

    def test_does_not_skip_when_flag_is_false(self, sut, tmp_path):
        (tmp_path / 'res-1.json').write_text('{}')
        processor = self._make_processor(True)
        result = sut.process_resource('res-1', self._make_ckan(), processor, tmp_path, skip_existing=False)
        assert result == 'successful'
        processor.process_event.assert_called_once()

    def test_returns_failed_when_metadata_missing(self, sut, tmp_path):
        ckan = MagicMock()
        ckan.resource_show.return_value = None
        result = sut.process_resource('res-1', ckan, self._make_processor(), tmp_path, False)
        assert result == 'failed'

    def test_returns_failed_on_processor_exception(self, sut, tmp_path):
        processor = MagicMock()
        processor.process_event.side_effect = RuntimeError('unexpected!')
        result = sut.process_resource('res-1', self._make_ckan(), processor, tmp_path, False)
        assert result == 'failed'

    def test_processor_receives_correct_event(self, sut, tmp_path):
        processor = self._make_processor()
        sut.process_resource('res-42', self._make_ckan(), processor, tmp_path, False)
        event_arg = processor.process_event.call_args[0][0]
        assert event_arg['resource_id'] == 'res-42'
        assert event_arg['event_type'] == 'batch-processing'


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------


class TestParseArgs:
    def test_defaults(self, sut):
        with patch('sys.argv', ['prog']):
            args = sut.parse_args()
        assert args.model == 'gpt-4.1-nano'
        assert args.ids is None
        assert args.skip_existing is False

    def test_custom_model(self, sut):
        with patch('sys.argv', ['prog', '--model', 'DeepSeek-V3.1']):
            args = sut.parse_args()
        assert args.model == 'DeepSeek-V3.1'

    def test_ids_parsed_as_list(self, sut):
        with patch('sys.argv', ['prog', '--ids', 'id-1', 'id-2', 'id-3']):
            args = sut.parse_args()
        assert args.ids == ['id-1', 'id-2', 'id-3']

    def test_skip_existing_flag(self, sut):
        with patch('sys.argv', ['prog', '--ids', 'id-1', '--skip-existing']):
            args = sut.parse_args()
        assert args.skip_existing is True


# ---------------------------------------------------------------------------
# main (integration-level)
# ---------------------------------------------------------------------------


class TestMain:
    def _run_main(self, sut, argv, ckan, processor):
        with (
            patch('sys.argv', argv),
            patch.object(sut, '_bootstrap'),
            patch.object(sut, 'build_ckan_client', return_value=ckan),
            patch.object(sut, 'build_event_processor', return_value=processor),
        ):
            sut.main()

    def test_exits_with_no_ids(self, sut):
        with (
            patch('sys.argv', ['prog']),
            patch.object(sut, '_bootstrap'),
            pytest.raises(SystemExit) as exc,
        ):
            sut.main()
        assert exc.value.code == 1

    def test_processes_all_ids(self, sut, tmp_path):
        processor = MagicMock()
        processor.process_event.return_value = (True, 'ok')
        ckan = MagicMock()
        ckan.resource_show.return_value = {
            'download_url': 'https://example.com/f.csv',
            'package_id': 'pkg',
            'name': 'f.csv',
        }

        with (
            patch('sys.argv', ['prog', '--ids', 'id-a', 'id-b']),
            patch.object(sut, '_bootstrap'),
            patch.object(sut, 'build_ckan_client', return_value=ckan),
            patch.object(sut, 'build_event_processor', return_value=processor),
            patch.object(sut, 'DEFAULT_OUTPUT_DIR', tmp_path),
        ):
            sut.main()

        assert processor.process_event.call_count == 2

    def test_summary_reflects_outcomes(self, sut, tmp_path, capsys):
        processor = MagicMock()
        processor.process_event.side_effect = [(True, 'ok'), (False, 'err')]
        ckan = MagicMock()
        ckan.resource_show.return_value = {
            'download_url': 'https://example.com/f.csv',
            'package_id': 'pkg',
            'name': 'f.csv',
        }

        with (
            patch('sys.argv', ['prog', '--ids', 'id-a', 'id-b']),
            patch.object(sut, '_bootstrap'),
            patch.object(sut, 'build_ckan_client', return_value=ckan),
            patch.object(sut, 'build_event_processor', return_value=processor),
            patch.object(sut, 'DEFAULT_OUTPUT_DIR', tmp_path),
        ):
            sut.main()

        out = capsys.readouterr().out
        assert '✅ Success: 1' in out
        assert '❌ Failed:  1' in out

    def test_calls_bootstrap_before_anything_else(self, sut, tmp_path):
        call_order = []
        processor = MagicMock()
        processor.process_event.return_value = (True, 'ok')
        ckan = MagicMock()
        ckan.resource_show.return_value = {
            'download_url': 'https://example.com/f.csv',
            'package_id': 'pkg',
            'name': 'f.csv',
        }

        with (
            patch('sys.argv', ['prog', '--ids', 'id-1']),
            patch.object(sut, '_bootstrap', side_effect=lambda: call_order.append('bootstrap')),
            patch.object(sut, 'build_ckan_client', side_effect=lambda: (call_order.append('ckan'), ckan)[1]),
            patch.object(
                sut, 'build_event_processor', side_effect=lambda *_: (call_order.append('processor'), processor)[1]
            ),
            patch.object(sut, 'DEFAULT_OUTPUT_DIR', tmp_path),
        ):
            sut.main()

        assert call_order[0] == 'bootstrap'
