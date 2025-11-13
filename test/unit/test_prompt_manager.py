from utils.prompt_manager import PromptManager
import pandas as pd
import pytest

prompt_manager = PromptManager()


def test_init_prompt_manager():
    prompt_manager = PromptManager()
    assert prompt_manager is not None


def test_list_versions():
    versions = prompt_manager.list_versions('pii_detection')
    assert versions is not None
    assert len(versions) > 0


def test_not_existing_prompt():
    with pytest.raises(FileNotFoundError):
        prompt_manager.list_versions('not_existing_prompt')


def test_get_prompt():
    prompt = prompt_manager.get_prompt('pii_detection', 'v0', {'column_name': 'name', 'sample_values': ['John Doe']})
    assert prompt is not None
    assert len(prompt) > 0


def test_get_prompt_not_existing_version():
    with pytest.raises(FileNotFoundError):
        prompt_manager.get_prompt(
            'pii_detection', 'not_existing_version', {'column_name': 'name', 'sample_values': ['John Doe']}
        )
