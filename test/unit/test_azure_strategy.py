import pytest
from unittest.mock import patch, MagicMock
from llm_model.azure_strategy import AzureOpenAIStrategy


@pytest.fixture
def azure_strategy(mock_azure_client):
    with patch('llm_model.azure_strategy.AzureOpenAI', return_value=mock_azure_client):
        return AzureOpenAIStrategy(
            model_name='mock-model',
            azure_endpoint='https://mock-endpoint.com',
            api_key='mock-key',
        )


def test_init_sets_attributes(azure_strategy):
    assert azure_strategy.model_name == 'mock-model'
    assert azure_strategy.azure_endpoint == 'https://mock-endpoint.com'
    assert azure_strategy.api_key == 'mock-key'
    assert azure_strategy.client is not None


def test_get_model_type(azure_strategy):
    assert azure_strategy._get_model_type() == 'azure'


def test_generate_success(azure_strategy):
    prompt = 'Hello'
    text, comp_tokens, prompt_tokens = azure_strategy.generate(prompt)
    assert text == 'mock text'
    assert comp_tokens == 5
    assert prompt_tokens == 10


def test_generate_json_success(azure_strategy):
    # Mock JSON response
    azure_strategy.client.chat.completions.create.return_value = MagicMock(
        choices=[MagicMock(message=MagicMock(content='{"key": "value"}'))],
        usage=MagicMock(completion_tokens=2, prompt_tokens=3),
    )

    result, comp_tokens, prompt_tokens = azure_strategy.generate_json('Prompt')
    assert result == {'key': 'value'}
    assert comp_tokens == 2
    assert prompt_tokens == 3


def test_get_azure_config(azure_strategy):
    config = azure_strategy.get_azure_config()
    assert config == {'endpoint': 'https://mock-endpoint.com', 'model': 'mock-model'}
