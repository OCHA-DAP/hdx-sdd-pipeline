"""Unit tests for AzureOpenAIProvider."""

import pytest
from unittest.mock import Mock, patch
from openai import AzureOpenAI

from src.infrastructure.llm.azure_openai_provider import AzureOpenAIProvider
from src.domain.exceptions import LLMProviderError


class TestAzureOpenAIProvider:
    """Test suite for AzureOpenAIProvider."""
    
    @pytest.fixture
    def mock_client(self):
        """Create mock Azure OpenAI client."""
        client = Mock(spec=AzureOpenAI)
        # Configure nested mock for chat.completions.create
        client.chat = Mock()
        client.chat.completions = Mock()
        client.chat.completions.create = Mock()
        return client
    
    @pytest.fixture
    def provider(self, mock_client):
        """Create provider with mocked client."""
        with patch('src.infrastructure.llm.azure_openai_provider.AzureOpenAI', return_value=mock_client):
            provider = AzureOpenAIProvider(
                model_name="gpt-4.1-nano",
                azure_endpoint="https://test.openai.azure.com/",
                api_key="test-key"
            )
            return provider
    
    def test_initialization(self):
        """Test provider initialization."""
        with patch('src.infrastructure.llm.azure_openai_provider.AzureOpenAI') as mock_azure:
            provider = AzureOpenAIProvider(
                model_name="gpt-4.1-nano",
                azure_endpoint="https://test.openai.azure.com/",
                api_key="test-key"
            )
            
            assert provider.model_name == "gpt-4.1-nano"
            assert provider.azure_endpoint == "https://test.openai.azure.com/"
            mock_azure.assert_called_once()
    
    def test_initialization_with_api_version(self):
        """Test initialization with custom API version."""
        with patch('src.infrastructure.llm.azure_openai_provider.AzureOpenAI') as mock_azure:
            provider = AzureOpenAIProvider(
                model_name="gpt-4.1-nano",
                azure_endpoint="https://test.openai.azure.com/",
                api_key="test-key",
                api_version="2024-02-01"
            )
            
            # Verify API version was passed
            call_kwargs = mock_azure.call_args[1]
            assert call_kwargs['api_version'] == "2024-02-01"
    
    def test_generate_success(self, provider, mock_client):
        """Test successful text generation."""
        # Mock response
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content="Generated text"))]
        mock_response.usage = Mock(completion_tokens=10, prompt_tokens=20)
        
        mock_client.chat.completions.create.return_value = mock_response
        
        # Call generate
        result, comp_tokens, prompt_tokens = provider.generate(
            prompt="Test prompt",
            max_tokens=100
        )
        
        assert result == "Generated text"
        assert comp_tokens == 10
        assert prompt_tokens == 20
        
        # Verify API call
        mock_client.chat.completions.create.assert_called_once()
        call_kwargs = mock_client.chat.completions.create.call_args[1]
        assert call_kwargs['model'] == "gpt-4.1-nano"
        assert call_kwargs['max_tokens'] == 100
        assert len(call_kwargs['messages']) == 1
        assert call_kwargs['messages'][0]['content'] == "Test prompt"
    
    def test_generate_with_temperature(self, provider, mock_client):
        """Test generation with custom temperature."""
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content="Response"))]
        mock_response.usage = Mock(completion_tokens=5, prompt_tokens=10)
        
        mock_client.chat.completions.create.return_value = mock_response
        
        provider.generate(prompt="Test", temperature=0.5)
        
        call_kwargs = mock_client.chat.completions.create.call_args[1]
        assert call_kwargs['temperature'] == 0.5
    
    def test_generate_json_success(self, provider, mock_client):
        """Test successful JSON generation."""
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content='{"key": "value"}'))]
        mock_response.usage = Mock(completion_tokens=15, prompt_tokens=25)
        
        mock_client.chat.completions.create.return_value = mock_response
        
        result, comp_tokens, prompt_tokens = provider.generate_json(
            prompt="Generate JSON",
            max_tokens=200
        )
        
        assert result == {"key": "value"}
        assert comp_tokens == 15
        assert prompt_tokens == 25
        
        # Verify response_format was set
        call_kwargs = mock_client.chat.completions.create.call_args[1]
        assert call_kwargs['response_format'] == {"type": "json_object"}
    
    def test_generate_json_invalid_json(self, provider, mock_client):
        """Test handling of invalid JSON response."""
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content='Not valid JSON'))]
        mock_response.usage = Mock(completion_tokens=10, prompt_tokens=20)
        
        mock_client.chat.completions.create.return_value = mock_response
        
        with pytest.raises(LLMProviderError, match="Invalid JSON"):
            provider.generate_json(prompt="Generate JSON")
    
    def test_generate_json_empty_response(self, provider, mock_client):
        """Test handling of empty JSON response."""
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content=''))]
        mock_response.usage = Mock(completion_tokens=5, prompt_tokens=10)
        
        mock_client.chat.completions.create.return_value = mock_response
        
        with pytest.raises(LLMProviderError, match="Invalid JSON"):
            provider.generate_json(prompt="Generate JSON")
    
    def test_generate_with_system_message(self, provider, mock_client):
        """Test generation with system message."""
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content="Response"))]
        mock_response.usage = Mock(completion_tokens=10, prompt_tokens=20)
        
        mock_client.chat.completions.create.return_value = mock_response
        
        # Note: Current implementation doesn't support system_message parameter
        # This test documents expected behavior for future enhancement
        provider.generate(prompt="User message")
        
        call_kwargs = mock_client.chat.completions.create.call_args[1]
        messages = call_kwargs['messages']
        
        # Currently only user message
        assert len(messages) == 1
        assert messages[0]['role'] == 'user'
        assert messages[0]['content'] == "User message"
    
    def test_generate_default_parameters(self, provider, mock_client):
        """Test that default parameters are used."""
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content="Response"))]
        mock_response.usage = Mock(completion_tokens=10, prompt_tokens=20)
        
        mock_client.chat.completions.create.return_value = mock_response
        
        provider.generate(prompt="Test")
        
        call_kwargs = mock_client.chat.completions.create.call_args[1]
        assert call_kwargs['temperature'] == 0.0  # Default
        assert call_kwargs['max_tokens'] == 256  # Default
    
    def test_generate_json_with_system_message(self, provider, mock_client):
        """Test JSON generation (system message not currently supported)."""
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content='{"result": "success"}'))]
        mock_response.usage = Mock(completion_tokens=10, prompt_tokens=20)
        
        mock_client.chat.completions.create.return_value = mock_response
        
        result, _, _ = provider.generate_json(prompt="Generate")
        
        assert result == {"result": "success"}
        
        call_kwargs = mock_client.chat.completions.create.call_args[1]
        messages = call_kwargs['messages']
        assert messages[0]['role'] == 'user'
    
    def test_multiple_generations(self, provider, mock_client):
        """Test multiple sequential generations."""
        mock_response1 = Mock()
        mock_response1.choices = [Mock(message=Mock(content="First"))]
        mock_response1.usage = Mock(completion_tokens=5, prompt_tokens=10)
        
        mock_response2 = Mock()
        mock_response2.choices = [Mock(message=Mock(content="Second"))]
        mock_response2.usage = Mock(completion_tokens=6, prompt_tokens=11)
        
        mock_client.chat.completions.create.side_effect = [mock_response1, mock_response2]
        
        result1, _, _ = provider.generate(prompt="First prompt")
        result2, _, _ = provider.generate(prompt="Second prompt")
        
        assert result1 == "First"
        assert result2 == "Second"
        assert mock_client.chat.completions.create.call_count == 2
    
    def test_generate_preserves_whitespace(self, provider, mock_client):
        """Test that whitespace in response is preserved."""
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content="  Response with spaces  "))]
        mock_response.usage = Mock(completion_tokens=10, prompt_tokens=20)
        
        mock_client.chat.completions.create.return_value = mock_response
        
        result, _, _ = provider.generate(prompt="Test")
        
        # Should preserve leading/trailing whitespace
        assert result == "  Response with spaces  "
    
    def test_generate_json_nested_structure(self, provider, mock_client):
        """Test JSON generation with nested structure."""
        json_response = '{"outer": {"inner": {"value": 123}}, "array": [1, 2, 3]}'
        
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content=json_response))]
        mock_response.usage = Mock(completion_tokens=20, prompt_tokens=30)
        
        mock_client.chat.completions.create.return_value = mock_response
        
        result, _, _ = provider.generate_json(prompt="Generate nested JSON")
        
        assert result == {
            "outer": {"inner": {"value": 123}},
            "array": [1, 2, 3]
        }
