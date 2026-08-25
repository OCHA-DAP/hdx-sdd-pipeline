"""Unit tests for OpenAIProvider with full branch coverage."""

from unittest.mock import Mock, patch

import pytest
from openai import OpenAI

from src.domain.exceptions import LLMProviderError
from src.infrastructure.openai_provider import OpenAIProvider


class TestOpenAIProvider:
    """Test suite for OpenAIProvider."""

    # ------------------------------------------------------------------
    # Fixtures
    # ------------------------------------------------------------------

    @pytest.fixture
    def mock_client(self):
        """Create mock OpenAI client."""
        client = Mock(spec=OpenAI)
        client.chat = Mock()
        client.chat.completions = Mock()
        client.chat.completions.with_raw_response = Mock()
        return client

    @pytest.fixture
    def provider(self, mock_client):
        """Create provider with mocked client."""
        with patch(
            'src.infrastructure.openai_provider.OpenAI',
            return_value=mock_client,
        ):
            return OpenAIProvider(
                model_name='gpt-4',
                endpoint='https://api.deepseek.com/v1',
                api_key='test-key',
            )

    # ------------------------------------------------------------------
    # Initialization Tests
    # ------------------------------------------------------------------

    def test_initialization(self):
        """Test provider initialization."""
        with patch('src.infrastructure.openai_provider.OpenAI') as mock_openai:
            provider = OpenAIProvider(
                model_name='gpt-4',
                endpoint='https://api.deepseek.com/v1',
                api_key='test-key',
            )

            assert provider.model_name == 'gpt-4'

            mock_openai.assert_called_once_with(
                base_url='https://api.deepseek.com/v1',
                api_key='test-key',
            )

    def test_initialization_without_endpoint(self):
        """Test provider initialization without base_url."""
        with patch('src.infrastructure.openai_provider.OpenAI') as mock_openai:
            OpenAIProvider(
                model_name='gpt-4',
                api_key='test-key',
            )

            mock_openai.assert_called_once_with(
                base_url=None,
                api_key='test-key',
            )

    def test_initialization_failure(self):
        """Test initialization failure handles errors."""
        with patch(
            'src.infrastructure.openai_provider.OpenAI',
            side_effect=Exception('Connection error'),
        ):
            with pytest.raises(
                LLMProviderError,
                match='OpenAI initialization failed',
            ):
                OpenAIProvider(
                    model_name='gpt-4',
                    endpoint='https://api.deepseek.com/v1',
                    api_key='test-key',
                )

    # ------------------------------------------------------------------
    # Helper Tests
    # ------------------------------------------------------------------

    def test_model_name_property(self, provider):
        """Test model_name property."""
        assert provider.model_name == 'gpt-4'

    def test_is_reasoning_model_true(self, provider):
        """Test reasoning model detection."""
        provider._model = 'gpt-5-mini'
        assert provider._is_reasoning_model() is True

    def test_is_reasoning_model_false(self, provider):
        """Test non-reasoning model detection."""
        provider._model = 'gpt-4.1-mini'
        assert provider._is_reasoning_model() is False

    def test_token_counts_with_usage(self):
        """Test token count extraction with usage data."""
        completion = Mock()
        completion.usage = Mock(
            completion_tokens=11,
            prompt_tokens=22,
        )

        completion_tokens, prompt_tokens = OpenAIProvider._token_counts(completion)

        assert completion_tokens == 11
        assert prompt_tokens == 22

    def test_token_counts_without_usage(self):
        """Test token count extraction without usage data."""
        completion = Mock()
        completion.usage = None

        completion_tokens, prompt_tokens = OpenAIProvider._token_counts(completion)

        assert completion_tokens == 0
        assert prompt_tokens == 0

    def test_token_counts_without_usage_attribute(self):
        """Test token count extraction without usage attribute."""

        class Completion:
            pass

        completion_tokens, prompt_tokens = OpenAIProvider._token_counts(Completion())

        assert completion_tokens == 0
        assert prompt_tokens == 0

    # ------------------------------------------------------------------
    # _call Tests
    # ------------------------------------------------------------------

    def test_call_standard_model(self, provider, mock_client):
        """Test _call for standard models."""
        parsed_response = Mock()

        raw_response = Mock()
        raw_response.parse.return_value = parsed_response

        mock_client.chat.completions.with_raw_response.create.return_value = raw_response

        result = provider._call(
            messages=[{'role': 'user', 'content': 'hello'}],
            temperature=0.7,
            max_tokens=100,
        )

        assert result == parsed_response

        mock_client.chat.completions.with_raw_response.create.assert_called_once_with(
            model='gpt-4',
            messages=[{'role': 'user', 'content': 'hello'}],
            temperature=0.7,
            max_tokens=100,
            seed=42,
        )

    def test_call_standard_model_with_response_format(self, provider, mock_client):
        """Test _call with response_format."""
        parsed_response = Mock()

        raw_response = Mock()
        raw_response.parse.return_value = parsed_response

        mock_client.chat.completions.with_raw_response.create.return_value = raw_response

        provider._call(
            messages=[{'role': 'user', 'content': 'hello'}],
            temperature=0.7,
            max_tokens=100,
            response_format={'type': 'json_object'},
        )

        call_kwargs = mock_client.chat.completions.with_raw_response.create.call_args[1]

        assert call_kwargs['response_format'] == {'type': 'json_object'}

    def test_call_standard_model_with_extra_kwargs(self, provider, mock_client):
        """Test _call forwards kwargs."""
        parsed_response = Mock()

        raw_response = Mock()
        raw_response.parse.return_value = parsed_response

        mock_client.chat.completions.with_raw_response.create.return_value = raw_response

        provider._call(
            messages=[{'role': 'user', 'content': 'hello'}],
            temperature=0.7,
            max_tokens=100,
            top_p=0.5,
        )

        call_kwargs = mock_client.chat.completions.with_raw_response.create.call_args[1]

        assert call_kwargs['top_p'] == 0.5

    def test_call_reasoning_model(self, mock_client):
        """Test _call for reasoning models."""
        with patch(
            'src.infrastructure.openai_provider.OpenAI',
            return_value=mock_client,
        ):
            provider = OpenAIProvider(
                model_name='gpt-5-reasoning',
                endpoint='https://api.openai.com/v1',
                api_key='test-key',
            )

            parsed_response = Mock()

            raw_response = Mock()
            raw_response.parse.return_value = parsed_response

            mock_client.chat.completions.with_raw_response.create.return_value = raw_response

            result = provider._call(
                messages=[{'role': 'user', 'content': 'hello'}],
                temperature=0.7,
                max_tokens=100,
            )

            assert result == parsed_response

            call_kwargs = mock_client.chat.completions.with_raw_response.create.call_args[1]

            assert call_kwargs['model'] == 'gpt-5-reasoning'
            assert call_kwargs['max_completion_tokens'] == 8292
            assert call_kwargs['reasoning_effort'] == 'minimal'
            assert call_kwargs['seed'] == 42
            assert 'temperature' not in call_kwargs
            assert 'top_p' not in call_kwargs

    def test_call_reasoning_model_custom_effort(self, mock_client):
        """Test _call for reasoning models with custom effort."""
        with patch(
            'src.infrastructure.openai_provider.OpenAI',
            return_value=mock_client,
        ):
            provider = OpenAIProvider(
                model_name='gpt-5-reasoning',
                endpoint='https://api.openai.com/v1',
                api_key='test-key',
            )

            parsed_response = Mock()
            raw_response = Mock()
            raw_response.parse.return_value = parsed_response
            mock_client.chat.completions.with_raw_response.create.return_value = raw_response

            provider._call(
                messages=[{'role': 'user', 'content': 'hello'}],
                temperature=0.7,
                max_tokens=100,
                reasoning_effort='medium',
                top_p=0.9,
            )

            call_kwargs = mock_client.chat.completions.with_raw_response.create.call_args[1]
            assert call_kwargs['reasoning_effort'] == 'medium'
            assert 'temperature' not in call_kwargs
            assert 'top_p' not in call_kwargs

    def test_call_reasoning_model_effort_none(self, mock_client):
        """Test _call for reasoning models with reasoning_effort='none' retains temperature."""
        with patch(
            'src.infrastructure.openai_provider.OpenAI',
            return_value=mock_client,
        ):
            provider = OpenAIProvider(
                model_name='gpt-5-reasoning',
                endpoint='https://api.openai.com/v1',
                api_key='test-key',
            )

            parsed_response = Mock()
            raw_response = Mock()
            raw_response.parse.return_value = parsed_response
            mock_client.chat.completions.with_raw_response.create.return_value = raw_response

            provider._call(
                messages=[{'role': 'user', 'content': 'hello'}],
                temperature=0.4,
                max_tokens=100,
                reasoning_effort='none',
                top_p=0.9,
            )

            call_kwargs = mock_client.chat.completions.with_raw_response.create.call_args[1]
            assert 'reasoning_effort' not in call_kwargs
            assert call_kwargs['temperature'] == 0.4
            assert call_kwargs['top_p'] == 0.9
            assert call_kwargs['max_tokens'] == 100
            assert 'max_completion_tokens' not in call_kwargs

    def test_call_reasoning_model_with_response_format(self, mock_client):
        """Test reasoning model token adjustment with response_format."""
        with patch(
            'src.infrastructure.openai_provider.OpenAI',
            return_value=mock_client,
        ):
            provider = OpenAIProvider(
                model_name='gpt-5-reasoning',
                endpoint='https://api.openai.com/v1',
                api_key='test-key',
            )

            parsed_response = Mock()

            raw_response = Mock()
            raw_response.parse.return_value = parsed_response

            mock_client.chat.completions.with_raw_response.create.return_value = raw_response

            provider._call(
                messages=[{'role': 'user', 'content': 'hello'}],
                temperature=0.7,
                max_tokens=100,
                response_format={'type': 'json_object'},
            )

            call_kwargs = mock_client.chat.completions.with_raw_response.create.call_args[1]

            assert call_kwargs['max_completion_tokens'] == 8292
            assert call_kwargs['response_format'] == {'type': 'json_object'}

    def test_call_error_with_response(self, provider, mock_client):
        """Test _call handles exceptions with response attribute."""
        mock_exception = Exception('API error')
        mock_exception.response = Mock(
            status_code=500,
            text='Internal server error',
            headers={'x-request-id': '123'},
        )

        mock_client.chat.completions.with_raw_response.create.side_effect = mock_exception

        with pytest.raises(Exception, match='API error'):
            provider._call(
                messages=[{'role': 'user', 'content': 'hello'}],
                temperature=0.7,
                max_tokens=100,
            )

    def test_call_error_without_response(self, provider, mock_client):
        """Test _call handles exceptions without response attribute."""
        mock_client.chat.completions.with_raw_response.create.side_effect = Exception('Plain error')

        with pytest.raises(Exception, match='Plain error'):
            provider._call(
                messages=[{'role': 'user', 'content': 'hello'}],
                temperature=0.7,
                max_tokens=100,
            )

    # ------------------------------------------------------------------
    # generate Tests
    # ------------------------------------------------------------------

    def test_generate_success(self, provider, mock_client):
        """Test successful text generation."""
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content='Generated text'))]
        mock_response.usage = Mock(
            completion_tokens=10,
            prompt_tokens=20,
        )

        mock_raw_response = Mock()
        mock_raw_response.parse.return_value = mock_response

        mock_client.chat.completions.with_raw_response.create.return_value = mock_raw_response

        result, comp_tokens, prompt_tokens = provider.generate(
            prompt='Test prompt',
            max_tokens=100,
        )

        assert result == 'Generated text'
        assert comp_tokens == 10
        assert prompt_tokens == 20

    def test_generate_with_system_prompt(self, provider, mock_client):
        """Test generate includes system prompt."""
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content='Generated text'))]
        mock_response.usage = Mock(
            completion_tokens=10,
            prompt_tokens=20,
        )

        mock_raw_response = Mock()
        mock_raw_response.parse.return_value = mock_response

        mock_client.chat.completions.with_raw_response.create.return_value = mock_raw_response

        provider.generate(
            prompt='Test prompt',
            system='You are helpful',
        )

        call_kwargs = mock_client.chat.completions.with_raw_response.create.call_args[1]

        assert call_kwargs['messages'] == [
            {'role': 'system', 'content': 'You are helpful'},
            {'role': 'user', 'content': 'Test prompt'},
        ]

    def test_generate_api_error(self, provider, mock_client):
        """Test generate propagates API errors."""
        mock_client.chat.completions.with_raw_response.create.side_effect = Exception('API Connection Error')

        with pytest.raises(
            LLMProviderError,
            match='LLM generation failed',
        ):
            provider.generate(prompt='Test prompt')

    def test_generate_no_choices_attribute(self, provider):
        """Test generate handles missing choices attribute."""

        completion = object()

        with patch.object(provider, '_call', return_value=completion):
            with pytest.raises(
                LLMProviderError,
                match='LLM generation returned no choices',
            ):
                provider.generate(prompt='Test prompt')

    def test_generate_empty_choices(self, provider):
        """Test generate handles empty choices."""
        completion = Mock()
        completion.choices = []

        with patch.object(provider, '_call', return_value=completion):
            with pytest.raises(
                LLMProviderError,
                match='LLM generation returned no choices',
            ):
                provider.generate(prompt='Test prompt')

    def test_generate_choice_without_message_attribute(self, provider):
        """Test generate handles missing message attribute."""
        completion = Mock()

        class Choice:
            pass

        completion.choices = [Choice()]

        with patch.object(provider, '_call', return_value=completion):
            with pytest.raises(
                LLMProviderError,
                match='LLM generation choice has no message',
            ):
                provider.generate(prompt='Test prompt')

    def test_generate_choice_with_none_message(self, provider):
        """Test generate handles None message."""
        completion = Mock()
        completion.choices = [Mock(message=None)]

        with patch.object(provider, '_call', return_value=completion):
            with pytest.raises(
                LLMProviderError,
                match='LLM generation choice has no message',
            ):
                provider.generate(prompt='Test prompt')

    def test_generate_none_content(self, provider):
        """Test generate fails when content is None."""
        completion = Mock()
        completion.choices = [Mock(message=Mock(content=None))]

        with patch.object(provider, '_call', return_value=completion):
            with pytest.raises(
                LLMProviderError,
                match='LLM generation message content is None',
            ):
                provider.generate(prompt='Test prompt')

    # ------------------------------------------------------------------
    # generate_json Tests
    # ------------------------------------------------------------------

    def test_generate_json_success(self, provider, mock_client):
        """Test successful JSON generation."""
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content='{"key": "value"}'))]
        mock_response.usage = Mock(
            completion_tokens=15,
            prompt_tokens=25,
        )

        mock_raw_response = Mock()
        mock_raw_response.parse.return_value = mock_response

        mock_client.chat.completions.with_raw_response.create.return_value = mock_raw_response

        result, comp_tokens, prompt_tokens = provider.generate_json(prompt='Generate JSON')

        assert result == {'key': 'value'}
        assert comp_tokens == 15
        assert prompt_tokens == 25

    def test_generate_json_with_system_prompt(self, provider, mock_client):
        """Test generate_json includes system prompt."""
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content='{"key": "value"}'))]
        mock_response.usage = Mock(
            completion_tokens=15,
            prompt_tokens=25,
        )

        mock_raw_response = Mock()
        mock_raw_response.parse.return_value = mock_response

        mock_client.chat.completions.with_raw_response.create.return_value = mock_raw_response

        provider.generate_json(
            prompt='Generate JSON',
            system='Return valid JSON',
        )

        call_kwargs = mock_client.chat.completions.with_raw_response.create.call_args[1]

        assert call_kwargs['messages'] == [
            {'role': 'system', 'content': 'Return valid JSON'},
            {'role': 'user', 'content': 'Generate JSON'},
        ]

    def test_generate_json_markdown_wrapped(self, provider):
        """Test JSON wrapped in markdown code fences."""
        completion = Mock()
        completion.choices = [Mock(message=Mock(content='```json\n{"key": "value"}\n```'))]
        completion.usage = Mock(
            completion_tokens=1,
            prompt_tokens=1,
        )

        with patch.object(provider, '_call', return_value=completion):
            result, _, _ = provider.generate_json(prompt='Generate JSON')

        assert result == {'key': 'value'}

    def test_generate_json_api_error(self, provider, mock_client):
        """Test generate_json propagates API errors."""
        mock_client.chat.completions.with_raw_response.create.side_effect = Exception('API Connection Error')

        with pytest.raises(
            LLMProviderError,
            match='LLM JSON generation failed',
        ):
            provider.generate_json(prompt='Generate JSON')

    def test_generate_json_no_choices_attribute(self, provider):
        """Test generate_json handles missing choices attribute."""
        completion = object()

        with patch.object(provider, '_call', return_value=completion):
            with pytest.raises(
                LLMProviderError,
                match='LLM JSON generation returned no choices',
            ):
                provider.generate_json(prompt='Generate JSON')

    def test_generate_json_empty_choices(self, provider):
        """Test generate_json handles empty choices."""
        completion = Mock()
        completion.choices = []

        with patch.object(provider, '_call', return_value=completion):
            with pytest.raises(
                LLMProviderError,
                match='LLM JSON generation returned no choices',
            ):
                provider.generate_json(prompt='Generate JSON')

    def test_generate_json_choice_without_message_attribute(self, provider):
        """Test generate_json handles missing message attribute."""
        completion = Mock()

        class Choice:
            pass

        completion.choices = [Choice()]

        with patch.object(provider, '_call', return_value=completion):
            with pytest.raises(
                LLMProviderError,
                match='LLM JSON generation choice has no message',
            ):
                provider.generate_json(prompt='Generate JSON')

    def test_generate_json_choice_with_none_message(self, provider):
        """Test generate_json handles None message."""
        completion = Mock()
        completion.choices = [Mock(message=None)]

        with patch.object(provider, '_call', return_value=completion):
            with pytest.raises(
                LLMProviderError,
                match='LLM JSON generation choice has no message',
            ):
                provider.generate_json(prompt='Generate JSON')

    def test_generate_json_none_content(self, provider):
        """Test generate_json handles None content."""
        completion = Mock()
        completion.choices = [Mock(message=Mock(content=None))]

        with patch.object(provider, '_call', return_value=completion):
            with pytest.raises(
                LLMProviderError,
                match='LLM JSON generation message content is None',
            ):
                provider.generate_json(prompt='Generate JSON')

    def test_generate_json_invalid(self, provider):
        """Test generate_json fails on invalid JSON response."""
        completion = Mock()
        completion.choices = [Mock(message=Mock(content='not a json'))]

        with patch.object(provider, '_call', return_value=completion):
            with pytest.raises(
                LLMProviderError,
                match='Invalid JSON response',
            ):
                provider.generate_json(prompt='Generate JSON')

    def test_generate_json_non_object(self, provider):
        """Test generate_json rejects non-object JSON."""
        completion = Mock()
        completion.choices = [Mock(message=Mock(content='["a", "b"]'))]

        with patch.object(provider, '_call', return_value=completion):
            with pytest.raises(
                LLMProviderError,
                match='Expected JSON object',
            ):
                provider.generate_json(prompt='Generate JSON')
