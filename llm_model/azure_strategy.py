import json

from dotenv import load_dotenv
from openai import AzureOpenAI

from src.shared.utils.exception_handler import handle_exception

import os


class AzureOpenAIStrategy:
    """
    Strategy for using OpenAI models through Azure API.
    """

    def __init__(self, model_name: str, azure_endpoint: str, api_key: str):
        load_dotenv()
        # Azure-specific configuration
        self.azure_endpoint = azure_endpoint
        self.api_key = api_key
        self.model_name = model_name
        self.client = None
        self._setup_client()

    def _get_model_type(self) -> str:
        """Return the model type identifier."""
        return 'azure'

    @handle_exception()
    def _setup_client(self) -> None:
        """Initialize Azure OpenAI client."""
        self.client = AzureOpenAI(
            api_version='2024-12-01-preview', azure_endpoint=self.azure_endpoint, api_key=self.api_key
        )

    @handle_exception()
    def generate(self, prompt: str, temperature: float = 0.3, max_new_tokens: int = 200) -> tuple[str, int, int]:
        """Generate text using Azure OpenAI API."""
        if 'gpt-5' in self.model_name:
            response = self.client.chat.completions.create(
                messages=[{'role': 'user', 'content': prompt}],
                max_completion_tokens=512,
                reasoning_effort='minimal',
                model=self.model_name,
            )
        else:
            response = self.client.chat.completions.create(
                messages=[{'role': 'user', 'content': prompt}],
                max_completion_tokens=max_new_tokens,
                model=self.model_name,
                temperature=temperature,
            )
        return response.choices[0].message.content, response.usage.completion_tokens, response.usage.prompt_tokens

    @handle_exception()
    def generate_json(self, prompt: str, temperature: float = 0.3, max_new_tokens: int = 300) -> tuple[dict, int, int]:
        """Generate JSON using Azure OpenAI API."""
        if self.model_name == 'gpt-5-nano' or self.model_name == 'gpt-5-mini':
            temperature = 1.0
            json_response = self.client.chat.completions.create(
                messages=[{'role': 'user', 'content': prompt}],
                max_completion_tokens=1000,
                reasoning_effort='minimal',
                model=self.model_name,
                response_format={'type': 'json_object'},
            )
        else:
            json_response = self.client.chat.completions.create(
                messages=[{'role': 'user', 'content': prompt}],
                max_completion_tokens=max_new_tokens,
                model=self.model_name,
                temperature=temperature,
                response_format={'type': 'json_object'},
            )
        return (
            json.loads(json_response.choices[0].message.content),
            json_response.usage.completion_tokens,
            json_response.usage.prompt_tokens,
        )

    @handle_exception()
    def get_azure_config(self) -> dict[str, str]:
        """Get Azure configuration details."""
        return {
            'endpoint': self.azure_endpoint,
            'model': self.model_name,
        }


if __name__ == '__main__':
    azure_strategy = AzureOpenAIStrategy(
        model_name='gpt-5-nano',
        azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT'),
        api_key=os.getenv('AZURE_OPENAI_API_KEY'),
    )
    print(azure_strategy.generate('Hello'))
