import os
import json

from dotenv import load_dotenv
from openai import AzureOpenAI


class AzureOpenAIStrategy:
    """
    Strategy for using OpenAI models through Azure API.
    """

    def __init__(self, model_name: str):
        load_dotenv()
        # Azure-specific configuration
        self.azure_endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')
        self.api_key = os.getenv('AZURE_OPENAI_API_KEY')
        self.client = None
        self.model = model_name
        self._setup_client()

    def _get_model_type(self) -> str:
        """Return the model type identifier."""
        return 'azure'

    def _setup_client(self) -> None:
        """Initialize Azure OpenAI client."""
        try:
            if not self.azure_endpoint or not self.api_key:
                raise ValueError('Azure OpenAI endpoint and API key must be provided')

            self.client = AzureOpenAI(
                api_version='2024-12-01-preview', azure_endpoint=self.azure_endpoint, api_key=self.api_key
            )

        except Exception as e:
            raise Exception(f'Error initializing Azure OpenAI client: {e}')

    def generate(self, prompt: str, temperature: float = 0.3, max_new_tokens: int = 200) -> tuple[str, int, int]:
        """Generate text using Azure OpenAI API."""
        try:
            if not self.client:
                raise ValueError('Azure OpenAI client not initialized')

            response = self.client.chat.completions.create(
                messages=[{'role': 'user', 'content': prompt}],
                max_completion_tokens=max_new_tokens,
                model=self.model,
                temperature=temperature,
            )

            return response.choices[0].message.content, response.usage.completion_tokens, response.usage.prompt_tokens

        except Exception as e:
            print(f'Error generating text with Azure OpenAI: {e}')
            return 'ERROR_GENERATION', 0, 0

    def generate_json(self, prompt: str, temperature: float = 0.3, max_new_tokens: int = 200) -> tuple[dict, int, int]:
        """Generate JSON using Azure OpenAI API."""
        try:
            if not self.client:
                raise ValueError('Azure OpenAI client not initialized')
            json_response = self.client.chat.completions.create(
                messages=[{'role': 'user', 'content': prompt}],
                max_completion_tokens=max_new_tokens,
                model=self.model,
                temperature=temperature,
                response_format={'type': 'json_object'},
            )
            return (
                json.loads(json_response.choices[0].message.content),
                json_response.usage.completion_tokens,
                json_response.usage.prompt_tokens,
            )
        except Exception as e:
            print(f'Error generating JSON with Azure OpenAI: {e}')
            return {'error': str(e)}, 0, 0

    def get_azure_config(self) -> dict[str, str]:
        """Get Azure configuration details."""
        return {
            'endpoint': self.azure_endpoint,
            'model': self.model,
        }


if __name__ == '__main__':
    model = AzureOpenAIStrategy(model_name='gpt-4o-mini')
    response = model.generate('What is the capital of France?')
    test = 'test'
    HALLO = 'hallo'
    print(response)
