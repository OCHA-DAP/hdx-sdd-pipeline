import os
from typing import Optional, Dict
from openai import AzureOpenAI
from dotenv import load_dotenv


class AzureOpenAIStrategy:
    """
    Strategy for using OpenAI models through Azure API.
    """

    def __init__(self, model_name: str, device: Optional[str] = None, **kwargs):
        load_dotenv()
        print(' ===== STARTING AZURE OPENAI STRATEGY ===== ')
        # Azure-specific configuration
        self.azure_endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')
        self.api_key = os.getenv('AZURE_OPENAI_API_KEY')
        self.client = None
        self.model = model_name
        print(f'[AZURE_OPENAI_STRATEGY] DEBUG Model: {self.model}')
        self._setup_client()
        print(' ===== AZURE OPENAI STRATEGY SETUP COMPLETED ===== ')

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

    def generate(self, prompt: str, temperature: float = 0.3, max_new_tokens: int = 200, **kwargs) -> str:
        """Generate text using Azure OpenAI API."""
        try:
            response = self.client.chat.completions.create(
                messages=[{'role': 'user', 'content': prompt}],
                max_completion_tokens=max_new_tokens,
                model=self.model,
            )

            return response.choices[0].message.content, response.usage.completion_tokens, response.usage.prompt_tokens

        except Exception as e:
            print(f'Error generating text with Azure OpenAI: {e}')

    def get_azure_config(self) -> Dict[str, str]:
        """Get Azure configuration details."""
        return {
            'endpoint': self.azure_endpoint,
            'model': self.model,
        }


if __name__ == '__main__':
    model = AzureOpenAIStrategy(model_name='gpt-4o-mini')
    response = model.generate('What is the capital of France?')
    print(response)
    print(' ===== AZURE OPENAI STRATEGY TEST COMPLETED ===== ')
