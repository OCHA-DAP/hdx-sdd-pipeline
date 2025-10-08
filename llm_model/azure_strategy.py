import os
import logging
from typing import Optional, Dict, Any
from openai import AzureOpenAI
import dotenv
from .base_model import BaseLLMModel


class AzureOpenAIStrategy(BaseLLMModel):
    """
    Strategy for using OpenAI models through Azure API.
    """

    def __init__(self, model_name: str, device: Optional[str] = None, **kwargs):
        # Azure-specific configuration
        self.azure_endpoint = kwargs.get("azure_endpoint") or os.getenv("AZURE_OPENAI_ENDPOINT")
        self.api_version = kwargs.get("api_version") or os.getenv("AZURE_OPENAI_API_VERSION")
        self.api_key = kwargs.get("api_key") or os.getenv("AZURE_OPENAI_API_KEY")

        print(self.azure_endpoint, self.api_key, self.api_version)
        if not self.azure_endpoint or not self.api_key:
            raise ValueError(
                "Missing Azure OpenAI configuration. Please provide either kwargs "
                "or set AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_API_KEY in environment variables."
            )

        super().__init__(model_name, device, **kwargs)

    def _get_model_type(self) -> str:
        """Return the model type identifier."""
        return "azure"

    def _setup_model(self, **kwargs) -> None:
        """Initialize Azure OpenAI client."""
        try:
            # Load environment variables
            dotenv.load_dotenv()

            # Get configuration from kwargs or environment
            azure_endpoint = self.azure_endpoint or os.getenv("AZURE_OPENAI_ENDPOINT")
            api_key = self.api_key or os.getenv("AZURE_OPENAI_API_KEY")
            api_version = self.api_version or os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")

            if not azure_endpoint or not api_key:
                raise ValueError("Azure OpenAI endpoint and API key must be provided")

            # Initialize Azure OpenAI client
            self.client = AzureOpenAI(azure_endpoint=azure_endpoint, api_key=api_key, api_version=api_version)

            self._setup_logging("openai")
            print(f"Azure OpenAI client initialized for model: {self.model_name}")

        except Exception as e:
            print(f"Error initializing Azure OpenAI client: {e}")
            raise

    def generate(self, prompt: str, temperature: float = 0.3, max_new_tokens: int = 8, **kwargs) -> str:
        """Generate text using Azure OpenAI API."""
        try:
            # Handle different model versions
            if self.model_name in ["o3-mini", "o4-mini", "o3"]:
                response = self.client.chat.completions.create(
                    model=self.model_name,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=temperature,
                    max_completion_tokens=max_new_tokens,
                )
            else:
                response = self.client.chat.completions.create(
                    model=self.model_name,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=temperature,
                    max_tokens=max_new_tokens,
                )

            return response.choices[0].message.content

        except Exception as e:
            print(f"Error generating text with Azure OpenAI: {e}")
            raise

    def get_azure_config(self) -> Dict[str, str]:
        """Get Azure configuration details."""
        return {
            "endpoint": self.azure_endpoint,
            "api_version": self.api_version,
            "model_name": self.model_name,
        }
