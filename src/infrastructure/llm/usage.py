import sys
import os
from pathlib import Path

# Add project root to sys.path so we can import from src and config
ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from src.infrastructure.llm import LLMProviderFactory, LLMProviderType
from config.config import Config


# Initialize config and provider
config = Config()
provider = LLMProviderFactory.create(LLMProviderType.AZURE_OPENAI, config, 'gpt-5-nano')

# Test generation
print(f"Testing model: {provider.model_name}")

text, ct, pt = provider.generate('What is the capital of France?')
print(f"Text: {text}")
print(f"Tokens: {ct} completion, {pt} prompt")

data, ct, pt = provider.generate_json('Return a JSON object for France with keys "capital" and "population".')
print(f"JSON: {data}")
print(f"Tokens: {ct} completion, {pt} prompt")