"""
LLM Model Strategy Pattern Implementation

This module provides a strategy pattern implementation for different LLM providers:
- OpenAI (direct API)
- Azure OpenAI
- Unsloth (optimized HuggingFace models)
- CohereLabs (Aya models)
"""

from .azure_strategy import AzureOpenAIStrategy

__all__ = [
    'AzureOpenAIStrategy',
]
