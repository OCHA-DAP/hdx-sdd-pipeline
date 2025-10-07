#!/usr/bin/env python3
"""
Example usage of the LLM Model Strategy Pattern

This script demonstrates how to use the different model strategies.
"""

import os
from llm_model import (
    AzureOpenAIStrategy,
)


def example_direct_strategy_usage():
    """Example using strategies directly."""
    print("\n=== Direct Strategy Usage ===")

    # Azure OpenAI Strategy
    try:
        model = AzureOpenAIStrategy("gpt-4o-mini")
        response = model.generate("What is the capital of France?")
        print(f"Azure OpenAI Strategy Response: {response}")
    except Exception as e:
        print(f"Azure OpenAI Strategy Error: {e}")

    # Azure OpenAI Strategy (if configured)
    try:
        model = AzureOpenAIStrategy(
            "gpt-4o-mini",
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        )
        response = model.generate("What is the capital of France?")
        print(f"Azure OpenAI Response: {response}")
    except Exception as e:
        print(f"Azure OpenAI Error: {e}")


if __name__ == "__main__":
    print("LLM Model Strategy Pattern Examples")
    print("=" * 50)

    example_direct_strategy_usage()

    print("\n" + "=" * 50)
    print("Examples completed!")
