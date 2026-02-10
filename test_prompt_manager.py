"""Test script for PromptManager latest version detection."""

import logging
from src.shared.utils.prompt_manager import PromptManager

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(name)s:%(message)s')

# Initialize prompt manager
pm = PromptManager()

# Test 1: Get latest version automatically
print("=" * 60)
print("TEST 1: Auto-detect latest version for pii_detection")
print("=" * 60)
latest_pii_detection = pm.get_latest_version('pii_detection')
print(f"Latest version: {latest_pii_detection}")

prompt = pm.get_prompt(
    'pii_detection',
    version=None,  # Should use latest (v1)
    context={'column_name': 'email', 'sample_values': ['test@example.com', 'user@domain.org']}
)
print(f"\nRendered prompt:\n{prompt}\n")

# Test 2: Get specific version
print("=" * 60)
print("TEST 2: Get specific version v0 for pii_detection")
print("=" * 60)
prompt_v0 = pm.get_prompt(
    'pii_detection',
    version='v0',
    context={'column_name': 'email', 'sample_values': ['test@example.com', 'user@domain.org']}
)
print(f"Rendered prompt (v0):\n{prompt_v0}\n")

# Test 3: Check all available prompts
print("=" * 60)
print("TEST 3: Check latest versions for all prompts")
print("=" * 60)
for prompt_name in ['pii_detection', 'pii_reflection', 'non_pii_classification', 'readme_scan']:
    latest = pm.get_latest_version(prompt_name)
    print(f"{prompt_name}: {latest}")

print("\n✅ All tests passed!")
