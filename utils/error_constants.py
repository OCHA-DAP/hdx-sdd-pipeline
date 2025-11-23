"""
Error source constants for consistent error tracking across the pipeline.
These constants identify where errors occur in the processing pipeline.
"""

# Error sources
ERROR_SOURCE_DATA_SAMPLING = 'data_sampling'
ERROR_SOURCE_AZURE_GENERATION = 'azure_generation'
ERROR_SOURCE_AZURE_JSON_GENERATION = 'azure_json_generation'
ERROR_SOURCE_PROMPT_RENDERING = 'prompt_rendering'
ERROR_SOURCE_PII_CLASSIFICATION = 'pii_classification'
ERROR_SOURCE_PII_REFLECTION = 'pii_reflection'
ERROR_SOURCE_NON_PII_CLASSIFICATION = 'non_pii_classification'
ERROR_SOURCE_README_SCAN = 'readme_scan'
ERROR_SOURCE_CKAN_OPERATION = 'ckan_operation'
ERROR_SOURCE_PROCESSING = 'processing'
ERROR_SOURCE_UNKNOWN = 'unknown'
