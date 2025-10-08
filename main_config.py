"""Main configuration file for the HDX SSD Pipeline."""
import os

# Get the folder of the project
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

NON_PII_DETECT_MODEL = 'gpt-4o-mini'
PII_DETECT_MODEL = 'gpt-4o-mini'
PII_REFLECT_MODEL = 'gpt-4o-mini'

DEBUG = False
