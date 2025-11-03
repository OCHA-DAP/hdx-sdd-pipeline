"""utils/main_config.py: Main configuration file for the HDX SSD Pipeline."""

import os

# Get the folder of the project
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# HDX API Configuration
HDX_API_BASE_URL = 'https://data.humdata.org/api/action'

# Data directories
INPUT_DIR = os.path.join(PROJECT_ROOT, 'data', 'input')
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'data', 'output')

# Redis stream configuration
INPUT_STREAM = 'sdd:tables'
OUTPUT_STREAM = 'sdd:results'

NON_PII_DETECT_MODEL = 'gpt-4.1-mini'
PII_DETECT_MODEL = 'gpt-4.1-mini'
PII_REFLECT_MODEL = 'gpt-4.1-mini'

DEBUG = False
RERUN = False

PII_ENTITIES_LIST = [
    'IP_ADDRESS',
    'AGE',
    'CREDIT_CARD_NUMBER',
    'BIRTH_DATE',
    'DISABILITY_GROUP',
    'EDUCATION_LEVEL',
    'EMAIL_ADDRESS',
    'ETHNIC_GROUP',
    'GENDER',
    'GEO_COORDINATES',
    'MARITAL_STATUS',
    'MEDICAL_TERM',
    'OCCUPATION',
    'ORGANIZATION_NAME',
    'PERSON_NAME',
    'PHONE_NUMBER',
    'PROTECTION_GROUP',
    'RELIGIOUS_GROUPS',
    'SEXUALITY',
    'SPOKEN_LANGUAGE',
    'STREET_ADDRESS',
    'URL',
    'IBAN_CODE',
    'PASSPORT',
    'SWIFT_CODE',
    'ZIPCODE',
]
