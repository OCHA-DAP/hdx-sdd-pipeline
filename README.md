# HDX SSD Pipeline

A complete event-driven pipeline that processes tabular data through three classification stages and returns results to Redis.

## Overview

The HDX SSD (Sensitive Data Detection) Pipeline processes HDX resources through a three-stage classification system:

1. **PII Detection**: Identifies personally identifiable information in table columns
2. **PII Reflection**: Determines sensitivity levels for detected PII entities
3. **Non-PII Classification**: Assesses overall table sensitivity for non-PII aspects

## Architecture

```
Redis Event → Download CSV → Preprocess → PII Detection → PII Reflection → Non-PII Classification → Redis Response
```

## Setup Instructions

### Prerequisites

- Python 3.8+
- Redis server
- Azure OpenAI API access

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd hdx-ssd-pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

### Environment Variables

Create a `.env` file with the following variables:

```bash
# Azure OpenAI Configuration
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_KEY=your-api-key-here
AZURE_OPENAI_API_VERSION=2024-02-15-preview

# Redis Configuration
REDIS_STREAM_PORT=6379
REDIS_STREAM_DB=0
```

### Docker Setup for Redis

Use the provided docker-compose.yml to set up Redis:

```bash
docker-compose up -d
```

## Usage

### Running the Pipeline

Start the pipeline to listen for HDX events:

```bash
python main.py
```

The pipeline will:
1. Listen for events on the `sdd:tables` Redis stream
2. Download resources from HDX when events are received
3. Process files through the three classification stages
4. Return formatted results

### Testing with Event Generator

Use the provided event generator to test the pipeline:

```bash
python redis_streams_event_generator.py
```

## Pipeline Workflow

### 1. Event Processing
- Receives HDX events containing `resource_id`
- Downloads CSV/Excel files from HDX API
- Preprocesses files to extract table structure
```python
{
    'resource_id': '1234567890',
    'file_name': 'example.csv',
    'file_url': 'https://example.com/example.csv'
    'processing_timestamp': '2025-01-01 12:00:00'
    'processing_success': True,
    'n_records': 100,
    'n_columns': 10,
    'pii_sensitive': True,
    'non_pii_sensitive': True,
    'columns': [
        {
            'column_name': 'email_address',
            'sample_values': ['john@example.com', 'jane@company.com'],
            'pii': {
                'entity_type': 'EMAIL_ADDRESS',
                'sensitive': True
            }
        }
    ]
    'non_pii': {
        'sensitivity': 'LOW'
        'explanation': 'The table contains email addresses, which are considered sensitive data.'
    }
}
```

### 2. PII Detection Phase
- Analyzes each column for PII entities
- Uses sample values and column names
- Returns entity types (e.g., PERSON_NAME, EMAIL_ADDRESS, etc.)

```python
{
    'column_name': 'email_address',
    'sample_values': ['john@example.com', 'jane@company.com'],
    'pii': {
        'entity_type': 'EMAIL_ADDRESS',
    }
}
```

### 3. PII Reflection Phase
- For columns with detected PII, determines sensitivity level
- Considers table context and entity type
- Returns sensitivity levels: NON_SENSITIVE, MODERATE_SENSITIVE, HIGH_SENSITIVE, SEVERE_SENSITIVE

```python
{
    'column_name': 'email_address',
    'sample_values': ['john@example.com', 'jane@company.com'],
    'pii': {
        'entity_type': 'EMAIL_ADDRESS',
        'sensitive': True
    }
}
```

### 4. Non-PII Classification Phase
- Analyzes overall table for non-PII sensitivity
- Uses ISP (Information Sensitivity Protocol) rules
- Considers humanitarian data sharing guidelines

### 5. Result Formatting
- Combines all classification results
- Formats for Redis response
- Includes processing metadata and summaries

## Configuration

### Model Configuration

Models can be configured in `utils/main_config.py`:

```python
NON_PII_DETECT_MODEL = 'gpt-4o-mini'
PII_DETECT_MODEL = 'gpt-4o-mini'
PII_REFLECT_MODEL = 'gpt-4o-mini'
```

### ISP Rules

Information Sensitivity Protocol rules are defined in `utils/main_config.py` under `ISP_DEFAULT`. These rules define sensitivity levels for humanitarian data sharing.

## File Structure

```
hdx-ssd-pipeline/
├── classifiers/          # Classification modules
├── llm_model/           # LLM integration
├── pipeline/            # Orchestration logic
├── preprocessing/       # Data preprocessing
├── prompts/            # Prompt templates
├── utils/          # Configuration and utils
├── main.py            # Main pipeline entry point
└── requirements.txt   # Dependencies
```

## Error Handling

The pipeline includes comprehensive error handling:

- Network errors during HDX downloads
- File processing errors
- Classification failures
- All errors are logged and returned in results

## Logging

Logs are written to:
- `logs/ssd.log` - General pipeline logs
- `logs/ssd-json.log` - JSON formatted logs

## Development

### Adding New Classifiers

1. Create new classifier in `classifiers/`
2. Extend `BaseClassifier`
3. Add to orchestrator in `pipeline/orchestrator.py`

### Testing

Run tests with:
```bash
python -m pytest test/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

[Add license information]

## Requirements
jinja2
openai
python-dotenv
requests