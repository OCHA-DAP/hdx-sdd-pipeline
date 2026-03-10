# HDX Sensitive Data Detection (SDD) Pipeline - Internal Workings

**Version:** 1.0  
**Last Updated:** February 19, 2026  
**Author:** HDX SDD Team

---

## Table of Contents

1. [Overview](#overview)
2. [Pipeline Architecture](#pipeline-architecture)
3. [Processing Flow](#processing-flow)
4. [Evaluation Steps](#evaluation-steps)
5. [Configuration Options](#configuration-options)
6. [Dataset Sampling](#dataset-sampling)
7. [Data Structures](#data-structures)
8. [LLM Integration](#llm-integration)
9. [ISP Rules System](#isp-rules-system)
10. [Error Handling](#error-handling)
11. [Performance Considerations](#performance-considerations)

---

## Overview

The HDX Sensitive Data Detection (SDD) Pipeline is a production-ready system that automatically analyzes humanitarian datasets to identify and classify sensitive information. The pipeline uses Azure OpenAI models to detect:

- **Personal Data Entities**: Names, emails, phone numbers, addresses, etc.
- **Personal Data Sensitivity**: Whether detected Personal Data Entities are actually sensitive in context (table-level)
- **Non-Personal Data Sensitivity**: Overall data sensitivity based on Information Sensitivity Protocols (ISP)

### Key Characteristics

- **Clean Architecture**: Domain-driven design with clear separation of concerns
- **Multi-Stage Evaluation**: Three distinct LLM-based classification steps
- **Flexible Configuration**: Supports different models for different tasks
- **ISP Integration**: Country-specific sensitivity rules
- **Production-Ready**: Comprehensive logging, error handling, and monitoring

---

## Pipeline Architecture

The pipeline follows **Clean Architecture** principles with four distinct layers:

### 1. Domain Layer (`src/domain/`)

Contains core business logic and entities:

- **Entities**: `SheetReport`, `Column`, `PIIClassification`, `NonPIIClassification`
- **Value Objects**: `PIIEntityType`, `SensitivityLevel`
- **Exceptions**: Domain-specific exceptions

### 2. Application Layer (`src/application/`)

Orchestrates business logic:

- **Use Cases**: `ProcessDatasetUseCase` - main pipeline orchestrator
- **Interfaces**: Abstract definitions for data loading, LLM providers, and repositories

### 3. Infrastructure Layer (`src/infrastructure/`)

External implementations:

- **LLM Provider**: `AzureOpenAIProvider` - Azure OpenAI integration
- **Data Loader**: `SmartDataLoader` - intelligent data loading and preprocessing

### 4. Shared Layer (`src/shared/`)

Cross-cutting concerns:

- **Prompt Manager**: Template-based prompt management
- **Utilities**: Helper functions and common tools

---

## Processing Flow

The pipeline processes datasets through a well-defined sequence of steps:

```
┌─────────────────────────────────────────────────────────────┐
│                    1. DATA LOADING                          │
│  • Load from URL or file (CSV, Excel)                       │
│  • Detect and concatenate multi-row headers                 │
│  • Filter empty rows/columns                                │
│  • Sample data for analysis                                 │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                 2. SHEET CLASSIFICATION                     │
│  • Identify README sheets (skip processing)                 │
│  • Create SheetReport for each data sheet                   │
│  • Extract column names and sample values                   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              3. Personal Data DETECTION (Column-Level)      │
│  • Classify each column for Personal Data entity type       │
│  • Use Personal Data detection model                        │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│         4. Personal Data SENSITIVITY REFLECTION Table-Level │
│  • Consider context and use case                            │
│  • Use Personal Data reflection model                       |└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│        5. NON-Personal Data CLASSIFICATION (Table-Level)    │
│  • Analyze overall table sensitivity                        │
│  • Apply ISP rules for country/context                      │
│  • Use non-Personal Data classification model               │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              6. SENSITIVITY FLAG UPDATE                     │
│  • Update personal_data_sensitive flag                      │
│  • Update non_personal_data_sensitive flag                  │
│  • Generate final report                                    │
└─────────────────────────────────────────────────────────────┘
```

---

## Evaluation Steps

The pipeline performs **three distinct evaluation steps**, each with specific objectives and configurations.

### Step 1: Personal Data Entity Detection (Column-Level)

**Objective**: Identify which columns contain personally identifiable information.

**Input**:

- Column name
- Sample values (default: 5 samples)

**Process**:

1. For each column in the dataset
2. Render Personal Data Entity detection prompt with column context
3. Call LLM with `max_tokens=8` (short response expected)
4. Parse response to extract PII entity type

**Output**: One of the following entity types per column:

- `NONE` - No PII detected
- `NAME` - Person names
- `EMAIL` - Email addresses
- `PHONE` - Phone numbers
- `ADDRESS` - Physical addresses
- `ID_NUMBER` - Identification numbers
- `DATE_OF_BIRTH` - Birth dates
- `FINANCIAL` - Financial information
- `HEALTH` - Health-related data
- `BIOMETRIC` - Biometric data
- `UNDETERMINED` - Cannot determine

**Prompt Template** (`src/prompts/pii_detection/v0.jinja`):

```jinja
### INSTRUCTION
You are a PII classification system. Given a column name **AND** sample values,
determine if this column contains a specific type of PII.
Choose ONE category from the following list or respond with 'None' if the column
doesn't contain PII:

PII entities list: {{ PII_ENTITIES_LIST }}

Return ONLY the entity name or only 'None' with no additional text.

### INPUT
Column name: {{ column_name }}
{% if sample_values %}{{ sample_values }}{% endif %}

### RESPONSE
```

**Token Usage**: ~240 prompt tokens, ~5 completion tokens per column

**Model Configuration**: Uses `PII_DETECT_MODEL` environment variable

---

### Step 2: Personal Data Entity Sensitivity Reflection (Table-Level)

**Objective**: Determine if detected Personal Data Entities are actually sensitive in the given context.

**Input**:

- Detected Personal Data Entity type
- Table context (sheet name)

**Process**:

1. For each sheet identified as containing Personal Data Entities
2. Render Personal Data Entity sensitivity reflection prompt with full context
3. Call LLM with `max_tokens=16`
4. Parse response to determine sensitivity

**Output**: Binary classification per sheet:

- `SENSITIVE` - Personal Data Entity could identify a person
- `NON_SENSITIVE` - Personal Data Entity cannot identify a person (e.g., aggregate data)

**Prompt Template** (`src/prompts/pii_reflection/v2.jinja`):

````jinja
### Instruction:
You are a personal data sensitivity classification system.

Given a table, a list of detected personal data entities in the table header, and sample rows, determine whether the dataset as a whole could reasonably be used to identify one or more individual persons.

The table may contain personal data entities such as age, gender, or other demographic attributes.
The presence of personal data alone does NOT make a dataset sensitive. Sensitivity depends on whether the data, in combination, enables identification of specific individuals.

Evaluate the dataset at the sheet level, not at the individual column level.

A dataset is considered:
- NON_SENSITIVE if it cannot reasonably be used to identify individuals, even if it contains personal data entities (e.g., age ranges, gender, non-unique demographics, aggregate or anonymized microdata).
- MODERATE_SENSITIVE if it contains row-level microdata with quasi-identifiers that somewhat increase re-identification risk, but without strong direct identifiers or highly unique combinations that make individuals readily identifiable.
- HIGH_SENSITIVE if it could reasonably be used to identify individuals due to row-level microdata combined with direct identifiers (e.g., names, full addresses, IDs) or powerful quasi-identifiers that meaningfully increase re-identification risk.

Important rules:
- Do NOT assume a column detected as a personal data entity is identifying by default.
- Do NOT assume a column detected as "date" is a date of birth unless clearly supported by context.
- Treat personal attributes (e.g., age, gender) as sensitive ONLY IF they are combined with other identifying or quasi-identifying variables such that identification is reasonably possible.
- Use only the information provided in the table header and sample rows.

### Input:
Table:
{{ table_markdown }}

### **JSON Response Format:**

Provide the output as a single, valid JSON object following this exact schema:

```json
{
  "sensitivity": "<ONE OF: NON_SENSITIVE / MODERATE_SENSITIVE / HIGH_SENSITIVE>",
  "explanation": "<Provide a brief, clear explanation of WHY the final SensitivityClassification was chosen.>"
}
````

**Token Usage**: ~300 prompt tokens, ~10 completion tokens per PII column

**Model Configuration**: Uses `PII_REFLECT_MODEL` environment variable

**Default Behavior**: If parsing fails or response is unclear, defaults to `SENSITIVE` (err on side of caution)

---

### Step 3: Non-Personal Data Sensitivity Classification (Table-Level)

**Objective**: Determine overall table sensitivity based on content and ISP rules.

**Input**:

- Table name
- Table summary (columns with Personal Data Entity annotations)
- Number of rows and columns
- ISP rules for the country/context

**Process**:

1. Create table summary with column overview
2. Load appropriate ISP rules
3. Render non-Personal Data classification prompt
4. Call LLM with `max_tokens=128` (longer response for explanation)
5. Extract sensitivity level from response
6. Store full explanation

**Output**: Sensitivity level classification:

- `NON_SENSITIVE` - Publicly shareable data
- `MODERATE_SENSITIVE` - Limited risk if disclosed
- `HIGH_SENSITIVE` - Significant harm if disclosed
- `SEVERE_SENSITIVE` - Serious harm or legal consequences
- `UNDETERMINED` - Cannot determine

**Prompt Template** (`src/prompts/non_pii_classification/v0.jinja`):

```jinja
### Instruction:
You are a data governance assistant. Your task is to determine the overall
sensitivity level of this table, based strictly on the provided Information
Sharing Protocols (ISP) and the table's content and state which columns make
the table sensitive.

Follow these exact steps:
1. Analyze the table schema AND the records of the table.
2. Use the ISP sensitivity levels: NON_SENSITIVE, MODERATE_SENSITIVE,
   HIGH_SENSITIVE, SEVERE_SENSITIVE. Only assign a sensitivity level if
   explicitly supported by ISP guidance.
3. Identify ONLY the columns that are sensitive on their own OR that become
   sensitive in combination with others, DIRECTLY supported by ISP guidance.
4. If multiple sensitivity levels might apply, always choose the highest one
   explicitly mentioned in the ISP for the relevant data type.

### Input:
ISP Rules
SEVERE_SENSITIVE: {{ isp.sensitivity_rules.SEVERE_SENSITIVE['data and information type'] }}
HIGH_SENSITIVE: {{ isp.sensitivity_rules.HIGH_SENSITIVE['data and information type'] }}
MODERATE_SENSITIVE: {{ isp.sensitivity_rules.MODERATE_SENSITIVE['data and information type'] }}
NON_SENSITIVE: {{ isp.sensitivity_rules['LOW/NON_SENSITIVE']['data and information type'] }}

Table:
{{ table_markdown }}

### Response Format:
- Sensitivity Classification: <ONE OF: NON_SENSITIVE / MODERATE_SENSITIVE / HIGH_SENSITIVE / SEVERE_SENSITIVE>
- List with ONLY the columns that are sensitive or in combination with other columns are sensitive.
- Cited ISP Rule(s): Quote the specific ISP rule(s) that directly support the classification.
- No markdown
```

**Token Usage**: ~500-1000 prompt tokens, ~50-100 completion tokens per table

**Model Configuration**: Uses `NON_PII_DETECT_MODEL` environment variable

**Extraction Logic**: The pipeline uses multiple strategies to extract sensitivity:

1. Look for "Classification: LEVEL" format
2. Search for sensitivity keywords in text
3. Fallback to `SensitivityLevel.from_string()` method

---

## Configuration Options

The pipeline supports extensive configuration through environment variables and programmatic settings.

### Environment Variables

#### Azure OpenAI Configuration

```bash
# Required: Azure OpenAI credentials
AZURE_OPENAI_API_KEY=your_api_key_here
AZURE_OPENAI_ENDPOINT=https://your-endpoint.openai.azure.com/

# Model Selection (can use different models for each task)
PII_DETECT_MODEL=gpt-4.1-nano          # Fast, cheap for PII detection
PII_REFLECT_MODEL=gpt-4.1-nano         # Fast for sensitivity reflection
NON_PII_DETECT_MODEL=gpt-4.1-mini      # More capable for complex ISP analysis
README_SCAN_MODEL=gpt-4.1-nano         # For README detection
```

#### Worker Configuration (for production deployment)

```bash
# Redis Stream Configuration
WORKER_ENABLED=true                     # Enable worker mode
REDIS_STREAM_STREAM_NAME=hdx_event_stream
REDIS_STREAM_GROUP_NAME=hdx_sdd_group
REDIS_STREAM_CONSUMER_NAME=hdx_sdd_consumer_1
REDIS_STREAM_HOST=redis
REDIS_STREAM_PORT=6379
REDIS_STREAM_DB=7
```

#### HDX Integration

```bash
# HDX CKAN Configuration
HDX_URL=https://data.humdata.org
HDX_KEY=your_hdx_api_key
```

#### Processing Configuration

```bash
# Processing Options
RERUN=false                             # Reprocess already-processed datasets
OUTPUT_DIR=/tmp/reports                 # Output directory for reports
DOWNLOAD_DIR=/tmp/download              # Temporary download directory
```

#### Slack Notifications

```bash
# Slack Integration
HDX_SDD_SLACK_CHANNEL=topic-sensitive-data-alerts
HDX_SDD_SLACK_ACCESS_TOKEN=xoxb-your-token
```

### Programmatic Configuration

#### Pipeline Initialization

```python
from src.application.use_cases.process_dataset import ProcessDatasetUseCase
from src.infrastructure.llm.azure_openai_provider import AzureOpenAIProvider
from src.infrastructure.storage.data_loader import SmartDataLoader
from src.shared.utils.prompt_manager import PromptManager

# Configure data loader
data_loader = SmartDataLoader(
    max_rows=1000,              # Maximum rows to load (None for unlimited)
)

# Configure LLM providers (can use different models)
pii_llm = AzureOpenAIProvider(
    model_name="gpt-4.1-nano",
    azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT'),
    api_key=os.getenv('AZURE_OPENAI_API_KEY'),
)

pii_reflection_llm = AzureOpenAIProvider(
    model_name="gpt-4.1-nano",
    azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT'),
    api_key=os.getenv('AZURE_OPENAI_API_KEY'),
)

non_pii_llm = AzureOpenAIProvider(
    model_name="gpt-4.1-mini",  # More capable model for complex analysis
    azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT'),
    api_key=os.getenv('AZURE_OPENAI_API_KEY'),
)

# Configure prompt manager
prompt_manager = PromptManager(
    prompts_dir='src/prompts'   # Directory containing prompt templates
)

# Create pipeline
pipeline = ProcessDatasetUseCase(
    data_loader=data_loader,
    pii_llm_provider=pii_llm,
    pii_reflection_llm_provider=pii_reflection_llm,
    non_pii_llm_provider=non_pii_llm,
    prompt_manager=prompt_manager,
    sample_size=5,              # Number of sample values per column
)
```

#### Execution Configuration

```python
# Process a dataset
reports = pipeline.execute(
    source="path/to/data.xlsx",     # URL or file path
    resource_id="dataset-123",      # Optional identifier
    is_url=False,              p     # True for URLs, False for files
    isp_rules=isp_rules,            # ISP rules dictionary (optional)
)
```

### Data Loader Configuration

The `SmartDataLoader` supports several configuration options:

```python
data_loader = SmartDataLoader(
    max_rows=1000,                  # Limit rows loaded (None = unlimited)
    # Additional options available in the implementation:
    # - Header detection sensitivity
    # - Empty row/column thresholds
    # - Sampling strategy
)
```

**Features**:

- Automatic header detection (handles multi-row headers)
- Empty row/column filtering
- Smart sampling (prioritizes most complete rows)
- Support for CSV, XLS, XLSX formats
- URL and local file loading

### Batch Processing Configuration

For batch processing multiple datasets:

```bash
# Process all datasets with a specific model
uv run python batch_process_model.py --model gpt-4.1-nano

# Skip already-processed datasets
uv run python batch_process_model.py --model gpt-4.1-nano --skip-existing

# Limit number of datasets (for testing)
uv run python batch_process_model.py --model gpt-4.1-nano --limit 10
```

**Batch Processing Features**:

- Processes all datasets in `research/results/test_results/groundtruth2/`
- Saves results to `research/results/test_results/{model_name}/`
- Supports skip-existing flag for incremental processing
- **Automatic ISP rules loading** using EventProcessor's country matching logic
- Comprehensive progress reporting

---

## Dataset Sampling

The pipeline uses intelligent dataset sampling to provide representative data to the LLMs while minimizing token usage and processing time.

### Sampling Strategy

**SmartDataLoader.sample_dataframe() Method**:

```python
def sample_dataframe(self, df: pd.DataFrame, sample_size: int = 5) -> Dict[str, List[Any]]:
    """
    Sample values from DataFrame using the most complete rows.

    Args:
        df: DataFrame to sample from
        sample_size: Number of samples per column (default: 5)

    Returns:
        Dictionary mapping column names to sample values
    """
```

### Sampling Process

1. **Row Ordering**: DataFrames are preprocessed to sort rows by completeness
   - Rows with the fewest null values appear first
   - Ensures highest quality samples for LLM analysis

2. **Column Sampling**: For each column:
   - Drop empty/null values from the column
   - Take the top `sample_size` values (default: 5)
   - Pad with empty strings if fewer values exist

3. **Sample Storage**: Samples are stored in `Column` entities:
   ```python
   column = Column(name=col_name, sample_values=sample_values)
   ```

### Configuration

**Sample Size Configuration**:

```python
# In ProcessDatasetUseCase initialization
pipeline = ProcessDatasetUseCase(
    data_loader=data_loader,
    pii_llm_provider=pii_llm,
    pii_reflection_llm_provider=pii_reflection_llm,
    non_pii_llm_provider=non_pii_llm,
    prompt_manager=prompt_manager,
    sample_size=5,  # Number of samples per column
)
```

**Recommended Sample Sizes**:

- **Default**: 5 samples per column (balanced accuracy vs. cost)
- **High Variability Data**: 10 samples per column
- **Large Datasets**: 3 samples per column (cost optimization)

### Sampling in LLM Prompts

**PII Detection Prompt**:

```jinja
### INPUT
Column name: {{ column_name }}
{% if sample_values %}{{ sample_values }}{% endif %}
```

**PII Reflection Prompt**: Uses sampled data in markdown table format:

```python
# Sample values are formatted as markdown table
column_samples = {}
for col in report.columns:
    if col.has_pii():
        key = f'{col.name} - {col.pii_classification.entity_type}'
    else:
        key = col.name
    column_samples[key] = col.sample_values
```

### Benefits of Smart Sampling

1. **Cost Efficiency**: Reduces token usage by ~90% compared to full dataset analysis
2. **Quality Focus**: Prioritizes complete, representative data
3. **Consistency**: Standardized sample size across all evaluations
4. **Flexibility**: Configurable sample size for different use cases

### Example Output

```python
# Sample dictionary for a dataset with 3 columns
{
    "name": ["John Doe", "Jane Smith", "Bob Johnson", "", ""],
    "email": ["john@example.com", "jane@example.com", "bob@example.com", "", ""],
    "age": [25, 30, 35, 28, 42]
}
```

---

## Data Structures

### SheetReport Entity

The main aggregate root representing a complete sheet analysis:

```python
@dataclass
class SheetReport:
    # Identification
    resource_id: Optional[str]              # Dataset identifier
    file_name: str                          # Source file name
    file_url: Optional[str]                 # Source URL (if applicable)
    sheet_name: str                         # Sheet/table name

    # Metadata
    processing_timestamp: datetime          # When processed
    processing_success: bool                # Success flag
    n_records: int                          # Number of rows
    n_columns: int                          # Number of columns

    # Token usage tracking
    completion_tokens: int                  # LLM completion tokens
    prompt_tokens: int                      # LLM prompt tokens

    # Model information
    pii_classifier_model: Optional[str]     # Model used for PII detection
    pii_reflection_model: Optional[str]     # Model used for PII reflection
    non_pii_model: Optional[str]            # Model used for non-PII
    readme_model: Optional[str]             # Model used for README detection

    # Classifications
    columns: List[Column]                   # Column-level classifications
    non_pii_classification: NonPIIClassification  # Table-level classification

    # Sensitivity flags (computed)
    personal_data_sensitive: bool           # Has sensitive PII
    non_personal_data_sensitive: bool       # Sensitive per ISP rules

    # Error handling
    error_source: Optional[str]             # Error location
    error_message: Optional[str]            # Error details

    # Special cases
    is_readme: bool                         # Is this a README sheet?
    readme_content: Optional[str]           # README content
```

**Key Methods**:

- `add_column(column)` - Add a column to the report
- `has_pii_columns()` - Check if any column contains PII
- `has_sensitive_pii()` - Check if any column has sensitive PII
- `update_pii_sensitivity()` - Update personal_data_sensitive flag
- `update_non_pii_sensitivity()` - Update non_personal_data_sensitive flag
- `is_sensitive()` - Check if sheet is sensitive (PII or non-PII)
- `total_tokens()` - Calculate total tokens used
- `to_dict()` - Convert to dictionary for serialization
- `from_dict(data)` - Create from dictionary

### Column Entity

Represents a single column with its classifications:

```python
@dataclass
class Column:
    name: str                               # Column name
    sample_values: List[Any]                # Sample values
    pii_classification: PIIClassification   # PII classification result
```

**Key Methods**:

- `has_pii()` - Check if column contains PII
- `is_sensitive()` - Check if column has sensitive PII
- `has_valid_samples()` - Check if sample values are valid

### PIIClassification

PII classification result for a column:

```python
@dataclass
class PIIClassification:
    entity_type: PIIEntityType              # Type of PII detected
    sensitive: bool                         # Is it sensitive?
    explanation: Optional[str]              # Explanation (optional)
```

**Sensitivity Flag Logic**: The `sensitive` field in PIIClassification is determined by the table-level `personal_data_sensitive` flag:

- **If `personal_data_sensitive=True`**: All recognized PII entity columns are marked as `sensitive=True`
- **If `personal_data_sensitive=False`**: All PII entity columns are marked as `sensitive=False`

This approach ensures that individual column sensitivity aligns with the overall table-level PII sensitivity assessment, providing consistent classification for Jira reporting and downstream processing.

### NonPIIClassification

Non-PII classification result for a table:

```python
@dataclass
class NonPIIClassification:
    sensitivity: SensitivityLevel           # Sensitivity level
    explanation: Optional[str]              # LLM explanation
    isp_name: Optional[str]                 # ISP rule set used
    sensitive_columns: List[str]            # Columns contributing to sensitivity
```

**Key Methods**:

- `is_sensitive()` - Check if table is sensitive per ISP rules

### Value Objects

**PIIEntityType** (Enum):

```python
class PIIEntityType(Enum):
    NONE = "None"
    NAME = "Name"
    EMAIL = "Email"
    PHONE = "Phone"
    ADDRESS = "Address"
    ID_NUMBER = "ID Number"
    DATE_OF_BIRTH = "Date of Birth"
    FINANCIAL = "Financial"
    HEALTH = "Health"
    BIOMETRIC = "Biometric"
    UNDETERMINED = "Undetermined"
```

**SensitivityLevel** (Enum):

```python
class SensitivityLevel(Enum):
    NON_SENSITIVE = "NON_SENSITIVE"
    MODERATE_SENSITIVE = "MODERATE_SENSITIVE"
    MEDIUM_SENSITIVE = "MEDIUM_SENSITIVE"
    HIGH_SENSITIVE = "HIGH_SENSITIVE"
    SEVERE_SENSITIVE = "SEVERE_SENSITIVE"
    UNDETERMINED = "UNDETERMINED"
```

---

## LLM Integration

### AzureOpenAIProvider

The pipeline uses Azure OpenAI through a custom provider implementation:

```python
class AzureOpenAIProvider(ILLMProvider):
    """Azure OpenAI implementation with token tracking and error handling."""

    def __init__(
        self,
        model_name: str,
        azure_endpoint: str,
        api_key: str,
    ):
        self.model_name = model_name
        self.client = AzureOpenAI(
            azure_endpoint=azure_endpoint,
            api_key=api_key,
            api_version="2024-02-15-preview"
        )
```

**Key Methods**:

1. **`generate(prompt, max_tokens)`** - Generate text completion
   - Returns: `(response_text, completion_tokens, prompt_tokens)`
   - Tracks token usage
   - Logs warnings for high token usage
   - Comprehensive error handling

2. **`generate_json(prompt, max_tokens)`** - Generate JSON response
   - Returns: `(json_dict, completion_tokens, prompt_tokens)`
   - Validates JSON structure
   - Handles parsing errors

**Token Tracking**:

- All token usage is tracked per request
- Cumulative tokens stored in `SheetReport`
- Warnings logged for requests exceeding thresholds
- Performance metrics included in logs

**Error Handling**:

- API errors caught and logged with context
- Rate limiting handled with exponential backoff
- Timeout errors logged with request details
- All errors include model name and prompt length

### Prompt Management

The `PromptManager` handles template-based prompt generation:

```python
class PromptManager:
    """Manages prompt templates using Jinja2."""

    def __init__(self, prompts_dir: str = 'src/prompts'):
        self.prompts_dir = prompts_dir
        self.env = Environment(loader=FileSystemLoader(prompts_dir))

    def get_prompt(
        self,
        prompt_type: str,      # e.g., 'pii_detection'
        version: str,          # e.g., 'v0'
        context: dict          # Template variables
    ) -> str:
        """Render a prompt template with context."""
```

**Prompt Organization**:

```
src/prompts/
├── pii_detection/
│   ├── v0.jinja
│   └── v1.jinja
├── pii_reflection/
│   └── v0.jinja
├── non_pii_classification/
│   ├── v0.jinja
│   └── v1.jinja
└── readme_scan/
    └── v0.jinja
```

**Versioning**: Multiple versions can coexist, allowing A/B testing and gradual rollout

---

## ISP Rules System

### ISP Structure

Information Sensitivity Protocols (ISP) are country-specific rules stored in `data/isps.json`:

```json
{
  "OCHA Afghanistan": {
    "country": "afghanistan",
    "sensitivity_rules": {
      "LOW/NON_SENSITIVE": {
        "data and information type": [
          "HNO and underlying national-level needs assessment data",
          "CODs",
          "3W/4W data (at national and provincial level)"
        ]
      },
      "MODERATE_SENSITIVE": {
        "data and information type": [
          "Survey or needs assessment data aggregated to the district level"
        ]
      },
      "HIGH_SENSITIVE": {
        "data and information type": [
          "Survey or needs assessment data aggregated to the community level",
          "Aid-Worker Contact Details / Lists"
        ]
      },
      "SEVERE_SENSITIVE": {
        "data and information type": [
          "Raw survey and raw needs assessment data",
          "Personal data of beneficiaries (i.e. Beneficiary lists)"
        ]
      }
    }
  }
}
```

### Available ISP Rule Sets

The pipeline includes ISP rules for:

- OCHA Afghanistan
- OCHA Burundi
- OCHA Cameroon (NWSW)
- OCHA Cameroon (Extreme Nord)
- OCHA Democratic Republic of the Congo
- OCHA Iraq
- OCHA Mozambique
- OCHA Myanmar
- OCHA Niger
- OCHA Occupied Palestinian Territory
- OCHA Somalia
- OCHA South Sudan
- OCHA Sudan
- OCHA Syria
- OCHA Ukraine
- OCHA Venezuela
- OCHA Yemen
- **Default** (global fallback)

### ISP Loading

```python
def load_isp_rules(country: str = 'default') -> dict:
    """Load ISP rules for a specific country."""
    # 1. Load data/isps.json
    # 2. Match country name (case-insensitive, partial match)
    # 3. Return matched ISP or default
```

**Matching Logic**:

- Exact match on country name
- Case-insensitive comparison
- Partial matching (e.g., "ukraine" matches "OCHA Ukraine")
- Falls back to "default" if no match

### Default ISP Rules

The default ISP provides general humanitarian data sensitivity guidelines:

**NON_SENSITIVE**:

- HNO/HRP data
- CODs
- 3W/4W/5W data at ADM1/ADM2
- Situation reports
- Generic contact details

**MODERATE_SENSITIVE**:

- Assessment data at ADM2/ADM3
- Disaggregated data without personal identifiers
- Access constraints data
- Security incident reports (aggregated)

**HIGH_SENSITIVE**:

- Aid-worker contact details (without consent)
- Community/household-level survey data
- Detailed operational presence data
- Facility data with exact coordinates

**SEVERE_SENSITIVE**:

- Personal data of beneficiaries
- Individual survey responses
- SEA/GBV/PSEA case data
- Raw feedback/complaints data
- Location of displaced individuals

---

## Error Handling

The pipeline implements comprehensive error handling at multiple levels:

### Domain-Level Exceptions

```python
class DataProcessingError(Exception):
    """Raised when data processing fails."""
    pass

class LLMProviderError(Exception):
    """Raised when LLM provider encounters an error."""
    pass

class DataLoadingError(Exception):
    """Raised when data loading fails."""
    pass
```

### Use Case Error Handling

```python
try:
    # Load data
    sheets = self.data_loader.load_from_url(source)

    # Process each sheet
    for sheet_name, df in sheets.items():
        report = self._create_data_report(...)
        reports.append(report)

    logger.info(f"Successfully processed {len(sheets)} sheets")
    return reports

except Exception as e:
    logger.error(
        f'Failed to process dataset: {e}',
        exc_info=True,
        extra={'source': source, 'resource_id': resource_id}
    )
    raise DataProcessingError(f'Dataset processing failed: {e}')
```

**Logging Improvements**: The pipeline now uses structured logging throughout:

- `logger.info()` for status updates (replacing `print()` statements)
- `logger.debug()` for detailed diagnostics
- `logger.error()` for error conditions with full context
- `logger.warning()` for important but non-fatal issues

### Column-Level Error Handling

```python
for column in report.columns:
    try:
        # Classify PII
        result, comp_tokens, prompt_tokens = self.pii_llm.generate(...)
        column.pii_classification.entity_type = PIIEntityType.from_string(result)

    except Exception as e:
        logger.error(f"PII classification failed for column '{column.name}': {e}")
        column.pii_classification.entity_type = PIIEntityType.UNDETERMINED
```

**Strategy**: Continue processing other columns even if one fails

### LLM Provider Error Handling

```python
try:
    response = self.client.chat.completions.create(...)
    return response.choices[0].message.content

except OpenAIError as e:
    logger.error(
        f'Azure OpenAI API error: {e}',
        extra={
            'model': self.model_name,
            'prompt_length': len(prompt)
        }
    )
    raise LLMProviderError(f'LLM generation failed: {e}')
```

### Batch Processing Error Handling

```python
for dataset_name in datasets:
    try:
        # Process dataset
        sheet_reports = pipeline.execute(...)
        successful += 1

    except Exception as e:
        logger.error(f'Failed to process {dataset_name}: {e}', exc_info=True)
        failed += 1
        # Continue with next dataset
```

**Strategy**: Log errors but continue batch processing

### Error Reporting

All errors are:

1. **Logged** with full context and stack traces
2. **Tracked** in metrics (success/failure counts)
3. **Stored** in SheetReport (error_source, error_message)
4. **Reported** via Slack (in production)

---

## Performance Considerations

### Token Usage Optimization

**Per-Column PII Detection**:

- Average: 240 prompt + 5 completion = 245 tokens
- For 20 columns: ~4,900 tokens

**Per-PII-Column Reflection**:

- Average: 300 prompt + 10 completion = 310 tokens
- For 5 PII columns: ~1,550 tokens

**Per-Table Non-PII Classification**:

- Average: 700 prompt + 75 completion = 775 tokens
- For 1 table: ~775 tokens

**Total for typical dataset** (20 columns, 5 PII, 1 table):

- ~7,225 tokens per sheet
- Cost: ~$0.001-0.01 depending on model

### Model Selection Strategy

**Recommended Configuration**:

```bash
# Fast, cheap model for simple classification
PII_DETECT_MODEL=gpt-4.1-nano

# Fast model for binary decisions
PII_REFLECT_MODEL=gpt-4.1-nano

# More capable model for complex ISP analysis
NON_PII_DETECT_MODEL=gpt-4.1-mini
```

**Cost vs. Quality Trade-offs**:

- `gpt-4.1-nano`: Fastest, cheapest, good for simple tasks
- `gpt-4.1-mini`: Balanced performance and cost
- `gpt-4.1`: Most capable, highest cost

### Data Loading Optimization

**SmartDataLoader Features**:

1. **Row Limiting**: Set `max_rows` to limit data loaded
2. **Smart Sampling**: Prioritizes most complete rows
3. **Header Detection**: Automatic multi-row header handling
4. **Empty Filtering**: Removes empty rows/columns

**Recommendations**:

- Set `max_rows=1000` for large datasets
- Use `sample_size=5` (default) for most cases
- Increase to `sample_size=10` for highly variable data

### Batch Processing Optimization

**Parallelization**: Currently sequential, could be parallelized:

```python
# Future: Process multiple datasets in parallel
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(process_dataset, ds) for ds in datasets]
```

**Caching**: Consider caching LLM responses for identical inputs

**Skip Existing**: Use `--skip-existing` flag to avoid reprocessing

### Logging Performance

**Log Levels**:

- **Production**: INFO level (minimal overhead)
- **Development**: DEBUG level (detailed diagnostics)
- **Performance Testing**: WARNING level (only issues)

**Configuration**:

```python
# logging.conf
[logger_root]
level=INFO  # Change to DEBUG for development
```

### Monitoring Metrics

**Key Metrics to Track**:

1. **Processing Time**: Time per sheet, per dataset
2. **Token Usage**: Total tokens, cost estimation
3. **Success Rate**: Successful vs. failed classifications
4. **Error Rate**: Errors per 100 datasets
5. **Model Performance**: Accuracy, precision, recall (with ground truth)

**Example Metrics Output**:

```
Successfully processed 3 sheet(s) in 10.23s, total_tokens=1,234
Sheet 'Data': personal_data_sensitive=True, non_pii_sensitivity=HIGH_SENSITIVE, tokens=456
```

---

## Appendix: Complete Configuration Reference

### All Environment Variables

```bash
# ============================================================================
# AZURE OPENAI CONFIGURATION
# ============================================================================
AZURE_OPENAI_API_KEY=your_api_key
AZURE_OPENAI_ENDPOINT=https://your-endpoint.openai.azure.com/

# ============================================================================
# MODEL SELECTION
# ============================================================================
PII_DETECT_MODEL=gpt-4.1-nano
PII_REFLECT_MODEL=gpt-4.1-nano
NON_PII_DETECT_MODEL=gpt-4.1-mini
README_SCAN_MODEL=gpt-4.1-nano

# ============================================================================
# WORKER CONFIGURATION (Production)
# ============================================================================
WORKER_ENABLED=true
REDIS_STREAM_STREAM_NAME=hdx_event_stream
REDIS_STREAM_GROUP_NAME=hdx_sdd_group
REDIS_STREAM_CONSUMER_NAME=hdx_sdd_consumer_1
REDIS_STREAM_HOST=redis
REDIS_STREAM_PORT=6379
REDIS_STREAM_DB=7

# ============================================================================
# HDX INTEGRATION
# ============================================================================
HDX_URL=https://data.humdata.org
HDX_KEY=your_hdx_api_key

# ============================================================================
# PROCESSING OPTIONS
# ============================================================================
RERUN=false
OUTPUT_DIR=/tmp/reports
DOWNLOAD_DIR=/tmp/download

# ============================================================================
# SLACK NOTIFICATIONS
# ============================================================================
HDX_SDD_SLACK_CHANNEL=topic-sensitive-data-alerts
HDX_SDD_SLACK_ACCESS_TOKEN=xoxb-your-token
```

### All Programmatic Options

```python
# Data Loader
data_loader = SmartDataLoader(
    max_rows=1000,                      # Maximum rows to load
)

# LLM Provider
llm_provider = AzureOpenAIProvider(
    model_name="gpt-4.1-nano",          # Model to use
    azure_endpoint="...",               # Azure endpoint
    api_key="...",                      # API key
)

# Prompt Manager
prompt_manager = PromptManager(
    prompts_dir='src/prompts'           # Prompts directory
)

# Pipeline
pipeline = ProcessDatasetUseCase(
    data_loader=data_loader,            # Data loader instance
    pii_llm_provider=pii_llm,           # PII detection LLM
    pii_reflection_llm_provider=refl_llm,  # PII reflection LLM
    non_pii_llm_provider=non_pii_llm,   # Non-PII classification LLM
    prompt_manager=prompt_manager,      # Prompt manager
    sample_size=5,                      # Samples per column
)

# Execution
reports = pipeline.execute(
    source="path/to/data.xlsx",         # Data source
    resource_id="dataset-123",          # Resource ID
    is_url=False,                       # URL or file path
    isp_rules=isp_rules,                # ISP rules dict
)
```

---

## Summary

The HDX SDD Pipeline is a sophisticated, production-ready system for detecting sensitive data in humanitarian datasets. Its three-stage evaluation process (PII Detection → PII Reflection → Non-PII Classification) provides comprehensive sensitivity analysis while maintaining flexibility through extensive configuration options.

**Key Strengths**:

- ✅ Clean, maintainable architecture
- ✅ Comprehensive error handling
- ✅ Flexible model configuration
- ✅ Country-specific ISP rules
- ✅ Production-ready logging and monitoring
- ✅ Extensive test coverage (97%)

**For More Information**:

- Architecture: See `README.md`
- API Reference: See code documentation
- Testing: See `tests/` directory
- Examples: See `tutorial.py`
