# HDX Sensitive Data Detection (SDD) Pipeline - Internal Workings

**Version:** 1.1  
**Last Updated:** September 3, 2026  
**Author:** HDX SDD Team

---

## Table of Contents

1. [Overview](#overview)
2. [Pipeline Architecture](#pipeline-architecture)
3. [Processing Flow](#processing-flow)
4. [Evaluation Steps](#evaluation-steps)
   - [Step 1: Personal Data Entity Detection (Column-Level)](#step-1-personal-data-entity-detection-column-level)
   - [Step 2: Personal Data Entity Sensitivity Reflection (Table-Level)](#step-2-personal-data-entity-sensitivity-reflection-table-level)
   - [Step 3: Non-Personal Data Sensitivity Classification (Table-Level)](#step-3-non-personal-data-sensitivity-classification-table-level)
5. [Risk Level Scoring & Propagation](#risk-level-scoring--propagation)
6. [Dataset Loading, Sampling & Normalization](#dataset-loading-sampling--normalization)
7. [Data Structures & Entities](#data-structures--entities)
8. [LLM Integration & Provider Architecture](#llm-integration--provider-architecture)
9. [ISP Rules & Strategy System](#isp-rules--strategy-system)
10. [Metadata-Aware Prompting](#metadata-aware-prompting)
11. [Configuration Reference & Security](#configuration-reference--security)
12. [Error Handling & Notifications](#error-handling--notifications)
13. [Performance Considerations](#performance-considerations)

---

## Overview

The HDX Sensitive Data Detection (SDD) Pipeline is an automated system that analyzes humanitarian datasets to identify and classify sensitive information. The pipeline uses OpenAI models to detect:

- **Personal Data Entities**: Names, emails, phone numbers, addresses, ID numbers, birth dates, financial/health/biometric data, and geographic coordinates.
- **Personal Data Sensitivity**: Whether detected Personal Data Entities enable direct or quasi-identifier re-identification of individual human beings in context (table-level).
- **Non-Personal Data Sensitivity**: Overall data sensitivity based on country-specific Information Sensitivity Protocols (ISP) or global fallback rules.

### Key Characteristics

- **Clean Architecture**: Domain-driven design with clear separation of concerns across Domain, Application, Infrastructure, and Shared layers.
- **Multi-Stage Evaluation**: Three distinct LLM-based classification stages.
- **Hierarchical Risk Scoring**: Numeric risk level scoring (0–3) propagated across sheets and resource files.
- **Flexible ISP System**: Decoupled strategy pattern supporting local JSON and live Google Sheets ISP repositories with Redis caching.
- **Metadata-Aware Prompting**: Injecting dataset and resource context into LLM prompts with description truncation and location safety limits.
- **Production Security & Monitoring**: Header-scoped API credentials, custom user-agent identification, structured logging, and Slack error notifications.

---

## Pipeline Architecture

The pipeline follows **Clean Architecture** principles across four distinct layers:

### 1. Domain Layer (`src/domain/`)

Contains core business logic, aggregate roots, value objects, and repository protocols:

- **Entities**: [`SheetReport`](file:///Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/src/domain/entities.py), [`Column`](file:///Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/src/domain/entities.py), [`PIIClassification`](file:///Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/src/domain/entities.py), [`NonPIIClassification`](file:///Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/src/domain/entities.py)
- **Value Objects**: `PIIEntityType`, `SensitivityLevel`
- **Protocols & Interfaces**: `IISPStrategy` protocol for dynamic ISP retrieval
- **Exceptions**: Domain-specific processing and loader exceptions

### 2. Application Layer (`src/application/`)

Orchestrates pipeline processing workflows:

- **Use Cases**: [`ProcessDatasetUseCase`](file:///Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/src/application/process_dataset.py) — main pipeline orchestrator
- **Event Processor**: Event-driven subscriber handling CKAN resource events and payload context

### 3. Infrastructure Layer (`src/infrastructure/`)

External services and data provider implementations:

- **LLM Provider**: [`OpenAIProvider`](file:///Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/src/infrastructure/llm/openai_provider.py) — unified OpenAI API integration supporting standard and reasoning models (e.g. GPT-5 series)
- **Data Loader**: [`SmartDataLoader`](file:///Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/src/infrastructure/data_loader.py) — incremental chunked dataset loader and numeric normalizer
- **ISP Strategies**: `LocalJSONISPStrategy` and `GoogleSheetsISPStrategy` for loading and caching country ISP rules

### 4. Shared Layer (`src/shared/`)

Cross-cutting system utilities:

- **Prompt Manager**: Jinja2 template manager supporting versioned prompt resolution
- **Version Module**: Pipeline version resolution exposing runtime version metadata
- **Security Utilities**: URL domain verification and custom HTTP user-agent header configuration

---

## Processing Flow

The pipeline processes each dataset sheet through a deterministic evaluation sequence:

```mermaid
flowchart TD
    A["<b>1. DATA LOADING</b><br/>• Load from URL or local file (CSV, Excel)<br/>• Perform numeric string normalization<br/>• Smart header detection & incremental chunked loading<br/>• Extract representative sample values (Seed 42)"] --> B["<b>2. Personal Data DETECTION (Column-Level)</b><br/>• Classify column PII entity types via OpenAI Provider<br/>• Apply sample-value confirmation & phone false-positive mitigation rules"]
    
    B --> C["<b>3. Personal Data SENSITIVITY REFLECTION (Table-Level)</b><br/>• Assess individual re-identification risk<br/>• Exclude functional/organizational emails<br/>• Classify: NON_SENSITIVE / HIGH_SENSITIVE / SEVERE_SENSITIVE"]
    
    C --> D["<b>4. NON-Personal Data CLASSIFICATION (Table-Level)</b><br/>• Analyze table context against country ISP rules<br/>• Calculate dynamic max_tokens budget (min 2000)<br/>• Fallback to SEVERE_SENSITIVE on errors or UNDETERMINED"]
    
    D --> E["<b>5. RISK LEVEL SCORING & AGGREGATION</b><br/>• Calculate sheet personal_data_risk_level (0-3)<br/>• Calculate sheet non_personal_data_risk_level (0-3)<br/>• Aggregate maximum resource sensitivity_level (0-3)"]
```

---

## Evaluation Steps

### Step 1: Personal Data Entity Detection (Column-Level)

**Objective**: Determine whether a dataset column contains personally identifiable information based on name and sample values.

**Supported Entity Types**:
- `NONE` - No PII detected
- `NAME` - Person names
- `EMAIL` - Personal email addresses
- `PHONE` - Personal phone numbers
- `ADDRESS` - Physical or residential addresses
- `ID_NUMBER` - Identification numbers
- `DATE_OF_BIRTH` - Birth dates
- `FINANCIAL` - Financial accounts or compensation
- `HEALTH` - Medical or health data
- `BIOMETRIC` - Biometric identifiers
- `GEO_COORDINATES` - Latitude, longitude, or precise point coordinates (`FR-SDD-034`)
- `UNDETERMINED` - Cannot determine

**Special Rules**:
- **Sample-Value Confirmation (`FR-SDD-064`)**: Classification for `PERSON_NAME`, `EMAIL_ADDRESS`, and `PHONE_NUMBER` must not rely on column header names alone. Actual representative sample values must confirm the presence of valid PII.
- **Phone Number False-Positive Mitigation (`FR-SDD-058`)**: Short geographic area codes, country codes, FAOSTAT codes (e.g. `206`), or regional identifiers combined with names like "Area Code" must **not** be flagged as `PHONE`.
- **Latitude / Longitude Auto-Mapping**: Columns named `latitude` or `longitude` (case-insensitive) are automatically classified as `GEO_COORDINATES`.

**Prompt Template**: `src/prompts/pii_detection/v3.jinja`

**Failure Fallback (`FR-SDD-037`)**: If classification fails or returns `UNDETERMINED`, the column entity type is set to `UNKNOWN`/`UNDETERMINED` and marked as sensitive.

---

### Step 2: Personal Data Entity Sensitivity Reflection (Table-Level)

**Objective**: Determine whether detected Personal Data Entities within a table enable individual re-identification in context.

**Output Classification Scale (`FR-SDD-044`)**:
- `NON_SENSITIVE`: Cannot reasonably identify specific individuals (e.g., aggregate statistics, non-unique demographics).
- `HIGH_SENSITIVE`: Row-level microdata containing indirect/quasi-identifiers that increase re-identification risk.
- `SEVERE_SENSITIVE`: Direct identifiers (names, full addresses, IDs) or highly unique combinations making individuals readily identifiable.

**Special Rules**:
- **Personal-Data-Only Risk Focus (`FR-SDD-065`)**: Assesses individual human re-identification risk only. Location or facility precision is evaluated separately under Non-PII ISP rules.
- **Organizational Email Exclusion (`FR-SDD-067`)**: Functional mailboxes (`info@`, `contact@`, `data@`, shared team inboxes) are **not** personal data and must not contribute to `HIGH_SENSITIVE` or `SEVERE_SENSITIVE` ratings.
- **3-Step Evaluation**: 1) Identify unit of analysis (person, household, aggregate); 2) Identify individual-level columns; 3) Assign sensitivity level based strictly on individual-level columns.

**Prompt Template**: `src/prompts/pii_reflection/v4.jinja`

**Failure Fallback (`FR-SDD-038`)**: If reflection fails, the sheet is marked `personal_data_sensitive=True` by default with diagnostic exception details logged.

---

### Step 3: Non-Personal Data Sensitivity Classification (Table-Level)

**Objective**: Determine table sensitivity based on data content and applicable Information Sensitivity Protocols (ISP).

**Output Classification Scale**:
- `NON_SENSITIVE`: Publicly shareable data (CODs, national 3W/4W, generic boundaries).
- `MEDIUM_SENSITIVE`: Limited operational risk if disclosed (district-level assessments, aggregated access data).
- `HIGH_SENSITIVE`: Significant risk of harm (community-level surveys, aid-worker lists without consent, exact facility coordinates).
- `SEVERE_SENSITIVE`: Serious harm or legal consequences (beneficiary lists, raw survey microdata, SEA/GBV case data, exact locations of vulnerable groups).

**Special Rules**:
- **Dynamic Output Token Budget (`FR-SDD-056`)**: The LLM completion token budget (`max_tokens`) is dynamically set to `max(2000, n_columns * 5)` to prevent truncated explanations.
- **Admin Level & Operational Data Guidelines (`FR-SDD-055`)**:
  - The model leverages pre-trained geographic domain knowledge (e.g. recognizing that "Locality" in Sudan represents ADM2, not ADM3+).
  - 3W/4W/5W operational presence datasets are **not** classified as "contact lists" unless they contain actual individual personal contact details.
  - Aggregate population counts at ADM2 or higher are treated as general operational stats, not needs assessments.

**Prompt Templates**:
- **Standard ISP**: `src/prompts/non_pii_classification/v3.jinja`
- **Default ISP Fallback (`FR-SDD-033`, `FR-SDD-047`)**: `src/prompts/non_pii_classification/default/v1.jinja` (located in dedicated `default` subdirectory for country `'default'`).

**Failure Fallback (`FR-SDD-035`, `FR-SDD-039`)**: If non-PII classification fails or returns `UNDETERMINED`, sensitivity is automatically promoted to `SEVERE_SENSITIVE` with error details recorded in the explanation.

---

## Risk Level Scoring & Propagation

The pipeline converts categorical sensitivity outcomes into a numeric risk scoring system (`FR-SDD-044`) to allow downstream filtering and hierarchical aggregation.

### Numeric Risk Scale

| Level | Categorical Rating | Meaning |
| :---: | :--- | :--- |
| **0** | `NONE` / `NON_SENSITIVE` / `UNDETERMINED` | No sensitivity identified |
| **1** | `MEDIUM_SENSITIVE` | Medium operational sensitivity (NPD only) |
| **2** | `HIGH_SENSITIVE` | High sensitivity / quasi-identifier risk |
| **3** | `SEVERE_SENSITIVE` | Critical sensitivity / direct re-identification risk |

### Sheet-Level Risk

Every sheet report computes two independent risk scores:
- `personal_data_risk_level`: Mapped from PII reflection outcome (`NON_SENSITIVE` → 0, `HIGH_SENSITIVE` → 2, `SEVERE_SENSITIVE` → 3).
- `non_personal_data_risk_level`: Mapped from Non-PII classification outcome (`NON_SENSITIVE` → 0, `MEDIUM_SENSITIVE` → 1, `HIGH_SENSITIVE` → 2, `SEVERE_SENSITIVE` → 3).

Sheet overall risk is defined as:
$$\text{Sheet Risk} = \max(\text{personal\_data\_risk\_level}, \text{non\_personal\_data\_risk\_level})$$

### Resource-Level Maximum Propagation

Every resource (dataset file) report aggregates all contained sheet reports:
$$\text{Resource } \text{sensitivity\_level} = \max_{s \in \text{Sheets}}(\text{Sheet Risk}_s)$$

Resource overall sensitivity flag is categorized as:
- `not-sensitive` (both flags false)
- `sensitive-pd` (personal data sensitive only)
- `sensitive-non-pd` (non-personal data sensitive only)
- `sensitive-pd-and-non-pd` (both personal and non-personal data sensitive)

---

## Dataset Loading, Sampling & Normalization

Data loading and preprocessing are handled by `SmartDataLoader` ([`src/infrastructure/data_loader.py`](file:///Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/src/infrastructure/data_loader.py)).

### Incremental Chunked Loading & Random Sampling (`FR-SDD-043`)

Rather than loading entire large files into memory at once, datasets are loaded incrementally in chunks:
1. Data is read in increasing chunk steps (`100`, `1,000`, `10,000`, `25,000`, `50,000`, `100,000` rows).
2. Loading stops as soon as every column contains at least 5 unique non-empty values.
3. For each column, 5 representative sample values are randomly selected using a fixed random seed (`seed=42`) to maintain evaluation reproducibility.

### String Numeric Normalization (`FR-SDD-025`)

When parsing CSV or Excel dataframes, string-formatted numbers are normalized element-wise:
- Standard commas (`,`) and space thousands separators (including Unicode `NBSP` `\u00A0` and `NNBSP` `\u202F`) are stripped.
- Valid numeric strings are parsed into standard Python `int` or `float` objects, ensuring clean column analysis.

---

## Data Structures & Entities

### SheetReport Entity

The core aggregate root representing complete sheet analysis results:

```python
@dataclass
class SheetReport:
    resource_id: Optional[str]
    file_name: str
    file_url: Optional[str]
    sheet_name: str
    processing_timestamp: datetime
    processing_success: bool
    n_records: int
    n_columns: int

    # Token & Model Tracking
    completion_tokens: int
    prompt_tokens: int
    pii_classifier_model: Optional[str]
    pii_reflection_model: Optional[str]
    non_pii_model: Optional[str]
    readme_model: Optional[str]

    # Classifications
    columns: List[Column]
    non_pii_classification: NonPIIClassification

    # Sensitivity Flags & Numeric Risk Scores (FR-SDD-044)
    personal_data_sensitive: bool
    non_personal_data_sensitive: bool
    personal_data_risk_level: int       # Range 0-3
    non_personal_data_risk_level: int   # Range 0-3

    # Error & README Attributes
    error_source: Optional[str]
    error_message: Optional[str]
    is_readme: bool
    readme_content: Optional[str]
```

### Column Entity

```python
@dataclass
class Column:
    name: str
    sample_values: List[Any]
    pii_classification: PIIClassification
```

### PIIClassification Entity

```python
@dataclass
class PIIClassification:
    entity_type: PIIEntityType
    sensitive: bool
    explanation: Optional[str] = None
```

### NonPIIClassification Entity

```python
@dataclass
class NonPIIClassification:
    sensitivity: SensitivityLevel
    explanation: Optional[str] = None
    isp_name: Optional[str] = None
    sensitive_columns: List[str] = field(default_factory=list)
```

---

## LLM Integration & Provider Architecture

All LLM operations interact through a single, unified [`OpenAIProvider`](file:///Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/src/infrastructure/llm/openai_provider.py) class (`FR-SDD-036`).

```python
class OpenAIProvider:
    """Unified OpenAI API provider supporting standard and reasoning models."""
    def __init__(
        self,
        model_name: str,
        endpoint: Optional[str] = None,
        api_key: Optional[str] = None,
    ):
        self.model_name = model_name
        self.client = OpenAI(base_url=endpoint, api_key=api_key)
```

### Key Provider Capabilities

1. **Deterministic Execution (`seed=42`, FR-SDD-065)**: Passes `seed=42` on every completion request to ensure reproducible outputs across model runs.
2. **Reasoning Models Handling (`gpt-5` family, FR-SDD-059, FR-SDD-066)**:
   - Configures `reasoning_effort`: set to `'low'` for column PII detection and README scans; set to `'medium'` for PII reflection and Non-PII classification.
   - Automatically strips incompatible `temperature` and `top_p` parameters when reasoning is active (`reasoning_effort != 'none'`).
   - Expands `max_completion_tokens` by adding a safety buffer (`max_tokens + 8192`) to prevent internal token exhaustion.

---

## ISP Rules & Strategy System

The Information Sensitivity Protocol system is decoupled via the `IISPStrategy` protocol (`FR-SDD-061`).

```mermaid
classDiagram
    class IISPStrategy {
        <<interface>>
        +get_isp_rules(country: str) Dict
    }
    class LocalJSONISPStrategy {
        +Reads data/isps.json
        +get_isp_rules(country: str) Dict
    }
    class GoogleSheetsISPStrategy {
        +Fetches live Google Worksheet
        +Filters inactive/under-dev rules
        +get_isp_rules(country: str) Dict
    }
    IISPStrategy <|.. LocalJSONISPStrategy
    IISPStrategy <|.. GoogleSheetsISPStrategy
```

### ISP Strategy Implementations

1. **`LocalJSONISPStrategy`**: Reads country sensitivity definitions from `data/isps.json`.
2. **`GoogleSheetsISPStrategy` (`FR-SDD-063`, `FR-SDD-065`)**:
   - Fetches live protocol rules from the Google Sheets worksheet ("Data & Information Types Dataset").
   - Filters out rows marked as `Enabled == 'no'` or with `ISP Status` set to `'under development'` or `'not used'`.
   - Formats rules into modern `sensitivity_rules` dictionaries with legacy text-blob compatibility.
3. **Redis Caching (`FR-SDD-062`)**: In worker mode, resolved ISP rules are cached in Redis under `isp_rules_cache` with a 12-hour expiration time.

---

## Metadata-Aware Prompting

The pipeline injects dataset and resource context into PII reflection and Non-PII classification prompts (`FR-SDD-045`, `FR-SDD-046`).

### Supported Metadata Context

- **Dataset Level**: `dataset_title`, `dataset_description`, `dataset_source`, `dataset_location`, `organization_title`.
- **Resource Level**: `resource_name`, `resource_description`.

### Processing Rules

- **Description Truncation (`FR-SDD-048`)**: `dataset_description` and `resource_description` strings exceeding 1,000 characters are truncated at 1,000 characters.
- **Location Omission (`FR-SDD-049`)**: If `dataset_location` contains more than 5 comma-separated country locations, it is omitted (`None`) to reduce prompt noise.
- **CKAN API Optimization (`FR-SDD-054`)**: When fetching metadata, the pipeline calls `package_show` first and extracts nested resource details from the package payload, avoiding redundant `resource_show` calls.

---

## Configuration Reference & Security

### Environment Variables

```bash
# ============================================================================
# OPENAI / AZURE CONFIGURATION
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
# HTTP & NETWORK SECURITY
# ============================================================================
SDD_USER_AGENT=HDXINTERNAL:SDDPipeline/1.1.0

# ============================================================================
# WORKER & REDIS CONFIGURATION
# ============================================================================
WORKER_ENABLED=true
REDIS_STREAM_STREAM_NAME=hdx_event_stream
REDIS_STREAM_GROUP_NAME=hdx_sdd_group
REDIS_STREAM_CONSUMER_NAME=hdx_sdd_consumer_1
REDIS_STREAM_HOST=redis
REDIS_STREAM_PORT=6379
REDIS_STREAM_DB=7

# ============================================================================
# SLACK NOTIFICATIONS
# ============================================================================
HDX_SDD_SLACK_CHANNEL=topic-sensitive-data-alerts
HDX_SDD_SLACK_ACCESS_TOKEN=xoxb-your-token
```

### Domain Authorization & Security

- **Custom User-Agent (`FR-SDD-014`)**: All outbound HTTP requests include `SDD_USER_AGENT` (`HDXINTERNAL:SDDPipeline/{version}`).
- **Domain Token Security (`FR-SDD-015`)**: Downloader logic strips `Authorization` headers on file downloads unless the destination host matches the authorized HDX domain host.

---

## Error Handling & Notifications

1. **Structured Exception Fallbacks**: Pipeline evaluation steps trap exceptions at column, sheet, and use-case levels, populating diagnostic details into `explanation` fields while falling back to safe default sensitivity ratings (`SEVERE_SENSITIVE`).
2. **Slack Alerts (`FR-SDD-050`, `FR-SDD-051`)**: Critical runtime failures generate formatted Slack alerts sent to the configured alerts channel. Slack API failures are logged and suppressed to avoid crashing processing loops.
3. **Structured Diagnostic Logging (`FR-SDD-042`)**: Whenever classification returns `UNDETERMINED`, the system logs diagnostic context including raw LLM response payloads.

---

## Performance Considerations

- **Random Chunked Sampling**: Limits data frame parsing to representative 5-sample slices per column loaded across incremental chunks, optimizing memory footprint.
- **Dynamic Output Tokens**: Scales completion limits efficiently to prevent context overflow while ensuring complete non-PII rationale generation.
- **Redis ISP Caching**: Eliminates repeated Google Sheets / disk reads across high-throughput worker event streams.
