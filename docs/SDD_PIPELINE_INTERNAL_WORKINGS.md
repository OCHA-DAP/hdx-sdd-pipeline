# HDX Sensitive Data Detection (SDD) Pipeline - Internal Workings

**Version:** 2.0  
**Last Updated:** September 17, 2026  
**Author:** HDX SDD Team

---

## Table of Contents

1. [Overview](#overview)
2. [Pipeline Architecture](#pipeline-architecture)
3. [End-to-End Processing Flow](#end-to-end-processing-flow)
4. [Detailed Evaluation Stages & Prompts](#detailed-evaluation-stages--prompts)
5. [Dynamic Prompt Management (Google Sheets & Local Fallbacks)](#dynamic-prompt-management-google-sheets--local-fallbacks)
6. [Information Sensitivity Protocols (ISP) System](#information-sensitivity-protocols-isp-system)
7. [LLM Integration & Model Configurations](#llm-integration--model-configurations)
8. [Risk Level Scoring & Hierarchical Propagation](#risk-level-scoring--hierarchical-propagation)
9. [Metadata-Aware Prompting](#metadata-aware-prompting)
10. [Data Loading & Preprocessing](#data-loading--preprocessing)
11. [Data Structures & Entities](#data-structures--entities)
12. [Error Handling & Fallback Policies](#error-handling--fallback-policies)
13. [Configuration Reference](#configuration-reference)

---

## Overview

The HDX Sensitive Data Detection (SDD) Pipeline is a production-grade, clean architecture system that automatically evaluates humanitarian datasets (CSV, Excel) published on the Humanitarian Data Exchange (HDX). The pipeline classifies data sensitivity using OpenAI and Azure OpenAI models combined with country-specific Information Sensitivity Protocols (ISP):

- **Personal Data (PII) Detection**: Identifying personal identifiers (names, emails, phone numbers, addresses, demographic attributes, coordinates, etc.).
- **Personal Data Sensitivity Reflection**: Assessing table-level re-identification risk to determine if detected attributes genuinely threaten individual safety or privacy.
- **Non-Personal Data Sensitivity Classification**: Classifying dataset sensitivity according to contextual country ISP guidelines or default humanitarian protocols.
- **README / Metadata Scanning**: Identifying and scanning documentation sheets while filtering out non-sensitive operational/functional mailboxes.
- **Hierarchical Risk Scoring**: Computing standardized risk scores (0–3) with maximum risk propagation across sheets and dataset resources.

### Key Architectural Highlights

- **Clean Architecture**: Domain entities, application use cases, infrastructure adapters, and shared utilities are completely decoupled.
- **Dynamic Prompt System**: Real-time prompt instructions and entity definitions loaded from Google Sheets (or local Excel fallback) with Redis caching.
- **Dynamic ISP Strategy**: Flexible ISP rule retrieval from Google Sheets or local JSON, including multi-location fallback and rule status filtering.
- **Deterministic Execution**: Fixed random seed (`seed=42`) across model invocations ensures reproducible evaluations.
- **Reasoning Model Support**: Automated parameter adaptation, reasoning effort control (`low`/`medium`), and token buffer expansion for GPT-5 / GPT-5.4 models.
- **Production Resilience**: Multi-tier error fallbacks (safe promotion to sensitive on failure), structured logging, and Slack alerts.

---

## Pipeline Architecture

The pipeline follows **Clean Architecture** principles across four layers:

```
┌─────────────────────────────────────────────────────────────┐
│                       Domain Layer                          │
│  • Entities: SheetReport, Column, NonPIIClassification,     │
│              PersonalDataClassification                     │
│  • Value Objects: PIIEntityType, SensitivityLevel           │
│  • Domain Exceptions & Risk Level Calculations              │
└─────────────────────────────────────────────────────────────┘
                               ▲
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
│  • Use Cases: ProcessDatasetUseCase                         │
│  • Interfaces: IISPStrategy, ILLMProvider protocols         │
└─────────────────────────────────────────────────────────────┘
                               ▲
┌─────────────────────────────────────────────────────────────┐
│                   Infrastructure Layer                      │
│  • Data Loader: SmartDataLoader (chunked, normalized)       │
│  • LLM Provider: OpenAIProvider (OpenAI / Azure / DeepSeek) │
│  • Strategies: GoogleSheetsISPStrategy, LocalJSONISPStrategy│
│  • Dynamic Prompts: SpreadsheetPromptStrategy               │
│  • Pipeline Factory: PipelineFactory                        │
└─────────────────────────────────────────────────────────────┘
                               ▲
┌─────────────────────────────────────────────────────────────┐
│                       Shared Layer                          │
│  • Utilities: PromptManager, ISPRetriever, CKANClient       │
│  • JSON Serialization & Logging Configuration               │
└─────────────────────────────────────────────────────────────┘
```

---

## End-to-End Processing Flow

```
┌────────────────────────────────────────────────────────────────────────┐
│ 1. DATA INGESTION & SMART SAMPLING                                     │
│    • Ingest CSV, XLS, XLSX from local path or remote URL               │
│    • Parse multi-row headers, filter empty columns, normalize numbers  │
│    • Iterative chunk loading (100 -> 100k rows)                        │
│    • Random sampling (5 unique non-null values per column, seed=42)   │
└────────────────────────────────────────────────────────────────────────┘
                                   ↓
┌────────────────────────────────────────────────────────────────────────┐
│ 2. SHEET CLASSIFICATION                                                │
│    • Detect README / Metadata / Instructions sheets                    │
│    • If README: Run README scan LLM (v2) (filter out org emails)       │
│    • If Data Sheet: Initialize SheetReport & Column entities           │
└────────────────────────────────────────────────────────────────────────┘
                                   ↓
┌────────────────────────────────────────────────────────────────────────┐
│ 3. COLUMN-LEVEL PERSONAL DATA (PII) DETECTION                          │
│    • Check heuristics (e.g. 'latitude'/'longitude' -> GEO_COORDINATES) │
│    • Render PII Detection prompt (v4 / Google Sheets)                  │
│    • Verify sample values for PERSON_NAME, EMAIL_ADDRESS, PHONE_NUMBER │
│    • Disregard geographic/FAOSTAT short area codes as PHONE_NUMBER     │
│    • LLM call with reasoning_effort='low' (max_tokens=8)               │
└────────────────────────────────────────────────────────────────────────┘
                                   ↓
┌────────────────────────────────────────────────────────────────────────┐
│ 4. TABLE-LEVEL PERSONAL DATA SENSITIVITY REFLECTION                    │
│    • If no PII or only ORGANIZATION_NAME -> Mark NON_SENSITIVE         │
│    • Inject dataset & resource metadata context                        │
│    • 3-Step Evaluation: Unit of analysis -> Direct/Quasi IDs -> Risk   │
│    • Exclude organization/functional email addresses from risk         │
│    • Render PII Reflection prompt (v5 / Google Sheets)                 │
│    • LLM JSON generation with reasoning_effort='medium'                │
│    • Propagate table sensitivity down to identified PII columns        │
└────────────────────────────────────────────────────────────────────────┘
                                   ↓
┌────────────────────────────────────────────────────────────────────────┐
│ 5. TABLE-LEVEL NON-PERSONAL DATA CLASSIFICATION (ISP)                  │
│    • Match ISP rules from package location or resource name            │
│    • If multiple locations in dataset -> fallback to Default ISP       │
│    • Select standard (v5) or default (v4) prompt                       │
│    • Apply administrative level & operational presence guidelines      │
│    • LLM JSON generation with max_tokens=max(2000, n_columns * 5)      │
└────────────────────────────────────────────────────────────────────────┘
                                   ↓
┌────────────────────────────────────────────────────────────────────────┐
│ 6. HIERARCHICAL RISK LEVEL PROPAGATION & OUTPUT                        │
│    • Calculate Sheet personal_data_risk_level (0, 2, 3)                │
│    • Calculate Sheet non_personal_data_risk_level (0, 1, 2, 3)         │
│    • Propagate maximum risk level to resource/file report              │
│    • Persist JSON report locally or update CKAN resource metadata      │
│    • Dispatch Slack notifications on errors or processing completion   │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Detailed Evaluation Stages & Prompts

### Stage 1: README Sheet Scan

- **Objective**: Detect sensitive personal information accidentally embedded in documentation, cover sheets, or metadata notes.
- **Key Behavior**:
  - Excludes organization-level / generic functional mailboxes (e.g., `info@un.org`, `data@who.int`, `contact@ocha.org`).
  - Flags only individual personal data (names, private emails, personal phone numbers).
- **Latest Template**: `src/prompts/readme_scan/v2.jinja` (Worksheet: `readme`)
- **Model Setting**: `README_SCAN_MODEL` (e.g. `gpt-4.1-nano`, reasoning `low`)

### Stage 2: Personal Data (PII) Entity Detection (Column-Level)

- **Objective**: Identify which columns contain personal data entity types.
- **Key Rules & Safeguards**:
  - **Sample-Value Verification**: Detection of `PERSON_NAME`, `EMAIL_ADDRESS`, and `PHONE_NUMBER` must be confirmed by actual values in the samples, never on column name alone.
  - **Area Code Heuristics**: Numeric country/region codes (e.g., FAOSTAT area code `206`) are strictly separated from phone numbers.
  - **Geo Coordinates**: Heuristic detection maps columns named `latitude` or `longitude` directly to `GEO_COORDINATES`.
  - **Universal Reflection Routing**: All PII entity types are routed through table-level reflection to evaluate contextual re-identification risk.
- **Supported Entity Types**:
  - `PERSON_NAME`, `EMAIL_ADDRESS`, `PHONE_NUMBER`, `AGE`, `ORGANIZATION_NAME`, `LOCATION`, `ADDRESS`, `HOME_ADDRESS`, `DISABILITY`, `GEO_COORDINATES`, `ID_NUMBER`, `PASSPORT_NUMBER`, `SOCIAL_SECURITY_NUMBER`, `CREDIT_CARD`, `BANK_ACCOUNT`, `DATE_OF_BIRTH`, `GENDER`, `IP_ADDRESS`, `URL`, `NONE`, `UNKNOWN`.
- **Latest Template**: `src/prompts/pii_detection/v4.jinja` (Worksheet: `personal_data_detection`)
- **Model Setting**: `PII_DETECT_MODEL` (e.g. `gpt-4.1-nano`, max_tokens=8, reasoning `low`)

### Stage 3: Personal Data Sensitivity Reflection (Table-Level)

- **Objective**: Evaluate whether personal data detected in the table can reasonably be used to identify individual human beings.
- **Evaluation Framework (3-Step Evaluation)**:
  1. **Determine Unit of Analysis**: Person, household, facility/site, or administrative aggregate.
  2. **Identify Individual-Level Direct & Quasi Identifiers**: Isolate individual person data from aggregate statistics.
  3. **Assess Re-Identification Risk**:
     - `NON_SENSITIVE`: Aggregate data, general demographic statistics, or only organization names/functional emails.
     - `HIGH_SENSITIVE`: Microdata with indirect identifiers or quasi-identifiers that meaningfully increase re-identification risk.
     - `SEVERE_SENSITIVE`: Microdata with direct personal identifiers (e.g. full person names, personal phone numbers, direct individual IDs).
- **Organizational Email Exclusion**: Shared team mailboxes and organizational contacts are explicitly excluded from sensitivity scoring.
- **Latest Template**: `src/prompts/pii_reflection/v5.jinja` (Worksheet: `personal_data_reflection`)
- **Model Setting**: `PII_REFLECT_MODEL` (e.g. `gpt-4.1-nano` or `gpt-5.4-mini`, reasoning `medium`)

### Stage 4: Non-Personal Data Classification (Table-Level)

- **Objective**: Determine dataset sensitivity against contextual Information Sensitivity Protocols (ISP).
- **Key Guidelines**:
  - **Administrative Levels**: Leverages geographical knowledge (e.g., understanding that "Locality" in Sudan is ADM2).
  - **Operational Presence vs. Contact Lists**: 3W/4W/5W datasets showing where organizations operate are operational data, not contact lists.
  - **Aggregate Populations**: Aggregate population counts (displaced persons, IDPs) at ADM2 or higher are general statistics, not raw needs assessments.
  - **Output Token Budget**: Minimum 2000 output tokens (or `n_columns * 5` if larger) to ensure complete structured JSON responses.
- **Templates**:
  - Standard Country ISP: `src/prompts/non_pii_classification/v5.jinja` (Worksheet: `non_personal_data_classificatio`)
  - Default Humanitarian ISP: `src/prompts/non_pii_classification/default/v4.jinja` (Worksheet: `non_personal_data_default_class`)
- **Model Setting**: `NON_PII_DETECT_MODEL` (e.g. `gpt-4.1-mini`, reasoning `medium`)

---

## Dynamic Prompt Management (Google Sheets & Local Fallbacks)

The pipeline integrates a dynamic prompt management system (`SpreadsheetPromptStrategy`) that allows non-engineering stakeholders to update prompt instructions, rules, and schemas in real time without redeploying code.

```
┌─────────────────────────────────────────────────────────────┐
│                  Google Spreadsheet                         │
│  (Worksheets: personal_data_detection,                      │
│   personal_data_reflection, non_personal_data_classificatio,│
│   non_personal_data_default_class, readme)                  │
└─────────────────────────────────────────────────────────────┘
                               │
                ┌──────────────┴──────────────┐
                ▼                             ▼
   [Live Google Sheets Fetch]     [Local Excel Fallback]
   (Authenticated via gspread)    (src/prompts/prompts_dev.xlsx)
                │                             │
                └──────────────┬──────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                 SpreadsheetPromptStrategy                   │
│  1. Filter rows where 'enabled' is True                     │
│  2. Order strictly by numerical 'section_id'                │
│  3. Cache compiled template & rules in Redis (12h TTL)      │
└─────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                       PromptManager                         │
│  • Injects active spreadsheet rules into Jinja context      │
│  • Auto-detects latest Jinja template fallback on disk      │
└─────────────────────────────────────────────────────────────┘
```

### Prompt Preview CLI Tool

Inspect and debug active prompts across all categories:

```bash
uv run python print_prompts.py
```

---

## Information Sensitivity Protocols (ISP) System

### ISP Strategy Architecture (`IISPStrategy`)

ISP rules are retrieved via the `IISPStrategy` protocol:

1. **`GoogleSheetsISPStrategy`** (Default in production):
   - Reads the central Google Spreadsheet worksheet `"Data & Information Types Dataset"`.
   - Filters out disabled rows (`Enabled == 'no'`) and inactive statuses (`ISP Status` is `'under development'` or `'not used'`).
   - Maps country ISO3 codes and populates both modern `sensitivity_rules` dictionaries and legacy keys.
2. **`LocalJSONISPStrategy`** (Local/test fallback):
   - Reads rules directly from `data/isps.json`.
3. **Redis Caching**:
   - ISP rules are cached in Redis under key `isp_rules_cache` with a 12-hour TTL (`43200` seconds).

### Location Matching & Multi-Country Fallback

When resolving ISP rules for a dataset:

1. **CKAN Package Groups**:
   - If the package contains **exactly one** valid location group, match its ISO3 code against available ISPs.
   - If the package contains **multiple location groups** (multi-country dataset), the retriever automatically falls back to the **Default ISP** (`isps['default']`), routing the analysis through the default non-PII classification prompt.
2. **Resource Name Substring**:
   - When package metadata is unavailable (e.g. standalone file processing), searches the entire resource filename for any country ISO3 code substring.
3. **Global Default Fallback**:
   - Uses the general humanitarian sensitivity guidelines.

---

## LLM Integration & Model Configurations

### Unified `OpenAIProvider`

All model communications (Azure OpenAI, OpenAI, DeepSeek) are managed via `OpenAIProvider` using the official `openai` SDK.

### Deterministic Execution (`seed=42`)

All completion requests include a fixed `seed=42` parameter to ensure consistent and reproducible outputs across runs.

### Reasoning Models (GPT-5 / GPT-5.4)

When a model name contains `gpt-5`, the provider activates reasoning model adaptations:

- **Reasoning Effort Allocation**:
  - Column PII Detection & README Scan: `reasoning_effort='low'`
  - Table PII Reflection & Non-PII Classification: `reasoning_effort='medium'`
- **Parameter Cleansing**:
  - When reasoning is active (`reasoning_effort != 'none'`), incompatible parameters (`temperature`, `top_p`) are automatically stripped from the request payload.
- **Safety Token Buffer**:
  - `max_completion_tokens` is expanded with a safety buffer of `max_tokens + 8192` to prevent token starvation during internal reasoning.

---

## Risk Level Scoring & Hierarchical Propagation

The pipeline assigns numerical risk scores (0–3) that propagate upwards from sheet-level evaluations to the entire resource report:

### Scoring Rules

| Risk Level | Score | Personal Data (PD) Criteria | Non-Personal Data (NPD) Criteria |
| --- | :---: | --- | --- |
| **None / Non-Sensitive** | `0` | `NON_SENSITIVE`, `UNDETERMINED` | `NON_SENSITIVE`, `LOW`, `UNDETERMINED` |
| **Medium Sensitive** | `1` | — | `MEDIUM_SENSITIVE` |
| **High Sensitive** | `2` | `HIGH_SENSITIVE` | `HIGH_SENSITIVE` |
| **Severe Sensitive** | `3` | `SEVERE_SENSITIVE` | `SEVERE_SENSITIVE` |

### Maximum Risk Propagation Formula

$$\text{Sheet Risk} = \max(\text{PD Score}, \text{NPD Score})$$
$$\text{Resource Risk} = \max_{\text{all sheets}}(\text{Sheet Risk})$$

```json
{
  "resource_id": "dataset-123",
  "sensitive": true,
  "sensitivity_level": 3,
  "sdd_report": [
    {
      "sheet_name": "Beneficiaries",
      "personal_data_sensitive": true,
      "non_personal_data_sensitive": false,
      "personal_data_risk_level": 3,
      "non_personal_data_risk_level": 0
    }
  ]
}
```

---

## Metadata-Aware Prompting

Dataset and resource-level metadata provide critical context for sensitivity classification:

- **Injected Metadata Fields**:
  - `dataset_title`, `dataset_description` (truncated to 1000 chars), `dataset_source`, `dataset_location` (omitted if >5 locations), `organization_title`, `resource_name`, `resource_description` (truncated to 1000 chars).
- **Graceful Omission**: If metadata fields are missing or empty, they are cleanly omitted from the rendered prompt without leaving placeholders.

---

## Data Loading & Preprocessing

The `SmartDataLoader` handles raw data ingestion:

- **Chunked Loading**: Loads data in progressive chunks (`100`, `1,000`, `10,000`, `25,000`, `50,000`, `100,000` rows) until all columns achieve at least 5 unique non-null values.
- **Random Sampling**: Selects 5 unique non-null sample values per column using fixed random seed `42`.
- **Numeric Normalization**: Normalizes string-formatted integers, floats, comma separators, and Unicode spaces (NBSP, NNBSP).
- **Header Detection**: Automatically detects and concatenates multi-row headers.

---

## Data Structures & Entities

### `SheetReport` (Aggregate Root)

```python
@dataclass
class SheetReport:
    resource_id: Optional[str] = None
    file_name: str = ""
    sheet_name: str = "sheet1"
    processing_timestamp: datetime = field(default_factory=datetime.now)
    processing_success: bool = True
    n_records: int = 0
    n_columns: int = 0
    completion_tokens: int = 0
    prompt_tokens: int = 0
    personal_data_sensitive: bool = False
    non_personal_data_sensitive: bool = False
    personal_data_risk_level: int = 0
    non_personal_data_risk_level: int = 0
    columns: List[Column] = field(default_factory=list)
    personal_data_classification: PersonalDataClassification = field(default_factory=PersonalDataClassification)
    non_pii_classification: NonPIIClassification = field(default_factory=NonPIIClassification)
    is_readme: bool = False
    readme_report: Optional[Dict[str, Any]] = None
```

---

## Error Handling & Fallback Policies

1. **Non-Personal Data Failure**: If non-PII classification fails or returns `UNDETERMINED`, it is promoted to `SEVERE_SENSITIVE` as a safety precaution.
2. **Personal Data Reflection Failure**: If reflection fails, sheet-level `personal_data_sensitive` is set to `True` with `HIGH_SENSITIVE`.
3. **Column PII Failure**: If column entity detection fails, the column is marked as `UNKNOWN` and `sensitive=True`.
4. **Slack Delivery Failures**: Slack API errors are logged but suppressed to ensure the pipeline never blocks or crashes.

---

## Configuration Reference

### Environment Variables Summary

```bash
# OpenAI / Azure Configuration
OPENAI_ENDPOINT=https://your-endpoint.openai.azure.com/openai/v1
OPENAI_API_KEY=your_api_key_here

# Model Selection
PII_DETECT_MODEL=gpt-4.1-nano
PII_REFLECT_MODEL=gpt-4.1-nano
NON_PII_DETECT_MODEL=gpt-4.1-mini
README_SCAN_MODEL=gpt-4.1-nano

# Google Sheets Configuration
GOOGLE_SHEET_URL=https://docs.google.com/spreadsheets/d/.../edit
GOOGLE_SHEETS_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
GOOGLE_SHEETS_CLIENT_EMAIL=service-account@project.iam.gserviceaccount.com
GOOGLE_SHEETS_TOKEN_URI=https://oauth2.googleapis.com/token

# Strategy Toggles
ISP_STRATEGY=google_sheets             # 'google_sheets' or 'local'
PII_PROMPT_STRATEGY=google_sheets      # 'google_sheets' or 'local'
PII_REFLECTION_PROMPT_STRATEGY=google_sheets

# Redis & Worker Configuration
WORKER_ENABLED=true
REDIS_STREAM_HOST=redis
REDIS_STREAM_PORT=6379
REDIS_STREAM_DB=7

# CKAN Integration
HDX_URL=https://data.humdata.org
HDX_KEY=your_hdx_api_key

# Slack Alerts
HDX_SDD_SLACK_CHANNEL=topic-sensitive-data-alerts
HDX_SDD_SLACK_ACCESS_TOKEN=xoxb-your-token
```
