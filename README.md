# HDX Sensitive Data Detection (SDD) Pipeline

A production-ready, clean architecture pipeline for detecting and classifying sensitive data in humanitarian datasets using OpenAI and Azure OpenAI language models.

[![Tests](https://img.shields.io/badge/tests-passing-success)](tests/)
[![Coverage](https://img.shields.io/badge/coverage-90%25-brightgreen)](htmlcov/)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

## 🎯 Overview

The HDX Sensitive Data Detection (SDD) Pipeline is designed to process humanitarian datasets (CSV, Excel) published on the Humanitarian Data Exchange (HDX). It evaluates data sensitivity using a multi-stage LLM approach combined with country-specific Information Sensitivity Protocols (ISP):

1. **Smart Data Loading & Preprocessing**: Efficient chunked loading, header detection, numeric normalization, and random sample extraction (fixed seed `42`).
2. **README / Metadata Scanning**: Identifies non-standard documentation sheets and extracts personal data while filtering out organization-level functional mailboxes.
3. **Personal Data (PII) Detection (Column-Level)**: Classifies column entity types using sample-value verification, area code heuristics, and geo-coordinate detection.
4. **Personal Data Sensitivity Reflection (Table-Level)**: Evaluates whether detected personal data entities present genuine re-identification risk in context, utilizing metadata-aware prompt context.
5. **Non-Personal Data Classification (Table-Level)**: Analyzes dataset sensitivity against country-specific ISP rules or default humanitarian guidelines with automatic multi-location fallback.
6. **Hierarchical Risk Scoring**: Computes standardized risk levels (0–3) at column, sheet, and resource levels using maximum risk propagation.
7. **Dynamic Prompt Management**: Sourced dynamically from Google Sheets / Excel workbooks with local Jinja template fallbacks and Redis caching.

## 🚀 Quick Start

### Installation

This project uses **uv** for fast, deterministic dependency management:

```bash
# Clone and install dependencies
git clone https://github.com/OCHA-DAP/hdx-sdd-pipeline.git
cd hdx-sdd-pipeline
uv sync

# Or install development dependencies
uv sync --all-extras
```

### Configuration

Copy `.env.example` to `.env` and configure your credentials:

```bash
cp .env.example .env
```

Key environment variables:

| Variable | Description | Default |
| --- | --- | --- |
| `OPENAI_ENDPOINT` / `AZURE_OPENAI_ENDPOINT` | Endpoint URL for OpenAI / Azure OpenAI | — |
| `OPENAI_API_KEY` / `AZURE_OPENAI_API_KEY` | API Key for model completions | — |
| `PII_DETECT_MODEL` | Model for column-level PII detection | `pii-detect-v1` |
| `PII_REFLECT_MODEL` | Model for table-level PII reflection | `pii-reflect-v1` |
| `NON_PII_DETECT_MODEL` | Model for non-PII ISP classification | `non-pii-detect-v1` |
| `README_SCAN_MODEL` | Model for README scanning | `readme-scan-v1` |
| `GOOGLE_SHEET_URL` | Central Google Sheet URL for prompts and ISP rules | *HDX Central Spreadsheet* |
| `GOOGLE_SHEETS_PRIVATE_KEY` | Service account private key for Google Sheets | — |
| `GOOGLE_SHEETS_CLIENT_EMAIL` | Service account client email for Google Sheets | — |
| `ISP_STRATEGY` | ISP source strategy (`google_sheets` or `local`) | `google_sheets` |
| `WORKER_ENABLED` | Enable Redis event stream worker mode | `true` |
| `HDX_URL` / `HDX_KEY` | HDX CKAN endpoint and API token | — |
| `HDX_SDD_SLACK_ACCESS_TOKEN` | Slack Bot Token for alert notifications | — |

### Running the Pipeline

#### Option 1: Redis Event Processing (Production Worker)

```bash
# Run the main event processor
uv run python main.py
```

#### Option 2: FastAPI Web Service & Dashboard

```bash
# 1. Start the FastAPI backend
uv run uvicorn app.main_fastapi:app --host 127.0.0.1 --port 8000 --reload

# 2. In another terminal, start the Next.js frontend
cd dashboard/frontend
npm install
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) to access the interactive web dashboard.

#### Option 3: Direct Python Invocation

```python
from src.infrastructure.pipeline_factory import PipelineFactory
from config.config import get_config

# 1. Initialize configuration and pipeline
config = get_config()
pipeline = PipelineFactory(config).create_pipeline()

# 2. Execute pipeline on a dataset
reports = pipeline.execute(
    source="path/to/data.xlsx",
    resource_id="dataset-123",
    is_url=False
)

# 3. Inspect results
for report in reports:
    print(f"Sheet: {report.sheet_name}")
    print(f"  Sensitive: {report.is_sensitive()}")
    print(f"  PD Risk Level: {report.personal_data_risk_level}")
    print(f"  Non-PD Risk Level: {report.non_personal_data_risk_level}")
```

## 🏗️ Architecture

The pipeline follows **Clean Architecture** principles:

- **`src/domain`**: Core entities (`SheetReport`, `Column`, `PersonalDataClassification`, `NonPIIClassification`), value objects (`PIIEntityType`, `SensitivityLevel`), and risk calculation.
- **`src/application`**: Use cases (`ProcessDatasetUseCase`) and interfaces (`IISPStrategy`).
- **`src/infrastructure`**: Concrete implementations (OpenAI / Azure provider, smart data loader, Google Sheets & Local JSON ISP strategies, dynamic prompt strategy).
- **`src/shared`**: Utilities (`PromptManager`, `ISPRetriever`, `CKANClient`, JSON serializer).
- **`src/prompts/`**: Versioned Jinja2 prompt templates for detection, reflection, classification, and README scanning.
- **`app/`**: FastAPI backend service.
- **`dashboard/`**: Next.js monitoring and evaluation dashboard.

## 🔧 CLI & Utility Scripts

- **Inspect Prompts**: Preview active prompts rendered from Google Sheets or local templates:
  ```bash
  uv run python print_prompts.py
  ```
- **Batch Evaluation**: Run evaluations across test datasets with a specified model:
  ```bash
  uv run python batch_process_model.py --model gpt-4.1-mini --skip-existing
  ```
- **Tutorial & Examples**: [`scripts/tutorial.py`](scripts/tutorial.py) - Step-by-step programmatic examples.

## 🧪 Development & Quality

```bash
# Lint and format
uv run ruff check .
uv run ruff format .

# Run test suite
uv run pytest

# Generate test coverage report
uv run pytest --cov=src --cov-report=html
```

## 🐳 Docker Deployment

```bash
# Build and start services via Docker Compose
docker-compose up --build
```

