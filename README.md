# HDX Sensitive Data Detection (SDD) Pipeline

A production-ready, clean architecture pipeline for detecting and classifying sensitive data in humanitarian datasets using Azure OpenAI.

[![Tests](https://img.shields.io/badge/tests-passing-success)](tests/)
[![Coverage](https://img.shields.io/badge/coverage-97%25-brightgreen)](htmlcov/)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

## 🎯 Overview

This pipeline automatically analyzes datasets to identify:
- **PII (Personally Identifiable Information)**: Names, emails, phone numbers, etc.
- **Sensitivity**: Whether detected PII or the dataset context represents a risk based on Information Sensitivity Protocols (ISP).

## 🚀 Quick Start

### Installation

```bash
git clone https://github.com/OCHA-DAP/hdx-sdd-pipeline.git
cd hdx-sdd-pipeline
pip install -r requirements.txt
```

### Configuration

Copy `.env.example` to `.env` and configure your Azure OpenAI credentials:

```bash
cp .env.example .env
```

Required variables:
- `AZURE_OPENAI_API_KEY`
- `AZURE_OPENAI_ENDPOINT`
- `PII_DETECT_MODEL`, `PII_REFLECT_MODEL`, `NON_PII_DETECT_MODEL`

### Basic Usage

```python
from src.application.use_cases.process_dataset import ProcessDatasetUseCase
from src.infrastructure.factories import PipelineFactory
from config import get_config

# 1. Initialize
config = get_config()
pipeline = PipelineFactory(config).create_pipeline()

# 2. Process
reports = pipeline.execute(
    source="path/to/data.xlsx",
    resource_id="dataset-123"
)

# 3. Results
for report in reports:
    print(f"Sensitive: {report.is_sensitive()}")
```

See [`scripts/tutorial.py`](scripts/tutorial.py) for detailed examples.

## 🏗️ Architecture

The project follows **Clean Architecture**:

- **`src/domain`**: Entities (`SheetReport`, `Column`) and business logic.
- **`src/application`**: Use cases (`ProcessDatasetUseCase`) and interfaces.
- **`src/infrastructure`**: Implementations (Azure OpenAI, Local Storage).
- **`src/shared`**: Utilities and prompts.

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html
```

## 🔧 Scripts

- **Batch Processing**: [`scripts/BATCH_PROCESSING_GUIDE.md`](scripts/BATCH_PROCESSING_GUIDE.md) - Guide for running models on multiple datasets.
- **Event Processor**: [`src/event_processor.py`](src/event_processor.py) - Main entry point for Redis stream processing.
