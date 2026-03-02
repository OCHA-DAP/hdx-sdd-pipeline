# HDX Sensitive Data Detection (SDD) Pipeline

A production-ready, clean architecture pipeline for detecting and classifying sensitive data in humanitarian datasets using Azure OpenAI.

[![Tests](https://img.shields.io/badge/tests-passing-success)](tests/)
[![Coverage](https://img.shields.io/badge/coverage-97%25-brightgreen)](htmlcov/)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

## 🎯 Overview

## 🚀 Quick Start

### Installation

This project uses **uv** for dependency management:

```bash
# Clone and install dependencies
git clone https://github.com/OCHA-DAP/hdx-sdd-pipeline.git
cd hdx-sdd-pipeline
uv sync

# Or with pip (legacy)
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

### Running the Pipeline

#### Option 1: Redis Event Processing (Production)

```bash
# Run the main event processor
uv run python main.py
```

#### Option 2: FastAPI Web Service

```bash
# Start the FastAPI server
uv run uvicorn app.main_fastapi:app --host 127.0.0.1 --port 8000 --reload
```

#### Option 3: Direct Processing

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

### Dashboard

Access the web dashboard at `http://localhost:3000` (when running):

```bash
cd dashboard/frontend
npm install
npm run dev
```

See [`scripts/tutorial.py`](scripts/tutorial.py) for detailed examples.

## 🏗️ Architecture

The project follows **Clean Architecture**:

- **`src/domain`**: Entities (`SheetReport`, `Column`) and business logic.
- **`src/application`**: Use cases (`ProcessDatasetUseCase`) and interfaces.
- **`src/infrastructure`**: Implementations (Azure OpenAI, Local Storage).
- **`src/shared`**: Utilities and prompts.
- **`app/`**: FastAPI web service layer.
- **`dashboard/`**: Next.js frontend dashboard.

## 🧪 Development

### Code Quality

```bash
# Lint and format
uv run ruff check .
uv run ruff format .

# Run tests
uv run pytest

# Run with coverage
uv run pytest --cov=src --cov-report=html
```

### Environment Setup

```bash
# Development dependencies
uv sync --group dev

# Environment variables
cp .env.example .env
# Edit .env with your configuration
```

## 🔧 Scripts

- **Batch Processing**: [`scripts/BATCH_PROCESSING_GUIDE.md`](scripts/BATCH_PROCESSING_GUIDE.md) - Guide for running models on multiple datasets.
- **Event Processor**: [`src/event_processor.py`](src/event_processor.py) - Main entry point for Redis stream processing.
- **Tutorial**: [`scripts/tutorial.py`](scripts/tutorial.py) - Step-by-step usage examples.

## 🐳 Docker

```bash
# Build and run with Docker Compose
docker-compose up --build
```

## 📊 Monitoring

The pipeline includes:

- Structured JSON logging
- Slack integration for alerts
- Redis stream event processing
- Web dashboard for monitoring results
