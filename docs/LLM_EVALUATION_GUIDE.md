# LLM Evaluation Guide: Complete Setup and Execution

This guide provides step-by-step instructions for generating LLM evaluation reports for the HDX SSD Pipeline, starting from a fresh installation. This documentation is designed for UN development team members who need to evaluate different language models for sensitive data detection.

## Overview

The HDX SSD Pipeline evaluates LLMs on their ability to:

- **Detect PII (Personally Identifiable Information)** in table columns
- **Assess PII sensitivity levels** (e.g., NON_SENSITIVE, MODERATE_SENSITIVE, HIGH_SENSITIVE, SEVERE_SENSITIVE)
- **Classify non-PII sensitivity** based on Information Sensitivity Protocol (ISP) rules

You can evaluate models using two methods:

1. **Batch Processing**: Run evaluations programmatically via Python scripts
2. **Dashboard**: Use the Next.js web interface for interactive evaluation and visualization

---

## Prerequisites

### System Requirements

- **Operating System**: macOS, Linux, or Windows with WSL
- **Python**: Version 3.10 or higher
- **Node.js**: Version 18 or higher (for dashboard)
- **npm**: Version 8 or higher (for dashboard)
- **Git**: For cloning the repository
- **Redis**: For event-driven pipeline (optional for evaluation)

### Required Access

- **Azure OpenAI API Key**: Access to Azure OpenAI services
- **HDX API Key**: For accessing HDX data (optional for evaluation)

---

## Initial Setup

### Step 1: Clone the Repository

```bash
# Clone the repository
git clone <repository-url>
cd hdx-ssd-pipeline
```

### Step 2: Set Up Python Environment

```bash
# Create a virtual environment
python3 -m venv .venv

# Activate the virtual environment
# On macOS/Linux:
source .venv/bin/activate
# On Windows:
# .venv\Scripts\activate

# Install Python dependencies using uv (recommended) or pip
# Option 1: Using uv (faster)
pip install uv
uv sync

# Option 2: Using pip
pip install -e .
```

### Step 3: Configure Environment Variables

Create a `.env` file in the project root:

```bash
# Copy the example environment file
cp .env.example .env

# Edit the .env file with your credentials
nano .env
```

Add the following configuration to `.env`:

```bash
# Azure OpenAI Configuration
AZURE_OPENAI_API_KEY="your-azure-openai-api-key"
AZURE_OPENAI_ENDPOINT="https://your-resource.cognitiveservices.azure.com/"

# Model Configuration
PII_DETECT_MODEL="gpt-4.1-nano"
PII_REFLECT_MODEL="gpt-4.1-nano"
NON_PII_DETECT_MODEL="gpt-4.1-nano"
README_SCAN_MODEL="gpt-4.1-nano"

# Local Evaluation Settings
LOCAL="true"
DATASETS_DIR="./research/data"
REPORTS_DIR="./research/results/test_results"

# HDX Configuration (optional for evaluation)
HDX_KEY="your-hdx-api-key"
HDX_URL="https://data.humdata.org/"
```

### Step 4: Prepare Test Datasets

The evaluation requires:

1. **Test datasets** (CSV/Excel files) in `research/data/`
2. **Ground truth annotations** in `research/results/test_results/groundtruth/`

```bash
# Ensure directories exist
mkdir -p research/data
mkdir -p research/results/test_results/groundtruth

# Add your test datasets to research/data/
# Example: Copy sample datasets
# cp /path/to/your/datasets/*.xlsx research/data/
```

**Ground Truth Format**: Each ground truth file should be a JSON file with the same name as the dataset (e.g., `dataset.xlsx.json`) containing manually annotated predictions for comparison.

---

## Step 1: Batch Processing Evaluation

Batch processing allows you to evaluate multiple models programmatically and generate comprehensive statistics.

```bash
# Process all datasets with a specific model
uv run python batch_process_model.py --model gpt-4.1-nano

# Skip already-processed datasets
uv run python batch_process_model.py --model gpt-4.1-nano --skip-existing

# Limit number of datasets (for testing)
uv run python batch_process_model.py --model gpt-4.1-nano --limit 10
```

## Step 2: Dashboard-Based Evaluation

The dashboard provides an interactive web interface for running evaluations and visualizing results.

### Step 1: Start the Backend API

The backend is a FastAPI application that serves the evaluation API.

```bash
# Navigate to the project root
cd /path/to/hdx-ssd-pipeline

# Ensure Python environment is activated
source .venv/bin/activate

# Start the FastAPI backend
python -m uvicorn app.main_fastapi:app --reload --host 127.0.0.1 --port 8000
```

**Expected Output:**

```
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:     Started reloader process
INFO:     Started server process
INFO:     Waiting for application startup.
INFO:     Application startup complete.
```

The API will be available at: `http://localhost:8000`

### Step 2: Start the Frontend Dashboard

Open a **new terminal window** and navigate to the dashboard frontend:

```bash
# Navigate to the frontend directory
cd /path/to/hdx-ssd-pipeline/dashboard/frontend

# Install Node.js dependencies (first time only)
npm install

# Start the Next.js development server
npm run dev
```

**Expected Output:**

```
> frontend@0.1.0 dev
> next dev

   ▲ Next.js 16.1.2
   - Local:        http://localhost:3000
   - Environments: .env

 ✓ Starting...
 ✓ Ready in 2.3s
```

The dashboard will be available at: `http://localhost:3000`

### Step 3: Upload Datasets via Dashboard

1. **Open the dashboard** in your browser: `http://localhost:3000`

2. **Navigate to the "Upload Dataset" tab**

3. **Upload a dataset**:
   - Click "Choose File" and select a CSV or Excel file
   - The file will be uploaded to `research/data/`

4. **Upload ground truth** (optional):
   - Navigate to the "Ground Truth" tab
   - Upload the corresponding ground truth JSON file
   - The file will be saved to `research/results/test_results/groundtruth/`

### Step 4: Run Evaluation via Dashboard

1. **Navigate to the "Evaluate" tab**

2. **Select a dataset** from the dropdown menu

3. **Select a model** to evaluate:
   - `gpt-4.1-nano`
   - `gpt-4.1-mini`
   - `gpt-5-nano`
   - `gpt-5-mini`

4. **Click "Run Evaluation"**

5. **Monitor progress**:
   - The dashboard will show real-time progress
   - Processing time depends on dataset size and model

6. **View results**:
   - Results are displayed immediately after completion
   - Navigate to the "Results" tab to see detailed predictions

### Step 5: Compare Models

1. **Navigate to the "Model Comparison" tab**

2. **Select multiple models** to compare

3. **View comparison metrics**:
   - **Overall File-Level Performance**: Accuracy across all files
   - **File-Level PII Performance**: PII detection accuracy
   - **File-Level Non-PII Performance**: Non-PII sensitivity classification
   - **Sheet-Level Performance**: Granular sheet-level metrics
   - **Column-Level Performance**: Individual column predictions

4. **Export results**:
   - Click "Export PDF" to download a comprehensive report
   - Click "Export JSON" to download raw data

### Step 6: View Statistics Dashboard

1. **Navigate to the "Statistics" tab**

2. **View aggregated metrics**:
   - **Accuracy, Precision, Recall, F1 scores** for each model
   - **Cost analysis**: Token usage and estimated costs
   - **Error analysis**: Common misclassifications

3. **Filter by model** or **dataset** for detailed insights

---

## Understanding the Results

### Evaluation Metrics

- **Accuracy**: Percentage of correct predictions
- **Precision**: Of all positive predictions, how many were correct
- **Recall**: Of all actual positives, how many were detected
- **F1 Score**: Harmonic mean of precision and recall

### Sensitivity Levels

**Personal Data (PII)**:

- `true` / `false`: Whether the column contains sensitive personal data

**Non-Personal Data**:

- `NON_SENSITIVE`: Public or aggregated data
- `MODERATE_SENSITIVE`: Disaggregated data without personal identifiers
- `HIGH_SENSITIVE`: Community/household-level data or facility data with coordinates
- `SEVERE_SENSITIVE`: Individual-level data or personal beneficiary information

### Output Files

**Prediction Files** (`research/results/test_results/{MODEL}/dataset.xlsx.json`):

```json
[
  {
    "file_name": "dataset.xlsx",
    "sheet_name": "Sheet1",
    "personal_data_sensitive": true,
    "non_personal_data_sensitive": true,
    "columns": [
      {
        "column_name": "email",
        "personal_data": {
          "entity_type": "EMAIL_ADDRESS",
          "sensitive": true
        }
      }
    ],
    "non_personal_data": {
      "sensitivity": "HIGH_SENSITIVE",
      "explanation": "..."
    }
  }
]
```

**Metrics Files** (`research/results/test_results/{MODEL}_scores.json`):

```json
{
  "pii_columns": {
    "accuracy": 0.95,
    "precision": 0.93,
    "recall": 0.97,
    "f1": 0.95
  },
  "pii_table_level": {
    "accuracy": 0.92,
    ...
  },
  "non_pii_table_level": {
    "accuracy": 0.88,
    ...
  }
}
```

---

## Best Practices

### For Accurate Evaluations

1. **Use consistent ground truth**: Ensure all ground truth files follow the same annotation standards
2. **Test on diverse datasets**: Include various data types, sizes, and sensitivity levels
3. **Run multiple iterations**: Evaluate each model at least 3 times to account for variability
4. **Document assumptions**: Record any special handling or edge cases

### For Efficient Workflows

1. **Use batch processing for large-scale evaluations**: Faster than dashboard for 10+ datasets
2. **Use dashboard for exploratory analysis**: Better for understanding model behavior
3. **Version control results**: Commit evaluation results to track model improvements
4. **Automate repetitive tasks**: Create scripts for common evaluation workflows

---

## Next Steps

After completing evaluations:

1. **Analyze results**: Review metrics and identify model strengths/weaknesses
2. **Compare models**: Use the dashboard to visualize performance differences
3. **Optimize prompts**: Adjust prompts in `prompts/` directory to improve accuracy
4. **Update production**: Deploy the best-performing model to the production pipeline
5. **Document findings**: Create a summary report for stakeholders

---

## Appendix

### Available Models

| Model          | Description         | Use Case                          |
| -------------- | ------------------- | --------------------------------- |
| `gpt-4.1-nano` | Smallest, fastest   | Quick evaluations, cost-sensitive |
| `gpt-4.1-mini` | Balanced            | General-purpose evaluations       |
| `gpt-5-nano`   | Latest nano version | Testing new capabilities          |
| `gpt-5-mini`   | Latest mini version | Best performance                  |

### Directory Structure

```
hdx-ssd-pipeline/
├── research/
│   ├── data/                    # Test datasets
│   └── results/
│       └── test_results/
│           ├── groundtruth/     # Ground truth annotations
│           ├── gpt-4.1-nano/    # Model predictions
│           ├── gpt-4.1-mini/    # Model predictions
│           └── metrics_summary.json
├── dashboard/
│   └── frontend/                # Next.js dashboard
├── app/                         # FastAPI backend
├── classifiers/                 # Classification logic
├── prompts/                     # LLM prompts
└── docs/                        # Documentation
```
