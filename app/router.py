from fastapi import APIRouter, HTTPException, UploadFile, File, BackgroundTasks
from pathlib import Path
import shutil
import json
import logging
import asyncio
from typing import Dict, Any, List
from datetime import datetime

# Import from clean architecture
from config.config import Config
from src.infrastructure.factories.pipeline_factory import PipelineFactory
from src.application.use_cases.process_dataset import ProcessDatasetUseCase
from src.infrastructure.storage.data_loader import SmartDataLoader
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

router = APIRouter()

# Configuration
DATASETS_DIR = Path('/Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/research/data')
REPORTS_DIR = Path('/Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/research/results/test_results')
GROUNDTRUTH_DIR = REPORTS_DIR / 'groundtruth2'
ALLOWED_EXTENSIONS = {'.csv', '.xlsx'}

# Available models for batch processing
AVAILABLE_MODELS = ["gpt-4.1-nano", "gpt-4.1-mini", "gpt-4.1", "gpt-5-nano", "gpt-5-mini", "DeepSeek-V3.1"]

# Global variable to track batch processing status
batch_status = {
    "is_running": False,
    "current_model": None,
    "completed_models": [],
    "failed_models": [],
    "started_at": None,
    "progress": 0,
}


def setup_pipeline(model_name: str) -> ProcessDatasetUseCase:
    """Setup pipeline with specified model."""
    logger.info(f'Setting up pipeline with model: {model_name}')

    config = Config()
    config.PII_DETECT_MODEL = model_name
    config.PII_REFLECT_MODEL = model_name
    config.NON_PII_DETECT_MODEL = model_name

    factory = PipelineFactory(config)
    return factory.create_pipeline()


def create_groundtruth_template(dataset_path: Path, dataset_name: str) -> Path:
    """Create a groundtruth template for the uploaded dataset."""
    try:
        data_loader = SmartDataLoader(max_rows=1000)
        sheets_data = data_loader.load_dataset(str(dataset_path))

        template_data = {}
        for sheet_name, df in sheets_data.items():
            template_data[sheet_name] = {
                "columns": df.columns.tolist(),
                "sample_data": df.head(3).to_dict('records'),
                "row_count": len(df),
                "column_types": df.dtypes.astype(str).to_dict(),
            }

        template_path = GROUNDTRUTH_DIR / f"{dataset_name}.json"
        template_path.parent.mkdir(parents=True, exist_ok=True)

        with open(template_path, 'w') as f:
            json.dump(
                {
                    "dataset_name": dataset_name,
                    "created_at": datetime.now().isoformat(),
                    "sheets": template_data,
                    "status": "template",
                },
                f,
                indent=2,
            )

        logger.info(f"Created template: {template_path}")
        return template_path

    except Exception as e:
        logger.error(f"Error creating template for {dataset_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create template: {str(e)}")


@router.post("/upload")
async def upload_dataset(file: UploadFile = File(...)):
    """
    Upload dataset and immediately create groundtruth template.

    This endpoint:
    1. Saves the uploaded file to the data folder
    2. Creates a groundtruth template in groundtruth2
    3. Returns file info and template path
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")

    file_ext = Path(file.filename).suffix.lower()
    if file_ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"File type {file_ext} not allowed")

    try:
        # Ensure directories exist
        DATASETS_DIR.mkdir(parents=True, exist_ok=True)
        GROUNDTRUTH_DIR.mkdir(parents=True, exist_ok=True)

        # Save uploaded file to data folder
        dataset_path = DATASETS_DIR / file.filename
        with open(dataset_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Create groundtruth template immediately
        template_path = create_groundtruth_template(dataset_path, file.filename)

        logger.info(f"Successfully uploaded {file.filename} and created template")

        return {
            "message": "Upload successful",
            "filename": file.filename,
            "size": dataset_path.stat().st_size,
            "template_path": str(template_path),
            "dataset_path": str(dataset_path),
        }

    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@router.post("/batch-process")
async def start_batch_processing(background_tasks: BackgroundTasks, skip_existing: bool = True):
    """
    Start batch processing for all available models.

    This endpoint runs all models in parallel with skip-existing option.
    Returns immediately and runs processing in background.
    """
    global batch_status

    if batch_status["is_running"]:
        raise HTTPException(status_code=400, detail="Batch processing already running")

    # Get all datasets from groundtruth2
    datasets = [d for d in GROUNDTRUTH_DIR.iterdir() if d.is_file() and d.suffix == '.json']

    if not datasets:
        raise HTTPException(status_code=404, detail="No datasets found in groundtruth2")

    # Reset status
    batch_status = {
        "is_running": True,
        "current_model": None,
        "completed_models": [],
        "failed_models": [],
        "started_at": datetime.now().isoformat(),
        "progress": 0,
    }

    # Start background processing
    background_tasks.add_task(run_batch_processing, datasets, AVAILABLE_MODELS, skip_existing)

    return {
        "message": "Batch processing started",
        "datasets_count": len(datasets),
        "models_count": len(AVAILABLE_MODELS),
        "skip_existing": skip_existing,
    }


async def run_batch_processing(datasets: List[Path], models: List[str], skip_existing: bool):
    """Run batch processing for all models and datasets."""
    global batch_status

    total_tasks = len(models) * len(datasets)
    completed_tasks = 0

    try:
        for model in models:
            batch_status["current_model"] = model
            logger.info(f"Starting model: {model}")

            try:
                # Setup pipeline for this model
                pipeline = setup_pipeline(model)

                for dataset_file in datasets:
                    dataset_name = dataset_file.stem

                    # Check if result already exists
                    model_result_dir = REPORTS_DIR / model
                    expected_result = model_result_dir / f"{dataset_name}.json"

                    if skip_existing and expected_result.exists():
                        logger.info(f"Skipping {dataset_name} for {model} (already exists)")
                        completed_tasks += 1
                        batch_status["progress"] = int((completed_tasks / total_tasks) * 100)
                        continue

                    try:
                        # Process dataset
                        dataset_path = DATASETS_DIR / f"{dataset_name}.csv"
                        if not dataset_path.exists():
                            dataset_path = DATASETS_DIR / f"{dataset_name}.xlsx"

                        if not dataset_path.exists():
                            logger.warning(f"Dataset file not found: {dataset_name}")
                            continue

                        result = pipeline.process_dataset(str(dataset_path))

                        # Save result
                        model_result_dir.mkdir(parents=True, exist_ok=True)
                        with open(expected_result, 'w') as f:
                            json.dump(result, f, indent=2, default=str)

                        logger.info(f"Completed {dataset_name} with {model}")

                    except Exception as e:
                        logger.error(f"Failed to process {dataset_name} with {model}: {e}")

                    completed_tasks += 1
                    batch_status["progress"] = int((completed_tasks / total_tasks) * 100)

                batch_status["completed_models"].append(model)
                logger.info(f"✅ Completed: {model}")

            except Exception as e:
                batch_status["failed_models"].append(model)
                logger.error(f"❌ Failed: {model} - {e}")

    finally:
        batch_status["is_running"] = False
        batch_status["current_model"] = None
        batch_status["progress"] = 100

        logger.info("Batch processing complete!")


@router.get("/analytics/performance")
async def get_performance_metrics():
    """Calculate performance metrics for all models from test results."""
    metrics = {
        "overall_performance": [],
        "personal_sensitive": [],
        "non_personal_sensitive": [],
        "sheet_personal_sensitive": [],
        "sheet_non_personal_sensitive": [],
        "cost_analysis": [],
    }

    for model_name in AVAILABLE_MODELS:
        model_dir = REPORTS_DIR / model_name

        if not model_dir.exists():
            continue

        # Initialize metrics for this model
        model_metrics = {
            "model": model_name,
            "accuracy": 0.0,
            "precision": 0.0,
            "recall": 0.0,
            "f1_score": 0.0,
            "files_tested": 0,
            "true_positives": 0,
            "false_positives": 0,
            "false_negatives": 0,
            "true_negatives": 0,
        }

        personal_metrics = model_metrics.copy()
        non_personal_metrics = model_metrics.copy()
        sheet_personal_metrics = model_metrics.copy()
        sheet_non_personal_metrics = model_metrics.copy()

        sheet_personal_metrics["sheets_tested"] = 0
        sheet_non_personal_metrics["sheets_tested"] = 0

        # Process each dataset
        for result_file in model_dir.glob("*.json"):
            dataset_name = result_file.stem
            groundtruth_path = GROUNDTRUTH_DIR / f"{dataset_name}.json"

            if not groundtruth_path.exists():
                continue

            try:
                # Load model results and ground truth
                with open(result_file, 'r', encoding='utf-8') as f:
                    model_data = json.load(f)
                with open(groundtruth_path, 'r', encoding='utf-8') as f:
                    groundtruth_data = json.load(f)

                gt_personal = False
                gt_non_personal = False
                gt_sheets = {}
                
                if isinstance(groundtruth_data, list):
                    for sheet in groundtruth_data:
                        if isinstance(sheet, dict):
                            if sheet.get('personal_data_sensitive', False):
                                gt_personal = True
                            if sheet.get('non_personal_data_sensitive', False):
                                gt_non_personal = True
                            gt_sheets[sheet.get('sheet_name', 'unknown')] = {
                                'personal_data_sensitive': sheet.get('personal_data_sensitive', False),
                                'non_personal_data_sensitive': sheet.get('non_personal_data_sensitive', False)
                            }
                elif isinstance(groundtruth_data, dict):
                    gt_personal = groundtruth_data.get('personal_data_sensitive', False)
                    gt_non_personal = groundtruth_data.get('non_personal_data_sensitive', False)
                    # For legacy template format, assume one sheet or apply globally
                    gt_sheets['unknown'] = {
                        'personal_data_sensitive': gt_personal,
                        'non_personal_data_sensitive': gt_non_personal
                    }

                gt_overall = gt_personal or gt_non_personal

                # Determine model file-level predictions
                model_personal = False
                model_non_personal = False
                model_overall = False

                if isinstance(model_data, list):
                    for sheet in model_data:
                        if isinstance(sheet, dict):
                            if sheet.get('personal_data_sensitive', False):
                                model_personal = True
                            if sheet.get('non_personal_data_sensitive', False):
                                model_non_personal = True

                    model_overall = model_personal or model_non_personal

                # Update file-level confusion matrices
                # Overall metrics
                if model_overall and gt_overall:
                    model_metrics["true_positives"] += 1
                elif model_overall and not gt_overall:
                    model_metrics["false_positives"] += 1
                elif not model_overall and gt_overall:
                    model_metrics["false_negatives"] += 1
                else:
                    model_metrics["true_negatives"] += 1

                # Personal data metrics
                if model_personal and gt_personal:
                    personal_metrics["true_positives"] += 1
                elif model_personal and not gt_personal:
                    personal_metrics["false_positives"] += 1
                elif not model_personal and gt_personal:
                    personal_metrics["false_negatives"] += 1
                else:
                    personal_metrics["true_negatives"] += 1

                # Non-personal data metrics
                if model_non_personal and gt_non_personal:
                    non_personal_metrics["true_positives"] += 1
                elif model_non_personal and not gt_non_personal:
                    non_personal_metrics["false_positives"] += 1
                elif not model_non_personal and gt_non_personal:
                    non_personal_metrics["false_negatives"] += 1
                else:
                    non_personal_metrics["true_negatives"] += 1

                model_metrics["files_tested"] += 1
                personal_metrics["files_tested"] += 1
                non_personal_metrics["files_tested"] += 1

                # Sheet-level metrics
                if isinstance(model_data, list):
                    for sheet in model_data:
                        if not isinstance(sheet, dict):
                            continue

                        sheet_name = sheet.get('sheet_name', 'unknown')

                        # For sheet-level, we need to compare with ground truth if available
                        sheet_personal = sheet.get('personal_data_sensitive', False)
                        sheet_non_personal = sheet.get('non_personal_data_sensitive', False)
                        
                        sheet_gt = gt_sheets.get(sheet_name, gt_sheets.get('unknown', {}))
                        sheet_gt_personal = sheet_gt.get('personal_data_sensitive', False)
                        sheet_gt_non_personal = sheet_gt.get('non_personal_data_sensitive', False)

                        # Update sheet-level confusion matrices
                        if sheet_personal and sheet_gt_personal:
                            sheet_personal_metrics["true_positives"] += 1
                        elif sheet_personal and not sheet_gt_personal:
                            sheet_personal_metrics["false_positives"] += 1
                        elif not sheet_personal and sheet_gt_personal:
                            sheet_personal_metrics["false_negatives"] += 1
                        else:
                            sheet_personal_metrics["true_negatives"] += 1

                        if sheet_non_personal and sheet_gt_non_personal:
                            sheet_non_personal_metrics["true_positives"] += 1
                        elif sheet_non_personal and not sheet_gt_non_personal:
                            sheet_non_personal_metrics["false_positives"] += 1
                        elif not sheet_non_personal and sheet_gt_non_personal:
                            sheet_non_personal_metrics["false_negatives"] += 1
                        else:
                            sheet_non_personal_metrics["true_negatives"] += 1

                        sheet_personal_metrics["sheets_tested"] += 1
                        sheet_non_personal_metrics["sheets_tested"] += 1

            except Exception as e:
                logger.warning(f"Could not process {dataset_name} for {model_name}: {e}")
                continue

        # Calculate metrics for each category
        def calculate_metrics(confusion_matrix, is_sheet=False):
            tp = confusion_matrix["true_positives"]
            fp = confusion_matrix["false_positives"]
            fn = confusion_matrix["false_negatives"]
            tn = confusion_matrix["true_negatives"]

            accuracy = (tp + tn) / (tp + fp + fn + tn) if (tp + fp + fn + tn) > 0 else 0
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

            result = {
                "model": confusion_matrix["model"],
                "accuracy": accuracy,
                "precision": precision,
                "recall": recall,
                "f1_score": f1,
            }
            if is_sheet:
                result["sheets_tested"] = confusion_matrix.get("sheets_tested", 0)
            else:
                result["files_tested"] = confusion_matrix.get("files_tested", 0)
            return result

        # Add calculated metrics to results
        metrics["overall_performance"].append(calculate_metrics(model_metrics, False))
        metrics["personal_sensitive"].append(calculate_metrics(personal_metrics, False))
        metrics["non_personal_sensitive"].append(calculate_metrics(non_personal_metrics, False))
        metrics["sheet_personal_sensitive"].append(calculate_metrics(sheet_personal_metrics, True))
        metrics["sheet_non_personal_sensitive"].append(calculate_metrics(sheet_non_personal_metrics, True))

    return metrics


@router.get("/analytics/cost")
async def get_cost_analysis():
    """Calculate cost analysis from token usage in results."""
    cost_data = []

    # Pricing per 1M tokens (adjust as needed)
    pricing = {
        "gpt-4.1-nano": 0.17,
        "gpt-4.1-mini": 0.70,
        "gpt-4.1": 3.50,
        "gpt-5-nano": 0.15,
        "gpt-5-mini": 0.69,
        "DeepSeek-V3.1": 0.84,
    }

    for model_name in AVAILABLE_MODELS:
        model_dir = REPORTS_DIR / model_name

        if not model_dir.exists():
            continue

        total_prompt_tokens = 0
        total_completion_tokens = 0
        reports_count = 0

        for result_file in model_dir.glob("*.json"):
            try:
                with open(result_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # Extract token usage from the data structure
                if isinstance(data, list):
                    for sheet in data:
                        if isinstance(sheet, dict):
                            total_prompt_tokens += sheet.get('prompt_tokens', 0)
                            total_completion_tokens += sheet.get('completion_tokens', 0)

                reports_count += 1

            except Exception as e:
                logger.warning(f"Could not extract token data from {result_file}: {e}")
                continue

        total_tokens = total_prompt_tokens + total_completion_tokens
        total_cost = (total_tokens / 1000000) * pricing.get(model_name, 0)
        cost_per_report = total_cost / reports_count if reports_count > 0 else 0

        cost_data.append(
            {
                "model": model_name,
                "reports": reports_count,
                "prompt_tokens": total_prompt_tokens,
                "completion_tokens": total_completion_tokens,
                "total_tokens": total_tokens,
                "price_per_1m": pricing.get(model_name, 0),
                "total_cost": total_cost,
                "cost_per_report": cost_per_report,
            }
        )

    return {"cost_analysis": cost_data, "pricing": pricing}


@router.get("/batch-status")
async def get_batch_status():
    """Get current batch processing status."""
    return batch_status


@router.get("/datasets")
async def list_datasets():
    """List all available datasets in groundtruth2."""
    datasets = []
    for file in GROUNDTRUTH_DIR.iterdir():
        if file.is_file() and file.suffix == '.json':
            try:
                with open(file, 'r') as f:
                    data = json.load(f)
                    datasets.append(
                        {
                            "name": file.stem,
                            "status": data.get("status", "unknown"),
                            "created_at": data.get("created_at"),
                            "path": str(file),
                        }
                    )
            except Exception as e:
                logger.warning(f"Could not read dataset info for {file.name}: {e}")

    return {"datasets": datasets}


@router.get("/models")
async def list_models():
    """List all available models for processing."""
    return {"models": AVAILABLE_MODELS}


@router.get("/results/{model_name}")
async def get_model_results(model_name: str):
    """Get all results for a specific model."""
    if model_name not in AVAILABLE_MODELS:
        raise HTTPException(status_code=404, detail=f"Model {model_name} not found")

    results = []
    model_dir = REPORTS_DIR / model_name

    if not model_dir.exists():
        return {"results": results}

    for result_file in model_dir.glob("*.json"):
        try:
            with open(result_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Handle the actual data structure (array of sheet results)
            dataset_name = result_file.stem
            processed_at = 'Unknown'

            if isinstance(data, list) and len(data) > 0:
                # Get the first sheet's timestamp
                first_sheet = data[0]
                processed_at = first_sheet.get('processing_timestamp', 'Unknown')

                # Calculate totals from all sheets
                total_pii = sum(1 for sheet in data if sheet.get('personal_data_sensitive', False))
                total_rows = sum(sheet.get('n_records', 0) for sheet in data)
                total_columns = sum(sheet.get('n_columns', 0) for sheet in data)

                # Determine sensitivity based on PII detection
                sensitivity = "Low"
                if any(sheet.get('personal_data_sensitive', False) for sheet in data):
                    sensitivity = "High"
                elif any(sheet.get('non_personal_data_sensitive', False) for sheet in data):
                    sensitivity = "Medium"

                results.append(
                    {
                        "model": model_name,
                        "dataset": dataset_name,
                        "processed_at": processed_at,
                        "file_path": str(result_file),
                        "sensitivity": sensitivity,
                        "pii_count": total_pii,
                        "row_count": total_rows,
                        "status": "completed",
                    }
                )
            else:
                # Fallback for unexpected format
                results.append(
                    {
                        "model": model_name,
                        "dataset": dataset_name,
                        "processed_at": result_file.stat().st_mtime,
                        "file_path": str(result_file),
                        "status": "completed",
                    }
                )

        except Exception as e:
            logger.warning(f"Could not read result file {result_file}: {e}")
            results.append(
                {
                    "model": model_name,
                    "dataset": result_file.stem,
                    "processed_at": result_file.stat().st_mtime,
                    "file_path": str(result_file),
                    "status": "failed",
                }
            )

    return {"results": sorted(results, key=lambda x: x['processed_at'], reverse=True)}


@router.get("/report/{model_name}/{dataset_name}")
async def get_report_detail(model_name: str, dataset_name: str):
    """Get detailed report for a specific model and dataset."""
    if model_name not in AVAILABLE_MODELS:
        raise HTTPException(status_code=404, detail=f"Model {model_name} not found")

    report_path = REPORTS_DIR / model_name / f"{dataset_name}.json"
    groundtruth_path = GROUNDTRUTH_DIR / f"{dataset_name}.json"

    if not report_path.exists():
        raise HTTPException(status_code=404, detail=f'Report not found for {model_name}/{dataset_name}')

    try:
        with open(report_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Load ground truth data if available
        groundtruth_data = None
        if groundtruth_path.exists():
            with open(groundtruth_path, 'r', encoding='utf-8') as f:
                groundtruth_data = json.load(f)

        # Format groundtruth data to file-level format for the frontend
        formatted_groundtruth = None
        if groundtruth_data is not None:
            if isinstance(groundtruth_data, list):
                has_personal = any(s.get('personal_data_sensitive', False) for s in groundtruth_data if isinstance(s, dict))
                has_non_personal = any(s.get('non_personal_data_sensitive', False) for s in groundtruth_data if isinstance(s, dict))
                formatted_groundtruth = {
                    'personal_data_sensitive': has_personal,
                    'non_personal_data_sensitive': has_non_personal
                }
            elif isinstance(groundtruth_data, dict):
                formatted_groundtruth = {
                    'personal_data_sensitive': groundtruth_data.get('personal_data_sensitive', False),
                    'non_personal_data_sensitive': groundtruth_data.get('non_personal_data_sensitive', False)
                }

        # Handle the actual data structure (array of sheet results)
        formatted_data = {
            "dataset_name": dataset_name,
            "model": model_name,
            "processed_at": "Unknown",
            "sheets": {},
            "groundtruth": formatted_groundtruth,
        }

        if isinstance(data, list) and len(data) > 0:
            # Get processing timestamp from first sheet
            formatted_data["processed_at"] = data[0].get('processing_timestamp', 'Unknown')

            # Process each sheet
            for sheet_data in data:
                if isinstance(sheet_data, dict):
                    sheet_name = sheet_data.get('sheet_name', 'unknown')

                    # Extract column predictions
                    predictions = {}
                    columns = sheet_data.get('columns', [])

                    # Create predictions for each column
                    for col in columns:
                        if isinstance(col, dict):
                            col_name = col.get('column_name', 'unknown')
                            sample_values = col.get('sample_values', [])

                            predictions[col_name] = {
                                "prediction": (
                                    "personal_data_sensitive"
                                    if sheet_data.get('personal_data_sensitive', False)
                                    else "non_personal_data_sensitive"
                                ),
                                "confidence": None,  # Not available in current format
                                "reasoning": f"Processed by {model_name} on {sheet_data.get('processing_timestamp', 'Unknown')}",
                                "sample_values": sample_values,
                                "explanation": sheet_data.get('explanation', ''),
                                "isp_used": sheet_data.get('isp_used', 'Unknown'),
                            }

                    # Calculate metadata
                    total_rows = sheet_data.get('n_records', 0)
                    pii_detected = 1 if sheet_data.get('personal_data_sensitive', False) else 0
                    sensitivity_level = "High" if sheet_data.get('personal_data_sensitive', False) else "Low"

                    formatted_data["sheets"][sheet_name] = {
                        "columns": [
                            col.get('column_name', 'unknown') if isinstance(col, dict) else str(col) for col in columns
                        ],
                        "predictions": predictions,
                        "metadata": {
                            "total_rows": total_rows,
                            "pii_detected": pii_detected,
                            "sensitivity_level": sensitivity_level,
                            "personal_data_sensitive": sheet_data.get('personal_data_sensitive', False),
                            "non_personal_data_sensitive": sheet_data.get('non_personal_data_sensitive', False),
                            "explanation": sheet_data.get('explanation', ''),
                            "isp_used": sheet_data.get('isp_used', 'Unknown'),
                        },
                    }

        return formatted_data

    except Exception as e:
        logger.error(f"Error reading report {report_path}: {e}")
        raise HTTPException(status_code=500, detail="Failed to read report")


@router.delete("/batch-stop")
async def stop_batch_processing():
    """Stop current batch processing (placeholder)."""
    global batch_status
    if not batch_status["is_running"]:
        raise HTTPException(status_code=400, detail="No batch processing running")

    # In a real implementation, you'd need to handle graceful cancellation
    batch_status["is_running"] = False
    batch_status["current_model"] = None

    return {"message": "Batch processing stop requested"}
