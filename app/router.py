"""Research API router — clean, thin, no business logic inline."""

from __future__ import annotations

import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, BackgroundTasks, File, HTTPException, UploadFile
from dotenv import load_dotenv

from config.config import Config
from src.infrastructure.factories.pipeline_factory import PipelineFactory
from src.application.use_cases.process_dataset import ProcessDatasetUseCase
from src.infrastructure.storage.data_loader import SmartDataLoader

from .schemas import SDDReport, BatchStatus
from .sdd_io import load_sdd, save_sdd, sensitivity_from_sheets
from .metrics_service import compute_performance, compute_cost

load_dotenv()
logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_BASE = Path(os.getenv("PROJECT_ROOT", str(Path(__file__).resolve().parents[1])))
DATASETS_DIR    = Path(os.getenv("DATASETS_DIR",    str(_BASE / "research" / "data")))
REPORTS_DIR     = Path(os.getenv("REPORTS_DIR",     str(_BASE / "research" / "results" / "test_results")))
GROUNDTRUTH_DIR = Path(os.getenv("GROUNDTRUTH_DIR", str(REPORTS_DIR / "groundtruth2")))
ALLOWED_EXTENSIONS = {".csv", ".xlsx"}

AVAILABLE_MODELS = [
    "gpt-4.1-nano", "gpt-4.1-mini", "gpt-4.1",
    "gpt-5-nano", "gpt-5-mini",
    "DeepSeek-V3.1", "DeepSeek-V4-Flash",
]

PRICING: dict[str, float] = {
    "gpt-4.1-nano":      0.17,
    "gpt-4.1-mini":      0.70,
    "gpt-4.1":           3.50,
    "gpt-5-nano":        0.15,
    "gpt-5-mini":        0.69,
    "DeepSeek-V3.1":     0.84,
    "DeepSeek-V4-Flash": 0.10,
}

# ---------------------------------------------------------------------------
# Batch state  (replace with Redis / DB in production)
# ---------------------------------------------------------------------------

_batch_status = BatchStatus(
    is_running=False,
    current_model=None,
    completed_models=[],
    failed_models=[],
    started_at=None,
    progress=0,
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _safe_filename(filename: str) -> str:
    if any(sep in filename for sep in ("/", "\\")):
        raise HTTPException(400, "Invalid filename")
    return Path(filename).name


def _unique_path(directory: Path, filename: str) -> Path:
    target = directory / filename
    if not target.exists():
        return target
    stem, suffix = Path(filename).stem, Path(filename).suffix
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    return directory / f"{stem}_{ts}{suffix}"


def _setup_pipeline(model_name: str | None = None) -> ProcessDatasetUseCase:
    config = Config()
    if model_name:
        config.PII_DETECT_MODEL = model_name
        config.PII_REFLECT_MODEL = model_name
        config.NON_PII_DETECT_MODEL = model_name
    return PipelineFactory(config).create_pipeline()


def _create_groundtruth_template(dataset_path: Path, dataset_name: str) -> Path:
    """Run the pipeline data-loader only and write a TODO-annotated template."""
    pipeline = ProcessDatasetUseCase(
        data_loader=SmartDataLoader(max_rows=1000),
        pii_llm_provider=None,
        pii_reflection_llm_provider=None,
        non_pii_llm_provider=None,
        readme_llm_provider=None,
    )

    sheets_data = pipeline.data_loader.load_from_file(str(dataset_path))
    template_sheets = []

    for sheet_name, df in sheets_data.items():
        if pipeline._is_readme_sheet(sheet_name):
            report_dict = pipeline._create_readme_report(sheet_name, str(dataset_path), dataset_name, df).to_dict()
        else:
            report_dict = pipeline._create_data_report(sheet_name, str(dataset_path), dataset_name, df, isp_rules=None).to_dict()

        report_dict["personal_data_sensitive"]     = "TODO"
        report_dict["non_personal_data_sensitive"]  = "TODO"
        report_dict.setdefault("non_personal_data", {})["sensitivity"] = "TODO"
        for col in report_dict.get("columns", []):
            col.setdefault("personal_data", {})["entity_type"] = "TODO"
            col.setdefault("personal_data", {})["sensitive"]   = "TODO"

        template_sheets.append(report_dict)

    out = {
        "resource_id": dataset_name,
        "sensitive": "TODO",
        "timestamp": datetime.now().isoformat(),
        "sdd_report": template_sheets,
    }
    dest = GROUNDTRUTH_DIR / f"{dataset_name}.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "w") as f:
        json.dump(out, f, indent=2)
    return dest


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/upload")
async def upload_dataset(file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(400, "No file provided")

    safe_name = _safe_filename(file.filename)
    if Path(safe_name).suffix.lower() not in ALLOWED_EXTENSIONS:
        raise HTTPException(400, f"File type not allowed: {Path(safe_name).suffix}")

    DATASETS_DIR.mkdir(parents=True, exist_ok=True)
    GROUNDTRUTH_DIR.mkdir(parents=True, exist_ok=True)

    dest = _unique_path(DATASETS_DIR, safe_name)
    with open(dest, "wb") as buf:
        shutil.copyfileobj(file.file, buf)

    template_path = _create_groundtruth_template(dest, dest.name)
    logger.info("Uploaded %s → template %s", dest.name, template_path)

    return {
        "message": "Upload successful",
        "filename": dest.name,
        "size": dest.stat().st_size,
        "template_path": str(template_path),
        "dataset_path": str(dest),
    }


@router.post("/batch-process")
async def start_batch_processing(background_tasks: BackgroundTasks, skip_existing: bool = True):
    global _batch_status
    if _batch_status.is_running:
        raise HTTPException(400, "Batch processing already running")

    datasets = [d for d in GROUNDTRUTH_DIR.iterdir() if d.is_file() and d.suffix == ".json"]
    if not datasets:
        raise HTTPException(404, "No datasets found in groundtruth dir")

    _batch_status = BatchStatus(
        is_running=True,
        current_model=None,
        completed_models=[],
        failed_models=[],
        started_at=datetime.now().isoformat(),
        progress=0,
    )
    background_tasks.add_task(_run_batch, datasets, AVAILABLE_MODELS, skip_existing)

    return {
        "message": "Batch processing started",
        "datasets_count": len(datasets),
        "models_count": len(AVAILABLE_MODELS),
        "skip_existing": skip_existing,
    }


@router.get("/batch-status")
async def get_batch_status():
    return _batch_status


@router.delete("/batch-stop")
async def stop_batch_processing():
    global _batch_status
    if not _batch_status.is_running:
        raise HTTPException(400, "No batch processing running")
    _batch_status.is_running = False
    _batch_status.current_model = None
    return {"message": "Batch processing stop requested"}


@router.get("/datasets")
async def list_datasets():
    datasets = []
    for f in GROUNDTRUTH_DIR.iterdir():
        if not (f.is_file() and f.suffix == ".json"):
            continue
        try:
            sdd = load_sdd(f)
            datasets.append({"name": f.stem, "sensitive": sdd.sensitive, "timestamp": sdd.timestamp})
        except Exception as exc:
            datasets.append({"name": f.stem, "error": str(exc)})
    return {"datasets": datasets}


@router.get("/models")
async def list_models():
    return {"models": AVAILABLE_MODELS}


@router.get("/results/{model_name}")
async def get_model_results(model_name: str):
    if model_name not in AVAILABLE_MODELS:
        raise HTTPException(404, f"Unknown model: {model_name}")

    model_dir = REPORTS_DIR / model_name
    if not model_dir.exists():
        return {"results": []}

    results = []
    for result_file in model_dir.glob("*.json"):
        try:
            sdd = load_sdd(result_file)
            total_rows = sum(s.n_records for s in sdd.sdd_report)
            results.append({
                "model": model_name,
                "dataset": result_file.stem,
                "processed_at": sdd.timestamp,
                "sensitive": sdd.sensitive,
                "sheet_count": len(sdd.sdd_report),
                "total_rows": total_rows,
                "status": "completed",
            })
        except Exception as exc:
            results.append({
                "model": model_name,
                "dataset": result_file.stem,
                "status": "error",
                "error": str(exc),
            })

    return {"results": sorted(results, key=lambda x: x["processed_at"], reverse=True)}


@router.get("/report/{model_name}/{dataset_name}")
async def get_report_detail(model_name: str, dataset_name: str):
    if model_name not in AVAILABLE_MODELS:
        raise HTTPException(404, f"Unknown model: {model_name}")

    report_path = REPORTS_DIR / model_name / f"{dataset_name}.json"
    gt_path     = GROUNDTRUTH_DIR / f"{dataset_name}.json"

    if not report_path.exists():
        raise HTTPException(404, f"Report not found: {model_name}/{dataset_name}")

    try:
        pred = load_sdd(report_path)
        gt   = load_sdd(gt_path) if gt_path.exists() else None
    except Exception as exc:
        raise HTTPException(500, f"Failed to load report: {exc}")

    gt_map = (
        {s.sheet_name.strip().lower(): s for s in gt.sdd_report} if gt else {}
    )

    sheets_out = {}
    for sheet in pred.sdd_report:
        key = sheet.sheet_name.strip().lower()
        gt_sheet = gt_map.get(key)
        sheets_out[sheet.sheet_name] = {
            "sheet_name": sheet.sheet_name,
            "n_records": sheet.n_records,
            "personal_data_sensitive": sheet.personal_data_sensitive,
            "non_personal_data_sensitive": sheet.non_personal_data_sensitive,
            "personal_data": sheet.personal_data.model_dump(),
            "non_personal_data": sheet.non_personal_data.model_dump(),
            "columns": [c.model_dump() for c in sheet.columns],
            "is_readme": sheet.is_readme,
            "groundtruth": {
                "personal_data_sensitive": gt_sheet.personal_data_sensitive if gt_sheet else None,
                "non_personal_data_sensitive": gt_sheet.non_personal_data_sensitive if gt_sheet else None,
            },
        }

    return {
        "dataset_name": dataset_name,
        "model": model_name,
        "timestamp": pred.timestamp,
        "sensitive": pred.sensitive,
        "groundtruth_sensitive": gt.sensitive if gt else None,
        "sheets": sheets_out,
    }


@router.get("/analytics/performance")
async def get_performance_metrics():
    categories = [
        "overall_performance", "personal_sensitive", "non_personal_sensitive",
        "sheet_personal_sensitive", "sheet_non_personal_sensitive", "sheet_overall_sensitive",
    ]
    result = {c: [] for c in categories}

    for model in AVAILABLE_MODELS:
        model_dir = REPORTS_DIR / model
        if not model_dir.exists():
            continue
        metrics = compute_performance(model, model_dir, GROUNDTRUTH_DIR)
        if metrics:
            for key, value in metrics.to_response().items():
                result[key].append(value)

    return result


@router.get("/analytics/cost")
async def get_cost_analysis():
    cost_data = []
    for model in AVAILABLE_MODELS:
        model_dir = REPORTS_DIR / model
        if not model_dir.exists():
            continue
        cost_data.append(compute_cost(model, model_dir, PRICING.get(model, 0)))
    return {"cost_analysis": cost_data, "pricing": PRICING}


# ---------------------------------------------------------------------------
# Background task
# ---------------------------------------------------------------------------

async def _run_batch(datasets: list[Path], models: list[str], skip_existing: bool) -> None:
    global _batch_status
    total = len(models) * len(datasets)
    done  = 0

    try:
        for model in models:
            _batch_status.current_model = model
            try:
                pipeline = _setup_pipeline(model)

                for gt_file in datasets:
                    dataset_name = gt_file.stem
                    dest = REPORTS_DIR / model / f"{dataset_name}.json"

                    if skip_existing and dest.exists():
                        done += 1
                        _batch_status.progress = int(done / total * 100)
                        continue

                    # Resolve actual data file from groundtruth stem
                    data_file = next(
                        (DATASETS_DIR / f"{dataset_name}{ext}" for ext in ("", ".csv", ".xlsx")
                         if (DATASETS_DIR / f"{dataset_name}{ext}").exists()),
                        None,
                    )
                    if data_file is None:
                        logger.warning("Dataset file not found for: %s", dataset_name)
                        done += 1
                        continue

                    try:
                        reports = pipeline.execute(
                            source=str(data_file),
                            resource_id=dataset_name,
                            is_url=False,
                            isp_rules=None,
                        )
                        sheets = [r for r in reports]
                        sensitivity = sensitivity_from_sheets(sheets)
                        sdd = SDDReport(
                            resource_id=dataset_name,
                            sensitive=sensitivity,
                            timestamp=datetime.now().isoformat(),
                            sdd_report=[r.to_dict() for r in reports],
                        )
                        save_sdd(sdd, dest)
                        logger.info("Saved: %s / %s", model, dataset_name)
                    except Exception as exc:
                        logger.error("Failed %s / %s: %s", model, dataset_name, exc)

                    done += 1
                    _batch_status.progress = int(done / total * 100)

                _batch_status.completed_models.append(model)

            except Exception as exc:
                logger.error("Model %s failed: %s", model, exc)
                _batch_status.failed_models.append(model)

    finally:
        _batch_status.is_running    = False
        _batch_status.current_model = None
        _batch_status.progress      = 100
