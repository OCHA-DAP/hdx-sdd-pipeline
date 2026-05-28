"""Research API router — clean, thin, no business logic inline."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, HTTPException
from dotenv import load_dotenv
from .schemas import BatchStatus
from .sdd_io import load_sdd
from .metrics_service import compute_performance, compute_cost

load_dotenv()
logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_BASE = Path(os.getenv('PROJECT_ROOT', str(Path(__file__).resolve().parents[1])))
DATASETS_DIR = Path(os.getenv('DATASETS_DIR', str(_BASE / 'research' / 'data')))
REPORTS_DIR = Path(os.getenv('REPORTS_DIR', str(_BASE / 'research' / 'results' / 'test_results')))
GROUNDTRUTH_DIR = Path(os.getenv('GROUNDTRUTH_DIR', str(REPORTS_DIR / 'groundtruth2')))
ALLOWED_EXTENSIONS = {'.csv', '.xlsx'}

AVAILABLE_MODELS = [
    'gpt-4.1-nano',
    'gpt-4.1-mini',
    'gpt-4.1',
    'gpt-5-nano',
    'gpt-5-mini',
    'DeepSeek-V3.1',
    'DeepSeek-V4-Flash',
]

PRICING: dict[str, float] = {
    'gpt-4.1-nano': 0.17,
    'gpt-4.1-mini': 0.70,
    'gpt-4.1': 3.50,
    'gpt-5-nano': 0.15,
    'gpt-5-mini': 0.69,
    'DeepSeek-V3.1': 0.84,
    'DeepSeek-V4-Flash': 0.10,
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
    if any(sep in filename for sep in ('/', '\\')):
        raise HTTPException(400, 'Invalid filename')
    return Path(filename).name


def _unique_path(directory: Path, filename: str) -> Path:
    target = directory / filename
    if not target.exists():
        return target
    stem, suffix = Path(filename).stem, Path(filename).suffix
    ts = datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
    return directory / f'{stem}_{ts}{suffix}'


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get('/datasets')
async def list_datasets():
    datasets = []
    for f in GROUNDTRUTH_DIR.iterdir():
        if not (f.is_file() and f.suffix == '.json'):
            continue
        try:
            sdd = load_sdd(f)
            datasets.append({'name': f.stem, 'sensitive': sdd.sensitive, 'timestamp': sdd.timestamp})
        except Exception as exc:
            datasets.append({'name': f.stem, 'error': str(exc)})
    return {'datasets': datasets}


@router.get('/models')
async def list_models():
    return {'models': AVAILABLE_MODELS}


@router.get('/results/{model_name}')
async def get_model_results(model_name: str):
    if model_name not in AVAILABLE_MODELS:
        raise HTTPException(404, f'Unknown model: {model_name}')

    model_dir = REPORTS_DIR / model_name
    if not model_dir.exists():
        return {'results': []}

    results = []
    for result_file in model_dir.glob('*.json'):
        try:
            sdd = load_sdd(result_file)
            total_rows = sum(s.n_records for s in sdd.sdd_report)
            results.append(
                {
                    'model': model_name,
                    'dataset': result_file.stem,
                    'processed_at': sdd.timestamp,
                    'sensitive': sdd.sensitive,
                    'sheet_count': len(sdd.sdd_report),
                    'total_rows': total_rows,
                    'status': 'completed',
                }
            )
        except Exception as exc:
            try:
                mtime = datetime.fromtimestamp(result_file.stat().st_mtime).isoformat()
            except Exception:
                mtime = datetime.now().isoformat()
            results.append(
                {
                    'model': model_name,
                    'dataset': result_file.stem,
                    'processed_at': mtime,
                    'sensitive': 'error',
                    'sheet_count': 0,
                    'total_rows': 0,
                    'status': 'error',
                    'error': str(exc),
                }
            )

    return {'results': sorted(results, key=lambda x: x['processed_at'], reverse=True)}


@router.get('/report/{model_name}/{dataset_name}')
async def get_report_detail(model_name: str, dataset_name: str):
    if model_name not in AVAILABLE_MODELS:
        raise HTTPException(404, f'Unknown model: {model_name}')

    report_path = REPORTS_DIR / model_name / f'{dataset_name}.json'
    gt_path = GROUNDTRUTH_DIR / f'{dataset_name}.json'

    if not report_path.exists():
        raise HTTPException(404, f'Report not found: {model_name}/{dataset_name}')

    try:
        pred = load_sdd(report_path)
        gt = load_sdd(gt_path) if gt_path.exists() else None
    except Exception as exc:
        raise HTTPException(500, f'Failed to load report: {exc}')

    gt_map = {s.sheet_name.strip().lower(): s for s in gt.sdd_report} if gt else {}

    sheets_out = {}
    for sheet in pred.sdd_report:
        key = sheet.sheet_name.strip().lower()
        gt_sheet = gt_map.get(key)
        sheets_out[sheet.sheet_name] = {
            'sheet_name': sheet.sheet_name,
            'n_records': sheet.n_records,
            'personal_data_sensitive': sheet.personal_data_sensitive,
            'non_personal_data_sensitive': sheet.non_personal_data_sensitive,
            'personal_data_risk_level': sheet.personal_data_risk_level,
            'non_personal_data_risk_level': sheet.non_personal_data_risk_level,
            'personal_data': sheet.personal_data.model_dump(),
            'non_personal_data': sheet.non_personal_data.model_dump(),
            'columns': [c.model_dump() for c in sheet.columns],
            'is_readme': sheet.is_readme,
            'groundtruth': {
                'personal_data_sensitive': gt_sheet.personal_data_sensitive if gt_sheet else None,
                'non_personal_data_sensitive': gt_sheet.non_personal_data_sensitive if gt_sheet else None,
            },
        }

    return {
        'dataset_name': dataset_name,
        'model': model_name,
        'timestamp': pred.timestamp,
        'sensitive': pred.sensitive,
        'sensitivity_level': pred.sensitivity_level,
        'groundtruth_sensitive': gt.sensitive if gt else None,
        'sheets': sheets_out,
    }


@router.get('/analytics/performance')
async def get_performance_metrics():
    categories = [
        'overall_performance',
        'personal_sensitive',
        'non_personal_sensitive',
        'sheet_personal_sensitive',
        'sheet_non_personal_sensitive',
        'sheet_overall_sensitive',
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


@router.get('/analytics/cost')
async def get_cost_analysis():
    cost_data = []
    for model in AVAILABLE_MODELS:
        model_dir = REPORTS_DIR / model
        if not model_dir.exists():
            continue
        cost_data.append(compute_cost(model, model_dir, PRICING.get(model, 0)))
    return {'cost_analysis': cost_data, 'pricing': PRICING}
