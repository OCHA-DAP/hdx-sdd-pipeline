from fastapi import APIRouter, HTTPException, Query
from pathlib import Path
from fastapi import UploadFile, File
import shutil
import json
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
from typing import Dict, List, Any
from utils.processing import create_report
from utils.utils import determine_sensitivity
from main import sheet_processor, get_isp

router = APIRouter()
DATASETS_DIR = Path('/Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/research/data')  # change this
REPORTS_DIR = Path('/Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/research/results/test_results')  # change this
GROUNDTRUTH_DIR = REPORTS_DIR / 'groundtruth2'
ALLOWED_EXTENSIONS = {'.csv', '.xlsx'}


@router.get('/health')
async def health_check():
    return {'status': 'ok'}


@router.get('/list-datasets')
async def list_datasets():
    if not DATASETS_DIR.exists():
        raise HTTPException(status_code=500, detail='Datasets directory not found')
    datasets = [
        {
            'name': file.name,
            'path': str(file),
            'size_bytes': file.stat().st_size,
        }
        for file in DATASETS_DIR.iterdir()
        if file.is_file() and file.suffix.lower() in ALLOWED_EXTENSIONS
    ]
    return {
        'count': len(datasets),
        'datasets': datasets,
    }


# --- Upload endpoint ---
@router.post('/upload')
async def upload_dataset(file: UploadFile = File(...)):
    # Ensure the directory exists
    DATASETS_DIR.mkdir(parents=True, exist_ok=True)
    file_extension = Path(file.filename).suffix.lower()
    if file_extension not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail='Invalid file type')
    dest_file = DATASETS_DIR / file.filename
    # Check if file already exists
    if dest_file.exists():
        raise HTTPException(status_code=400, detail='File already exists')
    # Save uploaded file
    with dest_file.open('wb') as buffer:
        shutil.copyfileobj(file.file, buffer)
    return {
        'message': 'File uploaded successfully',
        'filename': file.filename,
        'path': str(dest_file),
    }


# --- Has report endpoint ---
# Based on dataset filename and model name, check if a report exists
@router.post('/has-report')
async def has_report(
    dataset_filename: str = Query(..., description='Name of the dataset file'),
    model_name: str = Query(..., description='Name of the model'),
):
    report_file = REPORTS_DIR / model_name / f'{dataset_filename}.json'
    if report_file.exists():
        return {
            'has_report': True,
            'report_path': str(report_file),
        }
    else:
        return {
            'has_report': False,
        }


# Generate report endpoint
@router.post('/generate-report')
async def generate_report(
    dataset_filename: str = Query(..., description='Name of the dataset file'),
    model_name: str = Query(..., description='Name of the model to use for classification'),
):
    report_file = REPORTS_DIR / model_name / f'{dataset_filename}.json'
    # Check if report file exists
    if report_file.exists():
        # Load report file
        with report_file.open('r') as f:
            report = json.load(f)
        return {
            'has_report': True,
            'report_path': str(report_file),
            'report': report,
        }
    # Read dataset file
    dataset_file = DATASETS_DIR / dataset_filename
    if not dataset_file.exists():
        raise HTTPException(status_code=404, detail='Dataset file not found')
    # Create initial reports from the dataset file
    sdd_reports = create_report(str(dataset_file))
    # Get ISP configuration (using default since we don't have package_id)
    isp = get_isp(None)
    # Try with filename as fallback
    if 'default' in isp:
        isp = get_isp(dataset_filename)
    # Process each report through sheet_processor
    for i, sdd_report in enumerate(sdd_reports):
        sdd_reports[i] = sheet_processor(sdd_report, isp, model=model_name)
        # If error_source is not None then set processing_success to False
        if sdd_reports[i].get('error_source', None):
            sdd_reports[i]['processing_success'] = False
    # Determine overall sensitivity
    sensitivity = determine_sensitivity(sdd_reports)
    # Ensure report directory exists
    report_file.parent.mkdir(parents=True, exist_ok=True)
    # Save report to file
    with report_file.open('w', encoding='utf-8') as f:
        json.dump(sdd_reports, f, indent=2, ensure_ascii=False)
    return {
        'message': 'Report generated successfully',
        'report_path': str(report_file),
        'sensitivity': sensitivity,
        'reports': sdd_reports,
    }


def load_json_file(file_path: Path) -> List[Dict[str, Any]]:
    """Load JSON file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def compute_file_level_metrics(model_name: str) -> Dict[str, Any]:
    """Compute file-level binary sensitivity metrics."""
    model_dir = REPORTS_DIR / model_name
    if not model_dir.exists():
        return {'error': f'Model directory {model_name} not found'}
    gt_files = set(f.name for f in GROUNDTRUTH_DIR.glob('*.json'))
    pred_files = set(f.name for f in model_dir.glob('*.json'))
    common_files = gt_files & pred_files
    if not common_files:
        return {'error': 'No common files found between groundtruth and predictions'}
    gt_arr, pred_arr = [], []
    misclassifications = []
    for filename in common_files:
        gt_data = load_json_file(GROUNDTRUTH_DIR / filename)
        pred_data = load_json_file(model_dir / filename)
        # File is sensitive if ANY sheet has pii_sensitive OR non_pii_sensitive
        gt_sensitive = any(
            sheet.get('pii_sensitive', False) or sheet.get('non_pii_sensitive', False) for sheet in gt_data
        )
        pred_sensitive = any(
            sheet.get('pii_sensitive', False) or sheet.get('non_pii_sensitive', False) for sheet in pred_data
        )
        gt_arr.append(int(gt_sensitive))
        pred_arr.append(int(pred_sensitive))
        if gt_sensitive != pred_sensitive:
            misclassifications.append(
                {
                    'file': filename,
                    'true_label': 'Sensitive' if gt_sensitive else 'Not Sensitive',
                    'predicted_label': 'Sensitive' if pred_sensitive else 'Not Sensitive',
                    'error_type': 'False Negative' if gt_sensitive and not pred_sensitive else 'False Positive',
                }
            )
    cm = confusion_matrix(gt_arr, pred_arr)
    tn, fp, fn, tp = cm.ravel() if cm.size == 4 else (0, 0, 0, 0)
    # Explicitly specify labels to ensure 2x2 matrix even if only one class is present
    cm = confusion_matrix(gt_arr, pred_arr, labels=[0, 1])
    tn, fp, fn, tp = cm.ravel()
    return {
        'accuracy': float(accuracy_score(gt_arr, pred_arr)),
        'precision': float(precision_score(gt_arr, pred_arr, zero_division=0)),
        'recall': float(recall_score(gt_arr, pred_arr, zero_division=0)),
        'f1': float(f1_score(gt_arr, pred_arr, zero_division=0)),
        'confusion_matrix': {
            'true_negative': int(tn),
            'false_positive': int(fp),
            'false_negative': int(fn),
            'true_positive': int(tp),
        },
        'total_files': len(common_files),
        'misclassifications': misclassifications,
    }


def compute_sheet_level_metrics(model_name: str, category: str) -> Dict[str, Any]:
    """Compute sheet-level metrics for pii_sensitive or non_pii_sensitive."""
    model_dir = REPORTS_DIR / model_name
    if not model_dir.exists():
        return {'error': f'Model directory {model_name} not found'}
    gt_files = set(f.name for f in GROUNDTRUTH_DIR.glob('*.json'))
    pred_files = set(f.name for f in model_dir.glob('*.json'))
    common_files = gt_files & pred_files
    if not common_files:
        return {'error': 'No common files found'}
    gt_arr, pred_arr = [], []
    misclassifications = []
    for filename in common_files:
        gt_data = load_json_file(GROUNDTRUTH_DIR / filename)
        pred_data = load_json_file(model_dir / filename)
        # Match sheets by index (assuming same order)
        for idx in range(min(len(gt_data), len(pred_data))):
            gt_sheet = gt_data[idx]
            pred_sheet = pred_data[idx]
            gt_val = int(gt_sheet.get(category, False))
            pred_val = int(pred_sheet.get(category, False))
            gt_arr.append(gt_val)
            pred_arr.append(pred_val)
            if gt_val != pred_val:
                misclassifications.append(
                    {
                        'file': filename,
                        'sheet_name': gt_sheet.get('sheet_name', f'Sheet {idx}'),
                        'true_label': 'Sensitive' if gt_val else 'Not Sensitive',
                        'predicted_label': 'Sensitive' if pred_val else 'Not Sensitive',
                        'error_type': 'False Negative' if gt_val and not pred_val else 'False Positive',
                    }
                )
    cm = confusion_matrix(gt_arr, pred_arr)
    tn, fp, fn, tp = cm.ravel() if cm.size == 4 else (0, 0, 0, 0)
    # Explicitly specify labels to ensure 2x2 matrix even if only one class is present
    cm = confusion_matrix(gt_arr, pred_arr, labels=[0, 1])
    tn, fp, fn, tp = cm.ravel()
    return {
        'accuracy': float(accuracy_score(gt_arr, pred_arr)),
        'precision': float(precision_score(gt_arr, pred_arr, zero_division=0)),
        'recall': float(recall_score(gt_arr, pred_arr, zero_division=0)),
        'f1': float(f1_score(gt_arr, pred_arr, zero_division=0)),
        'confusion_matrix': {
            'true_negative': int(tn),
            'false_positive': int(fp),
            'false_negative': int(fn),
            'true_positive': int(tp),
        },
        'total_sheets': len(gt_arr),
        'misclassifications': misclassifications,
    }


@router.get('/statistics')
async def get_statistics(
    model_name: str = Query(None, description='Model name (optional, returns all if not specified)')
):
    """Get comprehensive statistics for one or all models."""
    available_models = [
        d.name for d in REPORTS_DIR.iterdir() if d.is_dir() and d.name not in ('groundtruth', 'groundtruth2')
    ]
    if model_name:
        if model_name not in available_models:
            raise HTTPException(status_code=404, detail=f'Model {model_name} not found')
        models_to_process = [model_name]
    else:
        models_to_process = available_models
    results = {}
    for model in models_to_process:
        file_level = compute_file_level_metrics(model)
        pii_sheet_level = compute_sheet_level_metrics(model, 'pii_sensitive')
        non_pii_sheet_level = compute_sheet_level_metrics(model, 'non_pii_sensitive')
        results[model] = {
            'file_level': file_level,
            'sheet_level_pii': pii_sheet_level,
            'sheet_level_non_pii': non_pii_sheet_level,
        }
    return {
        'models': results,
        'available_models': available_models,
    }


@router.post('/compare-models')
async def compare_models(
    dataset_filename: str = Query(..., description='Name of the dataset file'),
):
    """Compare all models' predictions with ground truth for a specific dataset."""
    # Get ground truth
    gt_file = GROUNDTRUTH_DIR / f'{dataset_filename}.json'
    if not gt_file.exists():
        raise HTTPException(status_code=404, detail='Ground truth file not found')

    gt_data = load_json_file(gt_file)

    # Get all available models
    available_models = [
        d.name for d in REPORTS_DIR.iterdir() if d.is_dir() and d.name not in ('groundtruth', 'groundtruth2')
    ]

    # Load predictions from all models
    model_predictions = {}
    for model in available_models:
        model_file = REPORTS_DIR / model / f'{dataset_filename}.json'
        if model_file.exists():
            model_predictions[model] = load_json_file(model_file)

    # Build comparison structure
    comparison = {
        'dataset_filename': dataset_filename,
        'ground_truth': gt_data,
        'models': list(model_predictions.keys()),
        'sheets': [],
    }

    # For each sheet in ground truth
    for sheet_idx, gt_sheet in enumerate(gt_data):
        sheet_comparison = {
            'sheet_name': gt_sheet.get('sheet_name', f'Sheet {sheet_idx}'),
            'n_records': gt_sheet.get('n_records'),
            'n_columns': gt_sheet.get('n_columns'),
            'ground_truth': {
                'pii_sensitive': gt_sheet.get('pii_sensitive', False),
                'non_pii_sensitive': gt_sheet.get('non_pii_sensitive', False),
            },
            'model_predictions': {},
            'columns': [],
        }

        # Get predictions from each model for this sheet
        for model, pred_data in model_predictions.items():
            if sheet_idx < len(pred_data):
                pred_sheet = pred_data[sheet_idx]
                sheet_comparison['model_predictions'][model] = {
                    'pii_sensitive': pred_sheet.get('pii_sensitive', False),
                    'non_pii_sensitive': pred_sheet.get('non_pii_sensitive', False),
                    'non_pii_sensitivity_explanation': pred_sheet.get('non_pii', {}).get('explanation'),
                    'pii_correct': pred_sheet.get('pii_sensitive', False) == gt_sheet.get('pii_sensitive', False),
                    'non_pii_correct': pred_sheet.get('non_pii_sensitive', False)
                    == gt_sheet.get('non_pii_sensitive', False),
                }

        # For each column in ground truth
        if gt_sheet.get('columns'):
            for col_idx, gt_col in enumerate(gt_sheet['columns']):
                col_comparison = {
                    'column_name': gt_col.get('column_name'),
                    'sample_values': gt_col.get('sample_values', []),
                    'ground_truth': {
                        'pii_entity_type': gt_col.get('pii', {}).get('entity_type'),
                        'pii_sensitive': gt_col.get('pii', {}).get('sensitive', False),
                        'non_pii_sensitivity': gt_col.get('non_pii', {}).get('sensitivity'),
                    },
                    'model_predictions': {},
                }

                # Get predictions from each model for this column
                for model, pred_data in model_predictions.items():
                    if sheet_idx < len(pred_data) and pred_data[sheet_idx].get('columns'):
                        pred_columns = pred_data[sheet_idx]['columns']
                        if col_idx < len(pred_columns):
                            pred_col = pred_columns[col_idx]

                            gt_pii_sensitive = gt_col.get('pii', {}).get('sensitive', False)
                            pred_pii_sensitive = pred_col.get('pii', {}).get('sensitive', False)

                            gt_non_pii = gt_col.get('non_pii', {}).get('sensitivity')
                            pred_non_pii = pred_col.get('non_pii', {}).get('sensitivity')

                            col_comparison['model_predictions'][model] = {
                                'pii_entity_type': pred_col.get('pii', {}).get('entity_type'),
                                'pii_sensitive': pred_pii_sensitive,
                                'non_pii_sensitivity': pred_non_pii,
                                'pii_correct': pred_pii_sensitive == gt_pii_sensitive,
                                'non_pii_correct': pred_non_pii == gt_non_pii,
                            }

                sheet_comparison['columns'].append(col_comparison)

        comparison['sheets'].append(sheet_comparison)

    return comparison
