from fastapi import APIRouter, HTTPException, Query
from pathlib import Path
from fastapi import UploadFile, File
import shutil
import json
import os
import logging
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
from typing import Dict, List, Any
from datetime import datetime

# Import from clean architecture
from src.domain.entities import SheetReport
from src.application.use_cases.process_dataset import ProcessDatasetUseCase
from src.infrastructure.llm.azure_openai_provider import AzureOpenAIProvider
from src.infrastructure.storage.data_loader import SmartDataLoader
from src.shared.utils.prompt_manager import PromptManager
from src.shared.utils.json_serializer import make_json_serializable
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

router = APIRouter()
DATASETS_DIR = Path('/Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/research/data')  # change this
REPORTS_DIR = Path('/Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/research/results/test_results')  # change this
GROUNDTRUTH_DIR = REPORTS_DIR / 'groundtruth2'
ALLOWED_EXTENSIONS = {'.csv', '.xlsx'}


def setup_pipeline(model_name: str = 'gpt-4.1-nano') -> ProcessDatasetUseCase:
    """
    Setup the complete pipeline with all dependencies.

    This demonstrates dependency injection - we create all the
    infrastructure components and inject them into the use case.

    Args:
        model_name: Name of the model to use for all LLM tasks

    Returns:
        Configured ProcessDatasetUseCase
    """
    logger.info(f'Setting up pipeline with model: {model_name}')

    # 1. Create data loader
    data_loader = SmartDataLoader(max_rows=1000)

    # 2. Create LLM providers (using same model for all tasks)
    azure_endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')
    api_key = os.getenv('AZURE_OPENAI_API_KEY')

    pii_llm = AzureOpenAIProvider(
        model_name=model_name,
        azure_endpoint=azure_endpoint,
        api_key=api_key,
    )

    pii_reflection_llm = AzureOpenAIProvider(
        model_name=model_name,
        azure_endpoint=azure_endpoint,
        api_key=api_key,
    )

    non_pii_llm = AzureOpenAIProvider(
        model_name=model_name,
        azure_endpoint=azure_endpoint,
        api_key=api_key,
    )

    # 3. Create prompt manager
    prompt_manager = PromptManager(prompts_dir='src/prompts')

    # 4. Create use case with all dependencies
    use_case = ProcessDatasetUseCase(
        data_loader=data_loader,
        pii_llm_provider=pii_llm,
        pii_reflection_llm_provider=pii_reflection_llm,
        non_pii_llm_provider=non_pii_llm,
        prompt_manager=prompt_manager,
        sample_size=5,
    )

    logger.info('Pipeline setup complete!')
    return use_case


def load_isp_rules(country: str = 'default') -> dict:
    """
    Load ISP (Information Sensitivity Protocol) rules from data/isps.json.

    Args:
        country: Country name to load ISP rules for (default: "default")

    Returns:
        Dictionary containing ISP rules for the specified country
    """
    isp_file = Path('data/isps.json')

    if not isp_file.exists():
        logger.warning(f'ISP rules file not found: {isp_file}')
        exit()
        return {}

    try:
        with open(isp_file, 'r') as f:
            all_isps = json.load(f)

        # If requesting default, return it directly
        if country == 'default':
            isp_rules = all_isps.get('default', {})
            logger.info('Loaded default ISP rules')
            return isp_rules

        # Search through ISP entries to find matching country
        # ISP keys are like "OCHA Afghanistan" but the country field inside is "afghanistan"
        country_lower = country.lower()
        
        for isp_key, isp_data in all_isps.items():
            if isp_key == 'default':
                continue
            
            # Check if the country field matches (case-insensitive)
            isp_country = isp_data.get('country', '').lower()
            
            # Match if the country field contains or equals the search term
            if isp_country == country_lower or country_lower in isp_country or isp_country in country_lower:
                logger.info(f'Loaded ISP rules for: {isp_key} (matched country: {isp_country})')
                return isp_data
        
        # If no match found, use default
        isp_rules = all_isps.get('default', {})
        logger.info(f"Country '{country}' not found in ISPs, using default ISP rules")
        return isp_rules

    except Exception as e:
        logger.error(f'Failed to load ISP rules: {e}')
        return {}


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
    """
    Generate a sensitivity report for a dataset using the clean architecture pipeline.
    
    Args:
        dataset_filename: Name of the dataset file
        model_name: Name of the model to use for classification
        
    Returns:
        Report data including sensitivity classification
    """
    report_file = REPORTS_DIR / model_name / f'{dataset_filename}.json'
    
    # Check if report file exists
    if report_file.exists():
        # Load report file
        logger.info(f'Loading existing report: {report_file}')
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
    
    logger.info(f'Generating new report for: {dataset_filename} with model: {model_name}')
    
    try:
        # Setup pipeline with specified model
        pipeline = setup_pipeline(model_name=model_name)
        
        # Load ISP rules (using default for now)
        isp_rules = load_isp_rules('default')
        
        # Process dataset using the clean architecture
        sheet_reports: List[SheetReport] = pipeline.execute(
            source=str(dataset_file),
            resource_id=dataset_filename,
            is_url=False,
            isp_rules=isp_rules,
        )
        
        # Convert SheetReport entities to dictionaries
        reports_dict = [report.to_dict() for report in sheet_reports]
        
        # Determine overall sensitivity (any sheet is sensitive)
        is_sensitive = any(report.is_sensitive() for report in sheet_reports)
        sensitivity = 'SENSITIVE' if is_sensitive else 'NON-SENSITIVE'
        
        # Ensure report directory exists
        report_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Save report to file
        with report_file.open('w', encoding='utf-8') as f:
            json.dump(reports_dict, f, indent=2, ensure_ascii=False)
        
        logger.info(f'Report saved successfully: {report_file}')
        
        return {
            'message': 'Report generated successfully',
            'report_path': str(report_file),
            'sensitivity': sensitivity,
            'reports': reports_dict,
        }
        
    except Exception as e:
        logger.error(f'Failed to generate report: {e}', exc_info=True)
        raise HTTPException(status_code=500, detail=f'Failed to generate report: {str(e)}')


# Generate reports for all models
@router.post('/generate-all-reports')
async def generate_all_reports(
    dataset_filename: str = Query(..., description='Name of the dataset file'),
):
    """
    Generate sensitivity reports for a dataset using ALL available models.
    
    Args:
        dataset_filename: Name of the dataset file
        
    Returns:
        Summary of generated reports
    """
    # Available models
    models = ['gpt-5-nano', 'gpt-5-mini', 'gpt-4.1-nano', 'gpt-4.1-mini', 'gpt-4.1', 'DeepSeek-V3.1']
    
    # Read dataset file
    dataset_file = DATASETS_DIR / dataset_filename
    if not dataset_file.exists():
        raise HTTPException(status_code=404, detail='Dataset file not found')
    
    logger.info(f'Generating reports for all models: {dataset_filename}')
    
    results = {}
    for model_name in models:
        report_file = REPORTS_DIR / model_name / f'{dataset_filename}.json'
        
        # Skip if report already exists
        if report_file.exists():
            logger.info(f'Report already exists for {model_name}, skipping')
            results[model_name] = {'status': 'exists', 'report_path': str(report_file)}
            continue
        
        try:
            # Setup pipeline with specified model
            pipeline = setup_pipeline(model_name=model_name)
            
            # Load ISP rules
            isp_rules = load_isp_rules('default')
            
            # Process dataset
            sheet_reports: List[SheetReport] = pipeline.execute(
                source=str(dataset_file),
                resource_id=dataset_filename,
                is_url=False,
                isp_rules=isp_rules,
            )
            
            # Convert to dictionaries
            reports_dict = [report.to_dict() for report in sheet_reports]
            
            # Determine overall sensitivity
            is_sensitive = any(report.is_sensitive() for report in sheet_reports)
            sensitivity = 'SENSITIVE' if is_sensitive else 'NON-SENSITIVE'
            
            # Ensure report directory exists
            report_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Save report
            with report_file.open('w', encoding='utf-8') as f:
                json.dump(reports_dict, f, indent=2, ensure_ascii=False)
            
            logger.info(f'Report saved for {model_name}: {report_file}')
            results[model_name] = {
                'status': 'generated',
                'report_path': str(report_file),
                'sensitivity': sensitivity,
            }
            
        except Exception as e:
            logger.error(f'Failed to generate report for {model_name}: {e}', exc_info=True)
            results[model_name] = {'status': 'error', 'error': str(e)}
    
    return {
        'message': 'Batch generation complete',
        'dataset': dataset_filename,
        'results': results,
    }



# Create empty ground truth template
@router.post('/create-groundtruth-template')
async def create_groundtruth_template(
    dataset_filename: str = Query(..., description='Name of the dataset file'),
):
    """
    Create an empty ground truth template for manual annotation.
    
    This endpoint reads the file structure WITHOUT running any LLM processing,
    creating a template with TODO placeholders for manual annotation.
    
    Args:
        dataset_filename: Name of the dataset file
        
    Returns:
        Path to created template
    """
    # Read dataset file
    dataset_file = DATASETS_DIR / dataset_filename
    if not dataset_file.exists():
        raise HTTPException(status_code=404, detail='Dataset file not found')
    
    gt_file = GROUNDTRUTH_DIR / f'{dataset_filename}.json'
    
    # Check if template already exists
    if gt_file.exists():
        return {
            'message': 'Ground truth template already exists',
            'template_path': str(gt_file),
            'exists': True,
        }
    
    logger.info(f'Creating ground truth template for: {dataset_filename}')
    
    try:
        # Load data without LLM processing - just get structure
        data_loader = SmartDataLoader(max_rows=1000)
        
        if dataset_file.suffix.lower() == '.csv':
            sheets = data_loader.load_from_file(str(dataset_file))
        else:
            sheets = data_loader.load_from_file(str(dataset_file))

        
        # Create template with placeholders
        template = []
        
        for sheet_name, df in sheets.items():
            # Sample the data
            sample_dict = data_loader.sample_dataframe(df, sample_size=5)
            
            # Create columns with TODO placeholders
            columns = []
            for col_name, sample_values in sample_dict.items():
                # Convert any datetime objects in sample_values to strings
                serializable_values = make_json_serializable(sample_values)
                
                columns.append({
                    'column_name': col_name,
                    'sample_values': serializable_values,
                    'pii': {
                        'entity_type': 'TODO',
                        'sensitive': False
                    }
                })
            
            # Create sheet report with placeholders
            sheet_report = {
                'resource_id': None,
                'file_name': str(dataset_file),
                'file_url': None,
                'sheet_name': sheet_name,
                'processing_timestamp': datetime.now().isoformat(),
                'processing_success': True,
                'n_records': len(df),
                'n_columns': len(sample_dict),
                'completion_tokens': 0,
                'prompt_tokens': 0,
                'pii_sensitive': False,
                'non_pii_sensitive': False,
                'columns': columns
            }
            
            template.append(sheet_report)
        
        # Ensure directory exists
        gt_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Save template
        with gt_file.open('w', encoding='utf-8') as f:
            json.dump(template, f, indent=4, ensure_ascii=False)
        
        logger.info(f'Ground truth template created: {gt_file}')
        
        return {
            'message': 'Ground truth template created successfully',
            'template_path': str(gt_file),
            'exists': False,
            'sheets': len(template),
        }
        
    except Exception as e:
        logger.error(f'Failed to create ground truth template: {e}', exc_info=True)
        raise HTTPException(status_code=500, detail=f'Failed to create template: {str(e)}')


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


def compute_file_level_pii_metrics(model_name: str) -> Dict[str, Any]:
    """Compute file-level PII sensitivity metrics."""
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
        # File has PII if ANY sheet has pii_sensitive
        gt_pii = any(sheet.get('pii_sensitive', False) for sheet in gt_data)
        pred_pii = any(sheet.get('pii_sensitive', False) for sheet in pred_data)
        gt_arr.append(int(gt_pii))
        pred_arr.append(int(pred_pii))
        if gt_pii != pred_pii:
            misclassifications.append(
                {
                    'file': filename,
                    'true_label': 'Has PII' if gt_pii else 'No PII',
                    'predicted_label': 'Has PII' if pred_pii else 'No PII',
                    'error_type': 'False Negative' if gt_pii and not pred_pii else 'False Positive',
                }
            )
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


def compute_file_level_non_pii_metrics(model_name: str) -> Dict[str, Any]:
    """Compute file-level non-PII sensitivity metrics."""
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
        # File has non-PII sensitive if ANY sheet has non_pii_sensitive
        gt_non_pii = any(sheet.get('non_pii_sensitive', False) for sheet in gt_data)
        pred_non_pii = any(sheet.get('non_pii_sensitive', False) for sheet in pred_data)
        gt_arr.append(int(gt_non_pii))
        pred_arr.append(int(pred_non_pii))
        if gt_non_pii != pred_non_pii:
            misclassifications.append(
                {
                    'file': filename,
                    'true_label': 'Has Non-PII Sensitive' if gt_non_pii else 'No Non-PII Sensitive',
                    'predicted_label': 'Has Non-PII Sensitive' if pred_non_pii else 'No Non-PII Sensitive',
                    'error_type': 'False Negative' if gt_non_pii and not pred_non_pii else 'False Positive',
                }
            )
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
        file_level_pii = compute_file_level_pii_metrics(model)
        file_level_non_pii = compute_file_level_non_pii_metrics(model)
        pii_sheet_level = compute_sheet_level_metrics(model, 'pii_sensitive')
        non_pii_sheet_level = compute_sheet_level_metrics(model, 'non_pii_sensitive')
        results[model] = {
            'file_level': file_level,
            'file_level_pii': file_level_pii,
            'file_level_non_pii': file_level_non_pii,
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


@router.get('/cost-analysis')
async def get_cost_analysis():
    """
    Calculate token usage and costs for all models across all reports.
    
    Returns:
        Dictionary with cost analysis for each model
    """
    # Model pricing per 1M tokens (both prompt and completion use same rate)
    PRICING = {
        'gpt-4.1-nano': 0.17,
        'gpt-4.1-mini': 0.7,
        'gpt-4.1': 3.5,
        'gpt-5-nano': 0.15,
        'gpt-5-mini': 0.69,
        'DeepSeek-V3.1': 0.84,
    }
    
    # Get all available models
    available_models = [
        d.name for d in REPORTS_DIR.iterdir() 
        if d.is_dir() and d.name not in ('groundtruth', 'groundtruth2')
    ]
    
    cost_analysis = {}
    
    for model in available_models:
        model_dir = REPORTS_DIR / model
        
        total_prompt_tokens = 0
        total_completion_tokens = 0
        total_tokens = 0
        reports_processed = 0
        reports_with_errors = 0
        
        # Iterate through all JSON files for this model
        for report_file in model_dir.glob('*.json'):
            try:
                with open(report_file, 'r', encoding='utf-8') as f:
                    report_data = json.load(f)
                
                # report_data is a list of sheet reports
                if isinstance(report_data, list):
                    for sheet in report_data:
                        if isinstance(sheet, dict):
                            prompt_tokens = sheet.get('prompt_tokens', 0)
                            completion_tokens = sheet.get('completion_tokens', 0)
                            
                            if prompt_tokens or completion_tokens:
                                total_prompt_tokens += prompt_tokens
                                total_completion_tokens += completion_tokens
                                total_tokens += (prompt_tokens + completion_tokens)
                    
                    reports_processed += 1
                    
            except Exception as e:
                logger.error(f'Error processing {report_file}: {e}')
                reports_with_errors += 1
                continue
        
        # Calculate cost
        price_per_million = PRICING.get(model, 0)
        total_cost = (total_tokens / 1_000_000) * price_per_million
        
        cost_analysis[model] = {
            'prompt_tokens': total_prompt_tokens,
            'completion_tokens': total_completion_tokens,
            'total_tokens': total_tokens,
            'reports_processed': reports_processed,
            'reports_with_errors': reports_with_errors,
            'price_per_million': price_per_million,
            'total_cost_usd': round(total_cost, 4),
            'cost_per_report': round(total_cost / reports_processed, 4) if reports_processed > 0 else 0,
        }
    
    return {
        'models': cost_analysis,
        'pricing': PRICING,
    }
