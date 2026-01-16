"""
Tutorial: How to Use the HDX SSD Pipeline

This tutorial demonstrates how to process datasets for sensitive data detection
using the refactored clean architecture.

Features demonstrated:
1. Process a single dataset from URL
2. Process a single dataset from local file
3. Process multiple datasets from a folder
4. Customize LLM providers
5. Access and use results
"""

import os
import json
import logging
from pathlib import Path
from typing import List
from dotenv import load_dotenv

# Import from our clean architecture
from src.domain.entities import SheetReport
from src.application.use_cases.process_dataset import ProcessDatasetUseCase
from src.infrastructure.llm.azure_openai_provider import AzureOpenAIProvider
from src.infrastructure.storage.data_loader import SmartDataLoader
from src.shared.utils.prompt_manager import PromptManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def setup_pipeline() -> ProcessDatasetUseCase:
    """
    Setup the complete pipeline with all dependencies.
    
    This demonstrates dependency injection - we create all the
    infrastructure components and inject them into the use case.
    
    Returns:
        Configured ProcessDatasetUseCase
    """
    logger.info("Setting up pipeline...")
    
    # 1. Create data loader
    data_loader = SmartDataLoader(max_rows=1000)
    
    # 2. Create LLM providers (can use different models for each task)
    pii_llm = AzureOpenAIProvider(
        model_name=os.getenv('PII_DETECT_MODEL', 'gpt-4.1-nano'),
        azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT'),
        api_key=os.getenv('AZURE_OPENAI_API_KEY'),
    )
    
    pii_reflection_llm = AzureOpenAIProvider(
        model_name=os.getenv('PII_REFLECT_MODEL', 'gpt-4.1-nano'),
        azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT'),
        api_key=os.getenv('AZURE_OPENAI_API_KEY'),
    )
    
    non_pii_llm = AzureOpenAIProvider(
        model_name=os.getenv('NON_PII_DETECT_MODEL', 'gpt-4.1-nano'),
        azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT'),
        api_key=os.getenv('AZURE_OPENAI_API_KEY'),
    )
    
    # 3. Create prompt manager
    prompt_manager = PromptManager(prompts_dir='prompts')
    
    # 4. Create use case with all dependencies
    use_case = ProcessDatasetUseCase(
        data_loader=data_loader,
        pii_llm_provider=pii_llm,
        pii_reflection_llm_provider=pii_reflection_llm,
        non_pii_llm_provider=non_pii_llm,
        prompt_manager=prompt_manager,
        sample_size=5
    )
    
    logger.info("Pipeline setup complete!")
    return use_case


def example_1_process_url():
    """
    Example 1: Process a single dataset from URL
    
    This is the most common use case - processing a dataset
    directly from a URL.
    """
    print("\n" + "="*60)
    print("EXAMPLE 1: Process Dataset from URL")
    print("="*60 + "\n")
    
    # Setup pipeline
    pipeline = setup_pipeline()
    
    # Process dataset from URL
    url = "https://example.com/data.csv"
    
    logger.info(f"Processing dataset from URL: {url}")
    
    try:
        reports = pipeline.execute(
            source=url,
            resource_id="example-resource-123",
            is_url=True
        )
        
        # Display results
        print_results(reports)
        
        # Save results
        save_results(reports, "output/url_example.json")
        
    except Exception as e:
        logger.error(f"Failed to process URL: {e}")


def example_2_process_local_file():
    """
    Example 2: Process a single dataset from local file
    
    Useful for testing or processing files that are already downloaded.
    """
    print("\n" + "="*60)
    print("EXAMPLE 2: Process Dataset from Local File")
    print("="*60 + "\n")
    
    # Setup pipeline
    pipeline = setup_pipeline()
    
    # Process local file
    file_path = "research/data/panama.xlsx"
    
    if not Path(file_path).exists():
        logger.warning(f"File not found: {file_path}")
        logger.info("Skipping example 2")
        return
    
    logger.info(f"Processing local file: {file_path}")
    
    try:
        reports = pipeline.execute(
            source=file_path,
            resource_id="local-file-123",
            is_url=False
        )
        
        # Display results
        print_results(reports)
        
        # Save results
        save_results(reports, "output/local_file_example.json")
        
    except Exception as e:
        logger.error(f"Failed to process file: {e}")


def example_3_process_folder():
    """
    Example 3: Process multiple datasets from a folder
    
    Batch processing - useful for processing many files at once.
    """
    print("\n" + "="*60)
    print("EXAMPLE 3: Process Multiple Datasets from Folder")
    print("="*60 + "\n")
    
    # Setup pipeline
    pipeline = setup_pipeline()
    
    # Process all files in folder
    folder_path = Path("research/data")
    
    if not folder_path.exists():
        logger.warning(f"Folder not found: {folder_path}")
        logger.info("Skipping example 3")
        return
    
    # Find all supported files
    supported_extensions = ('.csv', '.xlsx', '.xls')
    files = [
        f for f in folder_path.iterdir() 
        if f.is_file() and f.suffix.lower() in supported_extensions
    ]
    
    logger.info(f"Found {len(files)} files to process")
    
    all_reports = []
    
    for file_path in files:
        logger.info(f"Processing: {file_path.name}")
        
        try:
            reports = pipeline.execute(
                source=str(file_path),
                resource_id=f"batch-{file_path.stem}",
                is_url=False
            )
            
            all_reports.extend(reports)
            
            # Print summary for this file
            print(f"\n{file_path.name}:")
            print(f"  Sheets: {len(reports)}")
            for report in reports:
                sensitivity = "SENSITIVE" if report.is_sensitive() else "NON-SENSITIVE"
                print(f"  - {report.sheet_name}: {sensitivity}")
            
        except Exception as e:
            logger.error(f"Failed to process {file_path.name}: {e}")
    
    # Save all results
    save_results(all_reports, "output/batch_results.json")
    
    print(f"\nTotal sheets processed: {len(all_reports)}")


def example_4_custom_configuration():
    """
    Example 4: Custom configuration with ISP rules
    
    Demonstrates how to customize the pipeline with specific
    Information Sensitivity Protocol rules.
    """
    print("\n" + "="*60)
    print("EXAMPLE 4: Custom Configuration with ISP Rules")
    print("="*60 + "\n")
    
    # Setup pipeline
    pipeline = setup_pipeline()
    
    # Custom ISP rules for a specific country/context
    isp_rules = {
        "country": "Ukraine",
        "rules": {
            "location_data": "HIGH_SENSITIVE",
            "demographic_data": "MODERATE_SENSITIVE",
            "health_data": "SEVERE_SENSITIVE"
        },
        "context": "Humanitarian crisis - extra caution required"
    }
    
    file_path = "research/data/panama.xlsx"
    
    if not Path(file_path).exists():
        logger.warning(f"File not found: {file_path}")
        logger.info("Skipping example 4")
        return
    
    logger.info("Processing with custom ISP rules...")
    
    try:
        reports = pipeline.execute(
            source=file_path,
            resource_id="custom-isp-123",
            is_url=False,
            isp_rules=isp_rules
        )
        
        print_results(reports)
        
    except Exception as e:
        logger.error(f"Failed to process: {e}")


def print_results(reports: List[SheetReport]):
    """
    Print results in a human-readable format.
    
    Args:
        reports: List of SheetReports to display
    """
    print("\n" + "="*60)
    print("RESULTS")
    print("="*60)
    
    for report in reports:
        print(f"\n📊 Sheet: {report.sheet_name}")
        print(f"   File: {report.file_name}")
        print(f"   Rows: {report.n_records:,}")
        print(f"   Columns: {report.n_columns}")
        
        if report.is_readme:
            print("   Type: README/Metadata")
            continue
        
        # Overall sensitivity
        overall = "🔴 SENSITIVE" if report.is_sensitive() else "🟢 NON-SENSITIVE"
        print(f"   Overall: {overall}")
        
        # PII summary
        pii_columns = [col for col in report.columns if col.has_pii()]
        sensitive_pii = [col for col in pii_columns if col.is_sensitive()]
        
        if pii_columns:
            print(f"\n   PII Detected: {len(pii_columns)} columns")
            print(f"   Sensitive PII: {len(sensitive_pii)} columns")
            
            for col in sensitive_pii[:5]:  # Show first 5
                print(f"     - {col.name}: {col.pii_classification.entity_type}")
        
        # Non-PII sensitivity
        print(f"\n   Non-PII Sensitivity: {report.non_pii_classification.sensitivity}")
        
        # Token usage
        total_tokens = report.total_tokens()
        print(f"\n   Tokens Used: {total_tokens:,}")
        print(f"   Models: PII={report.pii_classifier_model}, "
              f"Reflection={report.pii_reflection_model}, "
              f"Non-PII={report.non_pii_model}")


def save_results(reports: List[SheetReport], output_path: str):
    """
    Save results to JSON file.
    
    Args:
        reports: List of SheetReports to save
        output_path: Path to output file
    """
    # Create output directory if needed
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Convert to dict
    results = [report.to_dict() for report in reports]
    
    # Save to file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Results saved to: {output_path}")


def main():
    """
    Run all examples.
    
    Uncomment the examples you want to run.
    """
    print("\n" + "="*60)
    print("HDX SSD Pipeline - Tutorial")
    print("="*60)
    
    # Example 1: Process from URL
    # Note: Requires valid URL and API credentials
    # example_1_process_url()
    
    # Example 2: Process local file
    example_2_process_local_file()
    
    # Example 3: Process folder
    example_3_process_folder()
    
    # Example 4: Custom ISP rules
    example_4_custom_configuration()
    
    print("\n" + "="*60)
    print("Tutorial Complete!")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
