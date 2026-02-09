"""
Analyze PII entity type classifications across models.
Only analyze files that exist in ALL models (intersection).
"""

import json
import os
from pathlib import Path
from collections import defaultdict

RESULTS_DIR = Path('research/results/test_results')

def get_model_files(model_name):
    """Get set of filenames for a model."""
    model_dir = RESULTS_DIR / model_name
    if not model_dir.exists():
        return set()
    return set(f.name for f in model_dir.glob('*.json'))


def analyze_model_classifications(model_name, common_files):
    """Analyze PII entity classifications for a specific model, only for common files."""
    model_dir = RESULTS_DIR / model_name
    
    if not model_dir.exists():
        print(f"Model directory not found: {model_dir}")
        return None
    
    stats = {
        'total_columns': 0,
        'none_count': 0,
        'entity_types': defaultdict(int),
        'total_prompt_tokens': 0,
        'total_completion_tokens': 0,
        'files_processed': 0,
    }
    
    # Process only common files
    for filename in common_files:
        json_file = model_dir / filename
        
        if not json_file.exists():
            continue
            
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            stats['files_processed'] += 1
            
            # data is a list of sheet reports
            if isinstance(data, list):
                for sheet in data:
                    if isinstance(sheet, dict):
                        # Count tokens
                        stats['total_prompt_tokens'] += sheet.get('prompt_tokens', 0)
                        stats['total_completion_tokens'] += sheet.get('completion_tokens', 0)
                        
                        # Analyze columns
                        columns = sheet.get('columns', [])
                        for col in columns:
                            stats['total_columns'] += 1
                            
                            pii_info = col.get('personal_data', {})
                            entity_type = pii_info.get('entity_type', 'Unknown')
                            
                            # Normalize entity type (case-insensitive)
                            entity_type_normalized = entity_type.strip() if entity_type else 'Unknown'
                            
                            # Count entity types
                            stats['entity_types'][entity_type_normalized] += 1
                            
                            # Count "None" specifically
                            if entity_type_normalized.lower() == 'none':
                                stats['none_count'] += 1
        
        except Exception as e:
            print(f"Error processing {json_file}: {e}")
            continue
    
    return stats


def main():
    # Get all available models (excluding groundtruth)
    models = [d.name for d in RESULTS_DIR.iterdir() 
              if d.is_dir() and d.name not in ('groundtruth', 'groundtruth2')]
    
    if not models:
        print("No models found!")
        return
    
    print("=" * 80)
    print("Finding common files across all models...")
    print("=" * 80)
    
    # Get files for each model
    model_files = {}
    for model in models:
        files = get_model_files(model)
        model_files[model] = files
        print(f"{model}: {len(files)} files")
    
    # Find intersection of all files
    common_files = set.intersection(*model_files.values()) if model_files else set()
    
    print(f"\nCommon files across ALL models: {len(common_files)}")
    print("=" * 80)
    
    if not common_files:
        print("No common files found across all models!")
        return
    
    print("\nCommon files:")
    for filename in sorted(common_files):
        print(f"  - {filename}")
    
    print("\n" + "=" * 80)
    print("PII Entity Type Analysis (Common Files Only)")
    print("=" * 80)
    print()
    
    all_stats = {}
    
    for model in sorted(models):
        print(f"\n{'='*80}")
        print(f"Model: {model}")
        print(f"{'='*80}")
        
        stats = analyze_model_classifications(model, common_files)
        
        if stats:
            all_stats[model] = stats
            
            print(f"Files processed: {stats['files_processed']}")
            print(f"Total columns analyzed: {stats['total_columns']}")
            print(f"Columns classified as 'None': {stats['none_count']}")
            
            if stats['total_columns'] > 0:
                print(f"Percentage 'None': {stats['none_count'] / stats['total_columns'] * 100:.2f}%")
            
            print(f"\nToken usage:")
            print(f"  Prompt tokens: {stats['total_prompt_tokens']:,}")
            print(f"  Completion tokens: {stats['total_completion_tokens']:,}")
            print(f"  Total tokens: {stats['total_prompt_tokens'] + stats['total_completion_tokens']:,}")
            
            if stats['total_columns'] > 0:
                print(f"  Tokens per column: {stats['total_prompt_tokens'] / stats['total_columns']:.1f}")
            
            print(f"\nTop 10 Entity Types:")
            sorted_entities = sorted(stats['entity_types'].items(), 
                                    key=lambda x: x[1], reverse=True)[:10]
            for entity_type, count in sorted_entities:
                percentage = count / stats['total_columns'] * 100 if stats['total_columns'] > 0 else 0
                print(f"  {entity_type:30s}: {count:6d} ({percentage:5.2f}%)")
    
    # Comparison summary
    print("\n" + "=" * 80)
    print("COMPARISON SUMMARY (Common Files Only)")
    print("=" * 80)
    print()
    
    print(f"{'Model':<20} {'Files':>7} {'Cols':>8} {'None':>8} {'None %':>9} {'Prompt Tok':>12} {'Tok/Col':>10}")
    print("-" * 90)
    
    for model in sorted(all_stats.keys()):
        stats = all_stats[model]
        none_pct = stats['none_count'] / stats['total_columns'] * 100 if stats['total_columns'] > 0 else 0
        tok_per_col = stats['total_prompt_tokens'] / stats['total_columns'] if stats['total_columns'] > 0 else 0
        
        print(f"{model:<20} {stats['files_processed']:>7} {stats['total_columns']:>8,} {stats['none_count']:>8,} {none_pct:>8.2f}% {stats['total_prompt_tokens']:>12,} {tok_per_col:>10.1f}")
    
    # Find the model with highest "None" percentage
    if all_stats:
        highest_none_model = max(all_stats.items(), 
                                 key=lambda x: x[1]['none_count'] / x[1]['total_columns'] if x[1]['total_columns'] > 0 else 0)
        lowest_none_model = min(all_stats.items(), 
                                key=lambda x: x[1]['none_count'] / x[1]['total_columns'] if x[1]['total_columns'] > 0 else 0)
        
        print("\n" + "=" * 80)
        print("KEY FINDINGS")
        print("=" * 80)
        
        highest_pct = highest_none_model[1]['none_count'] / highest_none_model[1]['total_columns'] * 100
        lowest_pct = lowest_none_model[1]['none_count'] / lowest_none_model[1]['total_columns'] * 100
        
        print(f"\nHighest 'None' classification: {highest_none_model[0]} ({highest_pct:.2f}%)")
        print(f"Lowest 'None' classification: {lowest_none_model[0]} ({lowest_pct:.2f}%)")
        print(f"Difference: {highest_pct - lowest_pct:.2f} percentage points")
        
        # Token comparison
        if 'gpt-4.1-nano' in all_stats:
            nano_stats = all_stats['gpt-4.1-nano']
            nano_none_pct = nano_stats['none_count'] / nano_stats['total_columns'] * 100 if nano_stats['total_columns'] > 0 else 0
            nano_tok_per_col = nano_stats['total_prompt_tokens'] / nano_stats['total_columns'] if nano_stats['total_columns'] > 0 else 0
            
            print(f"\n{'='*80}")
            print("gpt-4.1-nano Analysis:")
            print(f"{'='*80}")
            print(f"  Files processed: {nano_stats['files_processed']}")
            print(f"  Total columns: {nano_stats['total_columns']:,}")
            print(f"  'None' classifications: {nano_stats['none_count']:,} ({nano_none_pct:.2f}%)")
            print(f"  Prompt tokens: {nano_stats['total_prompt_tokens']:,}")
            print(f"  Tokens per column: {nano_tok_per_col:.1f}")
            
            # Compare with other models
            print(f"\nComparison with other models:")
            print(f"{'Model':<20} {'None % Diff':>15} {'Tok/Col Diff':>15} {'Total Tok Diff':>18}")
            print("-" * 70)
            
            for model, stats in sorted(all_stats.items()):
                if model != 'gpt-4.1-nano' and stats['total_columns'] > 0:
                    other_none_pct = stats['none_count'] / stats['total_columns'] * 100
                    other_tok_per_col = stats['total_prompt_tokens'] / stats['total_columns']
                    
                    none_diff = nano_none_pct - other_none_pct
                    tok_col_diff = nano_tok_per_col - other_tok_per_col
                    token_diff = nano_stats['total_prompt_tokens'] - stats['total_prompt_tokens']
                    
                    print(f"{model:<20} {none_diff:>+14.2f}% {tok_col_diff:>+14.1f} {token_diff:>+18,}")


if __name__ == '__main__':
    main()
