from typing import Any

COST_MAPPING = {
    'gpt-5-mini': {'input': 0.24, 'output': 1.89},
    'gpt-5-nano': {'input': 0.05, 'output': 0.38},
    'gpt-4.1': {'input': 1.89, 'output': 7.53},
    'gpt-4.1-mini': {'input': 0.38, 'output': 1.51},
    'gpt-4.1-nano': {'input': 0.10, 'output': 0.38},
    'DeepSeek-V3.1': {'input': 1.05, 'output': 4.22},
    'DeepSeek-V4-Flash': {'input': 0.21, 'output': 0.85},
}

def calculate_token_costs(completion_tokens, prompt_tokens, model: str):
    cost_mapping = COST_MAPPING[model]
    return (prompt_tokens / 1000000) * cost_mapping['input'] + (completion_tokens / 1000000) * cost_mapping['output'] 
    

def calculate_report_costs(report: dict[str, list[dict[str, Any]]], model):
    completion_tokens, prompt_tokens = 0, 0
    for report in report['sdd_report']:
        completion_tokens += report['completion_tokens']
        prompt_tokens += report['prompt_tokens']

    return calculate_token_costs(completion_tokens, prompt_tokens, model)
    

if __name__ == '__main__':
    import json
    path = 'research/results/test_results/DeepSeek-V3.1/YEM Displacement Tracking Jan 11-17 2026.xlsx.json'

    with open(path, 'r') as f:
        report = json.load(f)

    calculate_report_costs(report, 'DeepSeek-V3.1')