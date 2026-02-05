"""
Compute the statistics for personal and non personal sensitive data detection per LLM
"""

import os
import json
import logging
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def load_json(file_path: str):
    """Load JSON file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def eval_sheet_level_sensitive_data(llm_model: str, key: str):
    """
    Evaluate the non personal sensitive data detection per LLM
    """
    # Load the groundtruth and predictions
    GROUNDTRUTH_FOLDER = 'research/results/test_results/groundtruth'
    prediction_folder = f'research/results/test_results/{llm_model}'
    # Compare file names in both folders
    groundtruth_files = os.listdir(GROUNDTRUTH_FOLDER)
    prediction_files = os.listdir(prediction_folder)
    # Only use files that are in both folders
    groundtruth_files = [file for file in groundtruth_files if file in prediction_files]
    prediction_files = [file for file in prediction_files if file in groundtruth_files]

    groundtruth_array = []
    predictions_array = []

    for file in groundtruth_files:
        groundtruth = load_json(f'{GROUNDTRUTH_FOLDER}/{file}')
        predictions = load_json(f'{prediction_folder}/{file}')
        # Compare the predictions with the groundtruth
        for idx in range(len(groundtruth)):
            groundtruth_item = groundtruth[idx]
            predictions_item = predictions[idx]

            non_personal_sensitive_data = groundtruth_item.get(key)
            predicted_non_personal_sensitive_data = predictions_item.get(key)

            groundtruth_array.append(non_personal_sensitive_data)
            predictions_array.append(predicted_non_personal_sensitive_data)

    # Map none to False
    groundtruth_array = [False if item is None else item for item in groundtruth_array]
    predictions_array = [False if item is None else item for item in predictions_array]
    logger.debug(f'Groundtruth array: {groundtruth_array}')
    logger.debug(f'Predictions array: {predictions_array}')

    # Convert to integers
    groundtruth_array = [int(item) for item in groundtruth_array]
    predictions_array = [int(item) for item in predictions_array]
    # Calculate the metrics
    metrics = {
        'accuracy': accuracy_score(groundtruth_array, predictions_array),
        'precision': precision_score(groundtruth_array, predictions_array, zero_division=0),
        'recall': recall_score(groundtruth_array, predictions_array, zero_division=0),
        'f1': f1_score(groundtruth_array, predictions_array, zero_division=0),
    }
    return metrics


def eval_personal_sensitive_data_column_level(llm_model: str):
    """
    Evaluate personal sensitive data detection per LLM (column level)
    Returns classification metrics + list of misclassified columns
    """
    GT_FOLDER = 'research/results/test_results/groundtruth'
    PRED_FOLDER = f'research/results/test_results/{llm_model}'

    groundtruth_files = os.listdir(GT_FOLDER)
    prediction_files = os.listdir(PRED_FOLDER)
    # Only use files that are in both folders
    groundtruth_files = [file for file in groundtruth_files if file in prediction_files]
    prediction_files = [file for file in prediction_files if file in groundtruth_files]
    assert set(groundtruth_files) == set(prediction_files)

    gt_arr, pred_arr, errors = [], [], []

    for file in groundtruth_files:
        gt_data = load_json(f'{GT_FOLDER}/{file}')
        pd_data = load_json(f'{PRED_FOLDER}/{file}')

        for i in range(len(gt_data)):
            gt_cols = gt_data[i]['columns']
            pd_cols = pd_data[i]['columns']

            pd_map = {c['column_name']: c for c in pd_cols}

            for col in gt_cols:
                col_name = col['column_name']
                gt_label = int(col['personal_data']['sensitive'])
                gt_arr.append(gt_label)

                default_col = {'personal_data': {'sensitive': 0}}
                pred_label = int(pd_map.get(col_name, default_col)['personal_data']['sensitive'])
                pred_arr.append(pred_label)

                if gt_label != pred_label:
                    errors.append(
                        {
                            'file': file,
                            'column_name': col_name,
                            'groundtruth': gt_label,
                            'prediction': pred_label,
                            'model': llm_model,
                        }
                    )

    metrics = {
        'accuracy': accuracy_score(gt_arr, pred_arr),
        'precision': precision_score(gt_arr, pred_arr, zero_division=0),
        'recall': recall_score(gt_arr, pred_arr, zero_division=0),
        'f1': f1_score(gt_arr, pred_arr, zero_division=0),
    }
    return metrics, errors


PRICES = {
    'gpt-4.1-nano': {'prompt': 0.17 / 1000000, 'completion': 0.17 / 1000000},
    'gpt-4.1-mini': {'prompt': 0.7 / 1000000, 'completion': 0.7 / 1000000},
    'gpt-5-nano': {'prompt': 0.14 / 1000000, 'completion': 0.14 / 1000000},
}


def compute_cost_for_model(model: str):
    PRED_FOLDER = f'research/results/test_results/{model}'
    files = os.listdir(PRED_FOLDER)

    total_prompt = 0
    total_completion = 0

    for file in files:
        data = load_json(f'{PRED_FOLDER}/{file}')
        for item in data:
            total_prompt += item.get('prompt_tokens', 0)
            total_completion += item.get('completion_tokens', 0)

    cost = total_prompt * PRICES[model]['prompt'] + total_completion * PRICES[model]['completion']
    return {
        'model': model,
        'total_prompt_tokens': total_prompt,
        'total_completion_tokens': total_completion,
        'total_cost_usd': round(cost, 6),
    }


if __name__ == '__main__':
    print('================================================')
    print('Evaluating sheet level sensitive data')
    print('================================================')

    LLM_MODEL = 'gpt-4.1-nano'
    print(f'Evaluating sheet level non-personal sensitive data for LLM: {LLM_MODEL}')
    eval_sheet_level_sensitive_data(LLM_MODEL, 'non_personal_data_sensitive')
    print('--------------------------------')
    print(f'Evaluating sheet level personal sensitive data for LLM: {LLM_MODEL}')
    eval_sheet_level_sensitive_data(LLM_MODEL, 'personal_data_sensitive')
    print('--------------------------------')
    print('Evaluating column level personal sensitive data')
    print('--------------------------------')
    print(eval_personal_sensitive_data_column_level(LLM_MODEL))
