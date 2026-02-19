import os
import json
import glob


def load_json(path):
    with open(path, 'r') as f:
        return json.load(f)


def get_files_in_dir(directory):
    return {
        os.path.basename(f)
        for f in glob.glob(os.path.join(directory, '*.json'))
        if not os.path.basename(f).endswith('metrics_summary.json') and not os.path.basename(f).endswith('scores.json')
    }


def calculate_metrics(predictions, ground_truth, label):
    tp = 0
    fp = 0
    fn = 0
    tn = 0

    # Check if predictions is a list (list of tables in file) or dict
    # Assuming standard structure where the json is a list of dicts (one per table, usually 1)
    if isinstance(predictions, list):
        pred_val = any(p.get(label, False) for p in predictions)
    else:
        pred_val = predictions.get(label, False)

    if isinstance(ground_truth, list):
        gt_val = any(p.get(label, False) for p in ground_truth)
    else:
        gt_val = ground_truth.get(label, False)

    if pred_val and gt_val:
        tp = 1
    elif pred_val and not gt_val:
        fp = 1
    elif not pred_val and gt_val:
        fn = 1
    else:
        tn = 1

    return tp, fp, fn, tn


def main():
    base_dir = '/Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/research/results'

    # Directories to compare
    dirs = {
        'new_gpt4.1': os.path.join(base_dir, 'test_results/gpt-4.1'),
        'new_gpt4.1-mini': os.path.join(base_dir, 'test_results/gpt-4.1-mini'),
        'new_gpt5-mini': os.path.join(base_dir, 'test_results/gpt-5-mini'),
        'old_gpt4.1': os.path.join(base_dir, 'test_results_old/gpt-4.1'),
        'old_gpt4.1-mini': os.path.join(base_dir, 'test_results_old/gpt-4.1-mini'),
        'old_gpt5-mini': os.path.join(base_dir, 'test_results_old/gpt-5-mini'),
        'groundtruth_new': os.path.join(base_dir, 'test_results/groundtruth2'),
        'groundtruth_old': os.path.join(base_dir, 'test_results_old/groundtruth'),
    }

    # Get file sets
    file_sets = {}
    for name, path in dirs.items():
        if os.path.exists(path):
            file_sets[name] = get_files_in_dir(path)
        else:
            print(f'Warning: Directory not found: {path}')
            file_sets[name] = set()

    # Calculate intersection
    common_files = set.intersection(*file_sets.values())
    print(f'Found {len(common_files)} common files across all directories.')
    print(f'Common Files: {sorted(list(common_files))}')

    if not common_files:
        print('No common files to analyze.')
        return

    # Metrics storage
    results = {
        name: {'tp_p': 0, 'fp_p': 0, 'fn_p': 0, 'tn_p': 0, 'tp_np': 0, 'fp_np': 0, 'fn_np': 0, 'tn_np': 0}
        for name in dirs
        if 'groundtruth' not in name
    }

    for filename in common_files:
        # Load ground truths
        gt_new = load_json(os.path.join(dirs['groundtruth_new'], filename))
        gt_old = load_json(os.path.join(dirs['groundtruth_old'], filename))

        # Process each model
        for name in results:
            prediction = load_json(os.path.join(dirs[name], filename))

            # Determine which ground truth to use
            if 'new' in name:
                gt = gt_new
            else:
                gt = gt_old

            # Personal Data Sensitive
            tp, fp, fn, tn = calculate_metrics(prediction, gt, 'personal_data_sensitive')
            results[name]['tp_p'] += tp
            results[name]['fp_p'] += fp
            results[name]['fn_p'] += fn
            results[name]['tn_p'] += tn

            # Non-Personal Data Sensitive
            tp, fp, fn, tn = calculate_metrics(prediction, gt, 'non_personal_data_sensitive')
            results[name]['tp_np'] += tp
            results[name]['fp_np'] += fp
            results[name]['fn_np'] += fn
            results[name]['tn_np'] += tn

    # Print Report
    print(
        f'{"Model":<20} | {"Type":<15} | {"Prec":<6} |'
        f' {"Recall":<6} | {"F1":<6} | {"Acc":<6} |'
        f' {"TP":<3} | {"FP":<3} | {"FN":<3} | {"TN":<3}'
    )
    print('-' * 100)

    for name, metrics in results.items():
        for label_type in ['p', 'np']:
            tp = metrics[f'tp_{label_type}']
            fp = metrics[f'fp_{label_type}']
            fn = metrics[f'fn_{label_type}']
            tn = metrics[f'tn_{label_type}']

            if (tp + fp) > 0:
                precision = tp / (tp + fp)
            else:
                precision = 0.0

            if (tp + fn) > 0:
                recall = tp / (tp + fn)
            else:
                recall = 0.0

            if (precision + recall) > 0:
                f1 = 2 * (precision * recall) / (precision + recall)
            else:
                f1 = 0.0

            if (tp + tn + fp + fn) > 0:
                accuracy = (tp + tn) / (tp + tn + fp + fn)
            else:
                accuracy = 0.0

            type_str = 'Personal' if label_type == 'p' else 'Non-Personal'
            print(
                f'{name:<20} | {type_str:<15} |'
                f' {precision:<6.2f} | {recall:<6.2f} |'
                f' {f1:<6.2f} | {accuracy:<6.2f} | {tp:<3} | {fp:<3} | {fn:<3} | {tn:<3}'
            )


if __name__ == '__main__':
    main()
