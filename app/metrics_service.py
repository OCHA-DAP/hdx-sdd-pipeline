"""Pure metrics calculation — no FastAPI, no file I/O."""

from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path

from .schemas import SDDReport
from .sdd_io import load_sdd, is_sensitive_pd, is_sensitive_npd


@dataclass
class ConfusionMatrix:
    tp: int = 0
    fp: int = 0
    fn: int = 0
    tn: int = 0
    count: int = 0  # files or sheets tested

    def update(self, predicted: bool, actual: bool) -> None:
        self.count += 1
        if predicted and actual:
            self.tp += 1
        elif predicted and not actual:
            self.fp += 1
        elif not predicted and actual:
            self.fn += 1
        else:
            self.tn += 1

    def to_dict(self, model: str, count_label: str = 'files_tested') -> dict:
        tp, fp, fn, tn = self.tp, self.fp, self.fn, self.tn
        total = tp + fp + fn + tn
        accuracy = (tp + tn) / total if total else 0
        precision = tp / (tp + fp) if (tp + fp) else 0
        recall = tp / (tp + fn) if (tp + fn) else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0
        return {
            'model': model,
            'accuracy': accuracy,
            'precision': precision,
            'recall': recall,
            'f1_score': f1,
            'true_positives': tp,
            'false_positives': fp,
            'true_negatives': tn,
            'false_negatives': fn,
            count_label: self.count,
        }


@dataclass
class ModelMetrics:
    model: str
    file_overall: ConfusionMatrix = field(default_factory=ConfusionMatrix)
    file_personal: ConfusionMatrix = field(default_factory=ConfusionMatrix)
    file_non_personal: ConfusionMatrix = field(default_factory=ConfusionMatrix)
    sheet_overall: ConfusionMatrix = field(default_factory=ConfusionMatrix)
    sheet_personal: ConfusionMatrix = field(default_factory=ConfusionMatrix)
    sheet_non_personal: ConfusionMatrix = field(default_factory=ConfusionMatrix)

    def add_file(self, pred: SDDReport, gt: SDDReport) -> None:
        pred_pd = is_sensitive_pd(pred)
        gt_pd = is_sensitive_pd(gt)
        pred_npd = is_sensitive_npd(pred)
        gt_npd = is_sensitive_npd(gt)

        self.file_personal.update(pred_pd, gt_pd)
        self.file_non_personal.update(pred_npd, gt_npd)
        self.file_overall.update(pred_pd or pred_npd, gt_pd or gt_npd)

        # Sheet-level: match by sheet_name
        gt_map = {s.sheet_name.strip().lower(): s for s in gt.sdd_report}
        pred_map = {s.sheet_name.strip().lower(): s for s in pred.sdd_report}

        for name in set(gt_map) | set(pred_map):
            p = pred_map.get(name)
            g = gt_map.get(name)
            p_pd = p.personal_data_sensitive if p else False
            p_npd = p.non_personal_data_sensitive if p else False
            g_pd = g.personal_data_sensitive if g else False
            g_npd = g.non_personal_data_sensitive if g else False

            self.sheet_personal.update(p_pd, g_pd)
            self.sheet_non_personal.update(p_npd, g_npd)
            self.sheet_overall.update(p_pd or p_npd, g_pd or g_npd)

    def to_response(self) -> dict:
        m = self.model
        return {
            'overall_performance': self.file_overall.to_dict(m),
            'personal_sensitive': self.file_personal.to_dict(m),
            'non_personal_sensitive': self.file_non_personal.to_dict(m),
            'sheet_personal_sensitive': self.sheet_personal.to_dict(m, 'sheets_tested'),
            'sheet_non_personal_sensitive': self.sheet_non_personal.to_dict(m, 'sheets_tested'),
            'sheet_overall_sensitive': self.sheet_overall.to_dict(m, 'sheets_tested'),
        }


def compute_performance(
    model: str,
    model_dir: Path,
    groundtruth_dir: Path,
) -> ModelMetrics | None:
    metrics = ModelMetrics(model=model)
    paired = 0

    for result_file in model_dir.glob('*.json'):
        gt_path = groundtruth_dir / result_file.name
        if not gt_path.exists():
            continue
        try:
            pred = load_sdd(result_file)
            gt = load_sdd(gt_path)
            metrics.add_file(pred, gt)
            paired += 1
        except Exception:
            continue

    return metrics if paired else None


def compute_cost(
    model: str,
    model_dir: Path,
    price_per_1m: float,
) -> dict:
    total_prompt = total_completion = reports = 0

    for result_file in model_dir.glob('*.json'):
        try:
            sdd = load_sdd(result_file)
            for sheet in sdd.sdd_report:
                total_prompt += sheet.prompt_tokens
                total_completion += sheet.completion_tokens
            reports += 1
        except Exception:
            continue

    total = total_prompt + total_completion
    cost = total / 1_000_000 * price_per_1m
    return {
        'model': model,
        'reports': reports,
        'prompt_tokens': total_prompt,
        'completion_tokens': total_completion,
        'total_tokens': total,
        'price_per_1m': price_per_1m,
        'total_cost': cost,
        'cost_per_report': cost / reports if reports else 0,
    }
