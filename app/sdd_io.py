"""Read / write SDD report JSON files with a single validated schema."""

from __future__ import annotations
import json
from pathlib import Path

from .schemas import SDDReport, SheetReport


def load_sdd(path: Path) -> SDDReport:
    """Load and validate an SDD report (model result OR groundtruth) from disk."""
    with open(path, 'r', encoding='utf-8') as f:
        raw = json.load(f)

    # Groundtruth files may be a bare list (legacy); normalise them.
    if isinstance(raw, list):
        raw = {
            'resource_id': path.stem,
            'sensitive': _derive_sensitivity(raw),
            'timestamp': '',
            'sdd_report': raw,
        }

    return SDDReport.model_validate(raw)


def save_sdd(report: SDDReport, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(report.model_dump(), f, indent=2, default=str)


# ---------------------------------------------------------------------------
# Sensitivity helpers
# ---------------------------------------------------------------------------


def _derive_sensitivity(sheets: list[dict]) -> str:
    has_pd = any(sheet.get('personal_data_sensitive', False) for sheet in sheets if isinstance(sheet, dict))
    has_npd = any(sheet.get('non_personal_data_sensitive', False) for sheet in sheets if isinstance(sheet, dict))
    if has_pd and has_npd:
        return 'sensitive-pd-and-non-pd'
    if has_pd:
        return 'sensitive-pd'
    if has_npd:
        return 'sensitive-non-pd'
    return 'not-sensitive'


def sensitivity_from_sheets(sheets: list[SheetReport]) -> str:
    has_pd = any(s.personal_data_sensitive for s in sheets)
    has_npd = any(s.non_personal_data_sensitive for s in sheets)
    if has_pd and has_npd:
        return 'sensitive-pd-and-non-pd'
    if has_pd:
        return 'sensitive-pd'
    if has_npd:
        return 'sensitive-non-pd'
    return 'not-sensitive'


def is_sensitive_pd(sdd: SDDReport) -> bool:
    return 'sensitive-pd' in sdd.sensitive


def is_sensitive_npd(sdd: SDDReport) -> bool:
    return 'non-pd' in sdd.sensitive
