"""GLiNER-based fast full-table PII pre-scan (FR-SDD-057).

Loads the model once at construction time and reuses it across all scans.
Processes the entire dataframe — all rows, all columns — in configurable
row-batches so that very large datasets are handled with bounded memory.

A regex fast-path handles obvious email addresses without consuming GLiNER
inference time. GLiNER covers personal names and exact street addresses,
including non-Western scripts (Arabic, Chinese, Cyrillic, etc.) via the
multilingual mGLiNER architecture of the bundled small model.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)

# Entity labels used for GLiNER inference.
_GLINER_LABELS = ['person name', 'street address']

# Regex fast-path for email detection — avoids GLiNER overhead entirely.
_EMAIL_RE = re.compile(r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b')

# Maximum character length of a single text chunk sent to GLiNER.
# The small model handles ~512 tokens; 1 000 characters is a safe proxy.
_MAX_CHUNK_CHARS = 1000


@dataclass
class GliNERScanResult:
    """Result of a full-table GLiNER PII scan."""

    flagged: bool = False
    evidence: list[dict[str, Any]] = field(default_factory=list)

    def add_hit(
        self,
        *,
        column: str,
        row_idx: int,
        text: str,
        label: str,
        score: float,
        method: str = 'gliner',
    ) -> None:
        """Record a single PII detection hit."""
        self.flagged = True
        self.evidence.append(
            {
                'column': column,
                'row_idx': row_idx,
                'text': text,
                'label': label,
                'score': score,
                'method': method,
            }
        )


class GliNERScanner:
    """Singleton-style wrapper that loads GLiNER once and exposes a scan API.

    Args:
        model_name: HuggingFace model ID to load.
        threshold: Minimum confidence score to flag an entity (0–1).
        batch_size: Number of dataframe rows processed per GLiNER call.
    """

    def __init__(
        self,
        model_name: str = 'gliner-community/gliner_small-v2.5',
        threshold: float = 0.5,
        batch_size: int = 256,
    ) -> None:
        self.model_name = model_name
        self.threshold = threshold
        self.batch_size = batch_size
        self._model: Any = None  # lazy-loaded

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scan_dataframe(self, df: 'pd.DataFrame') -> GliNERScanResult:
        """Scan every cell in *df* for personal names, emails, and addresses.

        Args:
            df: The full dataframe to inspect (all rows, all columns).

        Returns:
            GliNERScanResult with ``flagged=True`` if any PII is detected.
        """
        result = GliNERScanResult()

        if df.empty:
            return result

        # Phase 1 — regex fast-path for emails (no model load needed).
        self._scan_emails_regex(df, result)

        # Phase 2 — GLiNER scan for names and addresses in batches.
        # We skip loading the model if already flagged? No — we still scan
        # because the caller needs full evidence for auditability.
        self._scan_with_gliner(df, result)

        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_model(self) -> Any:
        """Lazy-load the GLiNER model (called once on first use)."""
        if self._model is not None:
            return self._model

        try:
            from gliner import GLiNER  # type: ignore[import]
        except ImportError as exc:
            raise ImportError("GLiNER is not installed. Run: pip install 'gliner[tokenizers]'") from exc

        logger.info('Loading GLiNER model: %s', self.model_name)
        self._model = GLiNER.from_pretrained(self.model_name)
        logger.info('GLiNER model loaded successfully')
        return self._model

    def _scan_emails_regex(self, df: 'pd.DataFrame', result: GliNERScanResult) -> None:
        """Fast-path: detect email addresses via regex, no model needed."""
        for col in df.columns:
            col_str = str(col)
            for row_idx, cell in enumerate(df[col]):
                cell_str = _cell_to_str(cell)
                if not cell_str:
                    continue
                for match in _EMAIL_RE.finditer(cell_str):
                    result.add_hit(
                        column=col_str,
                        row_idx=row_idx,
                        text=match.group(),
                        label='email address',
                        score=1.0,
                        method='regex',
                    )

    def _scan_with_gliner(self, df: 'pd.DataFrame', result: GliNERScanResult) -> None:
        """GLiNER scan for names and addresses, processed in row-batches."""
        model = self._load_model()
        columns = list(df.columns)
        n_rows = len(df)

        for batch_start in range(0, n_rows, self.batch_size):
            batch_end = min(batch_start + self.batch_size, n_rows)
            batch_df = df.iloc[batch_start:batch_end]

            # Build one text chunk per row, joining non-empty string cells.
            texts: list[str] = []
            row_indices: list[int] = []
            col_maps: list[list[tuple[str, int, int]]] = []  # (col_name, start, end)

            for local_idx, (abs_idx, row) in enumerate(
                zip(range(batch_start, batch_end), batch_df.itertuples(index=False))
            ):
                parts: list[tuple[str, str]] = []  # (col_name, cell_text)
                for col in columns:
                    cell_str = _cell_to_str(getattr(row, _safe_attr(col), None))
                    if cell_str:
                        parts.append((str(col), cell_str))

                if not parts:
                    continue

                # Concatenate with separator, track char offsets per column.
                sep = ' | '
                combined = ''
                col_map: list[tuple[str, int, int]] = []
                for col_name, cell_text in parts:
                    start = len(combined)
                    end = start + len(cell_text)
                    # Truncate oversized rows to stay within model token limit.
                    if end > _MAX_CHUNK_CHARS:
                        cell_text = cell_text[: _MAX_CHUNK_CHARS - start]
                        end = _MAX_CHUNK_CHARS
                    col_map.append((col_name, start, end))
                    combined += cell_text
                    if end < _MAX_CHUNK_CHARS:
                        combined += sep
                    if len(combined) >= _MAX_CHUNK_CHARS:
                        break

                combined = combined.rstrip(' |').strip()
                if combined:
                    texts.append(combined)
                    row_indices.append(abs_idx)
                    col_maps.append(col_map)

            if not texts:
                continue

            for row_idx, text, col_map in zip(row_indices, texts, col_maps):
                try:
                    entities = model.predict_entities(
                        text,
                        _GLINER_LABELS,
                        threshold=self.threshold,
                    )
                except Exception:
                    # Graceful fallback: log and skip this single row.
                    logger.exception('GLiNER prediction failed for row %d', row_idx)
                    continue

                for entity in entities:
                    e_start: int = entity['start']
                    e_end: int = entity['end']
                    label: str = entity['label']
                    score: float = entity['score']
                    matched_text: str = entity['text']

                    # Map entity back to the originating column.
                    matched_col = _find_column(col_map, e_start, e_end)
                    result.add_hit(
                        column=matched_col,
                        row_idx=row_idx,
                        text=matched_text,
                        label=label,
                        score=score,
                        method='gliner',
                    )


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _cell_to_str(value: Any) -> str:
    """Convert a cell value to a clean string; return empty string for nulls."""
    if value is None:
        return ''
    s = str(value).strip()
    if s.lower() in ('nan', 'none', 'null', ''):
        return ''
    return s


def _safe_attr(col_name: Any) -> str:
    """Convert a column name to a safe pandas NamedTuple attribute name."""
    import re

    name = str(col_name)
    # pandas replaces non-alphanumeric chars with underscore in namedtuple attrs
    name = re.sub(r'[^A-Za-z0-9]', '_', name)
    if name and name[0].isdigit():
        name = '_' + name
    return name


def _find_column(col_map: list[tuple[str, int, int]], e_start: int, e_end: int) -> str:
    """Return the column name whose char range best overlaps the entity span."""
    best = ''
    best_overlap = 0
    for col_name, c_start, c_end in col_map:
        overlap = max(0, min(e_end, c_end) - max(e_start, c_start))
        if overlap > best_overlap:
            best_overlap = overlap
            best = col_name
    return best or (col_map[0][0] if col_map else 'unknown')
