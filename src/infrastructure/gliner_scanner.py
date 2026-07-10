"""GLiNER-based fast full-table PII pre-scan (FR-SDD-057).

Loads the model once at construction time and reuses it across all scans.
Scans column-by-column: unique values per column are concatenated into
text chunks of at most _MAX_CHUNK_CHARS characters and fed to GLiNER one
chunk at a time. As soon as a hit is detected in a column the column is
marked and scanning moves on to the next one (early-exit).

For very wide tables, at most `batch_size` unique values per column are
sampled (deterministic seed) to bound total inference time.

A regex fast-path handles obvious email addresses without any model call.
GLiNER covers personal names and exact street addresses, including
non-Western scripts (Arabic, Chinese, Cyrillic, etc.) via the multilingual
mGLiNER architecture of the bundled small model.
"""

from __future__ import annotations

import logging
import random
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
# The small model handles ~512 tokens; 2 000 characters is a safe proxy that
# keeps context rich while staying well within the model limit.
_MAX_CHUNK_CHARS = 2000

# Deterministic seed for reproducible value sampling.
_SAMPLE_SEED = 42


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

    Scanning strategy (column-level):
      For each column the unique non-empty values are collected, optionally
      capped at *batch_size* (random sample with fixed seed), then fed to the
      model in text chunks of at most *_MAX_CHUNK_CHARS* characters. The column
      scan exits as soon as the first PII entity is detected, so clean columns
      are cheap and only suspicious columns spend extra inference time.

    Args:
        model_name: HuggingFace model ID to load.
        threshold: Minimum confidence score to flag an entity (0–1).
        batch_size: Max unique values sampled per column before chunking.
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
        self._rng = random.Random(_SAMPLE_SEED)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scan_dataframe(self, df: 'pd.DataFrame') -> GliNERScanResult:
        """Scan every column in *df* for personal names, emails, and addresses.

        Uses a column-level strategy: unique values per column are joined into
        text chunks and passed to GLiNER, rather than one call per row.

        Args:
            df: The full dataframe to inspect (all rows, all columns).

        Returns:
            GliNERScanResult with ``flagged=True`` if any PII is detected.
        """
        result = GliNERScanResult()

        if df.empty:
            return result

        # Phase 1 — vectorized regex fast-path for emails.
        self._scan_emails_regex(df, result)

        # Phase 2 — GLiNER column-level scan for names and addresses.
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
            raise ImportError('GLiNER is not installed. Run: pip install gliner') from exc

        logger.info('Loading GLiNER model: %s', self.model_name)
        self._model = GLiNER.from_pretrained(self.model_name)
        logger.info('GLiNER model loaded successfully')
        return self._model

    def _scan_emails_regex(self, df: 'pd.DataFrame', result: GliNERScanResult) -> None:
        """Vectorized regex fast-path: detect email addresses per column."""
        for col in df.columns:
            col_str = str(col)
            # fillna('') ensures str.findall never receives NaN and never returns float NaN.
            series = df[col].fillna('').astype(str)

            # Quick column-level skip: no '@' means no emails.
            if not series.str.contains('@', na=False).any():
                continue

            # findall returns a list of matches per cell (empty list when none found).
            matches_per_cell = series.str.findall(_EMAIL_RE.pattern)
            for row_idx, matches in enumerate(matches_per_cell):
                if not isinstance(matches, list):
                    # Guard: findall may still return NaN on certain mixed-type columns.
                    continue
                for email in matches:
                    result.add_hit(
                        column=col_str,
                        row_idx=row_idx,
                        text=email,
                        label='email address',
                        score=1.0,
                        method='regex',
                    )

    def _scan_with_gliner(self, df: 'pd.DataFrame', result: GliNERScanResult) -> None:
        """Column-level GLiNER scan for person names and street addresses.

        For each column:
          1. Collect unique, non-empty string values.
          2. Sample at most *batch_size* values (reproducible seed).
          3. Pack values into text chunks ≤ *_MAX_CHUNK_CHARS* characters.
          4. Call predict_entities on each chunk; stop at the first hit.
        """
        model = self._load_model()

        for col in df.columns:
            col_str = str(col)

            # Unique non-empty string values (fillna to avoid 'nan' strings from float NaN).
            series = df[col].fillna('').astype(str)
            values: list[str] = [
                v.strip() for v in series.unique() if v.strip().lower() not in ('nan', 'none', 'null', '')
            ]

            if not values:
                continue

            # Cap to batch_size to bound inference time on high-cardinality columns.
            if len(values) > self.batch_size:
                values = self._rng.sample(values, self.batch_size)

            # Pack values into text chunks and scan each chunk.
            hit_found = False
            chunk_parts: list[str] = []
            chunk_len = 0

            for val in values:
                entry = val + '\n'
                if chunk_len + len(entry) > _MAX_CHUNK_CHARS:
                    if chunk_parts:
                        hit_found = self._predict_column_chunk(model, col_str, ''.join(chunk_parts), result)
                        if hit_found:
                            break
                    # Start a new chunk with the current value (truncated if huge).
                    chunk_parts = [entry[:_MAX_CHUNK_CHARS]]
                    chunk_len = len(chunk_parts[0])
                else:
                    chunk_parts.append(entry)
                    chunk_len += len(entry)

            # Flush the last chunk unless a hit was already found.
            if not hit_found and chunk_parts:
                self._predict_column_chunk(model, col_str, ''.join(chunk_parts), result)

    def _predict_column_chunk(
        self,
        model: Any,
        col_str: str,
        text: str,
        result: GliNERScanResult,
    ) -> bool:
        """Run predict_entities on one text chunk.

        Args:
            model: Loaded GLiNER model instance.
            col_str: Column name (for evidence recording).
            text: Concatenated unique cell values to scan.
            result: Mutable result object to append hits to.

        Returns:
            True if at least one entity was detected, False otherwise.
        """
        try:
            entities = model.predict_entities(
                text.strip(),
                _GLINER_LABELS,
                threshold=self.threshold,
            )
        except Exception:
            logger.exception('GLiNER prediction failed for column %s', col_str)
            return False

        for entity in entities:
            result.add_hit(
                column=col_str,
                row_idx=-1,  # row position not tracked in column-level scan
                text=entity['text'],
                label=entity['label'],
                score=entity['score'],
                method='gliner',
            )

        return bool(entities)


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
