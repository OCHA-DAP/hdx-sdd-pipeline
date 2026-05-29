# LLM Evaluation Functional Requirements (Implemented Baseline)

Last updated: 2026-03-18
Scope: Evaluation workflows (batch processing + FastAPI/Next.js dashboard integration).
Purpose: Document currently implemented requirements so future changes preserve existing evaluation behavior.

## Requirement lifecycle for future features

Any new feature request for this project must follow this order:

1. Add or update a requirement in this file first (new ID, acceptance criteria, expected API/UX behavior).
2. Human reviewer validates and approves requirement text.
3. Implementation is performed (human or LLM) against approved requirements.
4. Validation is run (targeted API tests, integration checks, and/or UI checks).

## Requirements status legend

- `[x]` implemented
- `[ ]` planned or not yet implemented

## Requirements

### Environment and startup

- [x] FR-EVAL-001: Evaluation backend must initialize application settings and ensure required directories are present on startup.
  - Implemented behavior: FastAPI app factory loads settings and creates configured dataset/report directories before serving requests.

- [x] FR-EVAL-002: Evaluation storage paths must be configurable by environment variables with repository-relative defaults.
  - Implemented behavior: Dataset, report, and ground truth directories resolve from env vars with deterministic fallback paths.

### Dataset and ground truth management

- [x] FR-EVAL-010: Dataset upload must validate file type and reject unsafe filenames.
  - Implemented behavior: Upload endpoint only allows CSV/XLSX extensions and blocks path traversal/path separator filenames.

- [x] FR-EVAL-011: Upload flow must avoid overwriting existing files.
  - Implemented behavior: Colliding filenames are rewritten with timestamp-based uniqueness.

- [x] FR-EVAL-012: Ground-truth template creation must be automatic after successful upload.
  - Implemented behavior: Upload endpoint immediately generates and saves template JSON in groundtruth directory.

- [x] FR-EVAL-013: Ground-truth templates must include TODO placeholders for manual annotation.
  - Implemented behavior: Template generation sets personal/non-personal sensitivity and column classification fields to TODO placeholders.

- [x] FR-EVAL-014: System must support template generation from existing dataset path without running LLM classification.
  - Implemented behavior: Template-report endpoint loads/samples sheet data and outputs a structured report scaffold with unfilled classification fields.

### Model execution and batch processing

- [x] FR-EVAL-020: Evaluation must support an explicit list of selectable model names.
  - Implemented behavior: Backend publishes available model list and uses it for model-specific reporting/batch operations.

- [x] FR-EVAL-021: Batch processing must support per-model execution across all available datasets.
  - Implemented behavior: Background batch process iterates through datasets and configured model set, saving per-model outputs.

- [x] FR-EVAL-022: Batch process must support skip-existing behavior to avoid recomputation.
  - Implemented behavior: Existing model/dataset output files are detected and skipped when skip-existing is enabled.

- [x] FR-EVAL-023: Batch status must be observable via API.
  - Implemented behavior: Backend tracks running state, current model, completed/failed model lists, start time, and progress percentage.

- [x] FR-EVAL-024: Scripted batch execution must support single-model override for all pipeline stages.
  - Implemented behavior: CLI batch script sets one model for PII detection, PII reflection, and non-PII detection while disabling CKAN update.

- [x] FR-EVAL-025: Batch processing must support processing by a list of HDX resource IDs.
  - Implemented behavior: Scripted batch execution can accept a list of resource IDs, fetch their metadata from CKAN, and save reports to a specific research directory (e.g., research/results/false_positives_prod).

### Results and analytics

- [x] FR-EVAL-030: Results must be persisted in model-scoped directories using JSON per dataset.
  - Implemented behavior: Output convention uses research/results/test_results/{model}/{dataset}.json.

- [x] FR-EVAL-031: API must expose dataset list, model list, model results summary, and detailed per-dataset report retrieval.
  - Implemented behavior: Dedicated endpoints return these views with fallback/error-safe handling.

- [x] FR-EVAL-032: Performance analytics must be computed against available ground truth at both file and sheet levels.
  - Implemented behavior: Confusion-matrix metrics include accuracy, precision, recall, F1, and tested counts for overall, personal, and non-personal categories.

- [x] FR-EVAL-033: Ground-truth placeholder values must be normalized safely during analytics.
  - Implemented behavior: Ground-truth normalization converts recognized boolean-like values and treats unknown placeholders (for example TODO) as false.

- [x] FR-EVAL-034: Cost analytics must be derived from stored token usage and model pricing map.
  - Implemented behavior: Prompt/completion tokens are aggregated per model and converted to total and per-report cost estimates using separate input (prompt) and output (completion) prices and appropriate currencies (e.g. USD, EUR).

- [x] FR-EVAL-035: Performance analytics and report views must handle the new dict-based report format.
  - Implemented behavior: Data extraction logic for analytics and report details handles both legacy list-based and new dict-based (sdd_report) formats, correctly identifying overall and sheet-level sensitivity flags.

- [x] FR-EVAL-036: Batch processing in the router must save results in the standard dict-based format.
  - Implemented behavior: The batch-process endpoint saves model results as a dictionary containing resource_id, overall sensitivity, timestamp, and the sdd_report list, matching the EventProcessor output format.

### Operational safeguards

- [x] FR-EVAL-040: Background batch processing must fail gracefully per dataset/model without aborting whole run.
  - Implemented behavior: Per-item failures are logged and iteration continues.

- [x] FR-EVAL-041: APIs must return meaningful HTTP errors for invalid model names, missing files, and invalid upload inputs.
  - Implemented behavior: Endpoints raise explicit HTTPException codes/messages for these conditions.

## Notes for implementers

- Do not remove automatic template generation from upload flow without requirement update and review.
- Do not change result path conventions without updating all API consumers and approved requirements.
- Preserve backward-compatible handling for current ground truth JSON shapes unless explicitly changed by requirement.
