# SDD Pipeline Functional Requirements (Implemented Baseline)

Last updated: 2026-03-31
Scope: Event-driven HDX Sensitive Data Detection pipeline (worker/runtime path).
Purpose: Capture implemented functional requirements so future changes extend behavior instead of rewriting it.

## Requirement lifecycle for future features

Any new feature request for this project must follow this order:

1. Add or update a requirement in this file first (new ID, acceptance criteria, and expected behavior).
2. Human reviewer validates and approves requirement text.
3. Implementation is performed (human or LLM) against the approved requirement.
4. Validation is run for touched behavior (tests and/or targeted smoke checks).

## Requirements status legend

- `[x]` implemented
- `[ ]` planned or not yet implemented

## Requirements

### Startup and runtime

- [x] FR-SDD-001: Logging configuration must be initialized at process startup before runtime logic begins.
  - Implemented behavior: Runtime entry imports logging configuration module at the top of startup path so log messages are captured from early initialization onward.

- [x] FR-SDD-002: Runtime must support worker-enabled and worker-disabled modes.
  - Implemented behavior: If worker mode is disabled, process stays alive without consuming events. If enabled, it listens to configured Redis stream/group/consumer.

- [x] FR-SDD-003: Event consumer must process only approved event types for SDD resource workflows.
  - Implemented behavior: Event bus listener is restricted to SDD resource creation/data-change event types.

### Configuration and dependencies

- [x] FR-SDD-010: Configuration must be centralized and environment-driven.
  - Implemented behavior: Redis, HDX, model, processing-step toggles, directory paths, Azure credentials, and Slack settings are read via a centralized config object.

- [x] FR-SDD-011: The pipeline must support separate model configuration for PII detection, PII reflection, non-PII classification, and README scan.
  - Implemented behavior: Distinct model settings exist and are passed via pipeline factory/use case wiring.

- [x] FR-SDD-012: Processing steps must be individually switchable through configuration flags.
  - Implemented behavior: Personal data detection, personal data reflection, non-personal data detection, README scan, and CKAN update flags are configurable.

- [x] FR-SDD-013: The pipeline must track its runtime version for downstream metadata and integrations.
  - Implemented behavior: Runtime version is resolved from package metadata and exposed through a shared version module with a fallback when metadata is unavailable.

- [x] FR-SDD-014: All outbound HTTP requests must include a User-Agent header configurable from environment.
  - Implemented behavior: `SDD_USER_AGENT` controls the header value and defaults to `HDXINTERNAL:SDDPipeline/{version}`. The header is applied to CKAN API calls and URL-based file downloads.

- [x] FR-SDD-015: CKAN API tokens must only be forwarded to trusted HDX domains during data download requests.
  - Implemented behavior: URL-based download requests strip `Authorization` headers unless the destination hostname matches the configured HDX hostname (including subdomains).

### Event processing behavior

- [x] FR-SDD-020: Every incoming event must include resource identity before processing.
  - Implemented behavior: Missing resource ID is treated as invalid event and processing exits with failure status.

- [x] FR-SDD-021: Source retrieval must support both CKAN and non-CKAN execution paths.
  - Implemented behavior: With CKAN enabled, resource metadata is resolved through CKAN API. With CKAN disabled, event payload must provide source URL/file context.

- [x] FR-SDD-022: Existing reports should not be recomputed when report presence indicates prior processing.
  - Implemented behavior: When CKAN is enabled and report already exists, event returns successful no-op outcome.

- [x] FR-SDD-023: ISP rules must be retrieved and applied during sensitivity analysis.
  - Implemented behavior: Event processor resolves ISP rules from package/resource context before executing classification pipeline.

### Classification and sensitivity

- [x] FR-SDD-030: The dataset pipeline must execute the implemented multi-stage classification flow.
  - Implemented behavior: Data loading and sheet creation, column-level personal-data entity detection, table-level personal-data sensitivity reflection, table-level non-personal sensitivity classification, and final sensitivity flags.

- [x] FR-SDD-031: README/metadata sheets must be identified and handled as non-standard data sheets.
  - Implemented behavior: README detection path exists and those sheets are represented distinctly in reports.

- [x] FR-SDD-032: Final resource sensitivity must aggregate sheet-level outcomes.
  - Implemented behavior: Resource result is categorized as one of: not-sensitive, sensitive-pd, sensitive-non-pd, sensitive-pd-and-non-pd.

### Persistence and outputs

- [x] FR-SDD-040: Results must be persisted either to CKAN or local output depending on runtime mode.
  - Implemented behavior: CKAN update path writes SDD report and sensitivity fields to resource metadata. Local mode writes JSON report files to configured/custom output.

- [x] FR-SDD-041: Local output mode must ensure output directories exist and write structured JSON report payloads.
  - Implemented behavior: Output directories are created on demand and report payload includes resource ID, sensitivity, timestamp, and per-sheet report list.

### Error handling and notifications

- [x] FR-SDD-050: Critical processing failures must generate Slack notifications.
  - Implemented behavior: Important event-processing exceptions are formatted with event context and posted through Slack wrapper.

- [x] FR-SDD-051: Slack delivery failures must never block or crash processing.
  - Implemented behavior: Slack API errors are logged and suppressed.

- [x] FR-SDD-052: Processing failures must be logged with diagnostic context and returned as failure status.
  - Implemented behavior: Exceptions are logged with stack details and caller receives structured failure result.

## Notes for implementers

- Do not change startup logging order without explicit requirement update.
- Do not remove Slack error reporting on important processing failures without approved requirement change.
- Prefer extending existing pipeline factory/use-case flow rather than creating a parallel processing path.
