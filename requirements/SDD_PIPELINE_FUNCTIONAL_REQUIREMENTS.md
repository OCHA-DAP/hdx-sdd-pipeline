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

- [x] FR-SDD-021: Source retrieval must support both CKAN and non-CKAN execution paths and correctly identify source types.
  - Implemented behavior: With CKAN enabled, resource metadata is resolved through CKAN API. With CKAN disabled, event payload must provide source URL/file context. The system correctly identifies whether a source is a URL or a local file based on the source string itself, independent of output configuration.

- [x] FR-SDD-022: Existing reports should not be recomputed when report presence indicates prior processing.
  - Implemented behavior: When CKAN is enabled and report already exists, event returns successful no-op outcome.

- [x] FR-SDD-023: ISP rules must be retrieved and applied during sensitivity analysis.
  - Implemented behavior: Event processor resolves ISP rules from package/resource context before executing classification pipeline.

- [x] FR-SDD-024: ISP matching from resource name must search the entire filename for a match.
  - Implemented behavior: The system iterates through all configured ISPs and checks if their country ISO3 code is present as a substring (case-insensitive) within the entire resource name string.

- [x] FR-SDD-025: Data loader numeric value normalisation.
  - Implemented behavior: When loading data from CSV or Excel, elements in the preprocessed DataFrame are mapped element-wise to parse string-formatted integers or floats (including cleaning of comma and space thousands separators, supporting multiple and Unicode spaces like NBSP and NNBSP) into real numeric objects, ensuring data consistency across formats.

- [x] FR-SDD-058: Disable resource name matching fallback when package metadata is available.
  - Implemented behavior: When package_id is provided (not None/empty) and CKAN metadata is fetchable (ckan_client is available), the pipeline must NOT fall back to matching the country from the resource name if package groups matching fails. It must fall back directly to the default ISP rules. If CKAN is disabled/unavailable, resource name matching fallback is still used even if package_id is provided.

### Classification and sensitivity

- [x] FR-SDD-030: The dataset pipeline must execute the implemented multi-stage classification flow.
  - Implemented behavior: Data loading and sheet creation, column-level personal-data entity detection, table-level personal-data sensitivity reflection, table-level non-personal sensitivity classification, and final sensitivity flags.

- [x] FR-SDD-031: README/metadata sheets must be identified and handled as non-standard data sheets.
  - Implemented behavior: README detection path exists and those sheets are represented distinctly in reports.

- [x] FR-SDD-032: Final resource sensitivity must aggregate sheet-level outcomes.
  - Implemented behavior: Resource result is categorized as one of: not-sensitive, sensitive-pd, sensitive-non-pd, sensitive-pd-and-non-pd.

- [x] FR-SDD-033: The non-PII classification prompt must be specialized when using the default ISP rules.
  - Expected behavior: When the ISP country is 'default', use a simplified 2-level classification prompt (NON_SENSITIVE/SEVERE_SENSITIVE) with hardcoded humanitarian data sensitivity rules instead of the standard multi-level ISP-based prompt.

- [x] FR-SDD-034: The pipeline must support detection of Geo Coordinates as a PII entity.
  - Expected behavior: 'GEO_COORDINATES' is included in the PII entity type enumeration and the PII detection prompt. Additionally, columns named 'latitude' or 'longitude' (case-insensitive) are automatically classified as 'GEO_COORDINATES'.

- [x] FR-SDD-035: If non-personal data sensitivity classification fails or returns UNDETERMINED, the pipeline must promote the sensitivity to SEVERE_SENSITIVE as a safe default, and record the error details in the explanation field.
  - Expected behavior: Any decoding, connectivity, safety filter, or parse errors in non-personal classification, or an explicitly returned UNDETERMINED sensitivity, must trigger a fallback to SEVERE_SENSITIVE with diagnostic details.

- [x] FR-SDD-036: Unified OpenAI SDK usage.
  - Expected behavior: All LLM models (Azure OpenAI, DeepSeek, etc.) must be queried using the standard openai SDK through a unified OpenAIProvider class, completely removing the abstract ILLMProvider interface and factories, throwing LLMProviderError on failure rather than returning placeholder values like 'UNDETERMINED'.

- [x] FR-SDD-037: PII entity prediction failure fallback.
  - Expected behavior: If PII entity classification fails (raises an exception or returns UNDETERMINED/UNKNOWN), the column's entity type must be set to UNKNOWN and classified as sensitive.

- [x] FR-SDD-038: PII reflection failure fallback.
  - Expected behavior: If PII reflection classification fails (raises an exception), the sheet-level personal data classification must be set to sensitive (personal_data_sensitive = True), and the reason/exception details must be recorded in the explanation field.

- [x] FR-SDD-039: Non-PII classification failure fallback.
  - Expected behavior: If non-PII classification fails (raises an exception), the sheet-level non-personal data classification must be set to sensitive (SensitivityLevel.SEVERE_SENSITIVE), and the reason/exception details must be recorded in the explanation field.

- [x] FR-SDD-042: Logging raw response for UNDETERMINED generation outcomes.
  - Expected behavior: Whenever PII entity detection, PII reflection, or non-PII classification yields an UNDETERMINED result, the system must clearly log the issue in generation, including the raw response back.
- [x] FR-SDD-043: Incremental chunked loading and random sampling with fixed seed for sample values.
  - Expected behavior: SmartDataLoader loads datasets in chunks (100, 1000, 10000, 25000, 50000, and 100000 rows) using pandas. If all columns have at least 5 unique non-empty/non-null values, it stops loading. If not, it tries the next chunk size until the end of the file or max chunk size is reached. Unique values for each column are randomly sampled (5 values) with a random seed of 42.


- [x] FR-SDD-044: Risk level scoring, hierarchical maximum risk propagation, and pii_reflection prompt alignment.
  - Expected behavior: Every sheet report receives `personal_data_risk_level` (0-3) and `non_personal_data_risk_level` (0-3) keys serialized right after `non_personal_data_sensitive`. Every resource (file) report receives a `sensitivity_level` (0-3) key serialized right after `sensitive`. Risk scoring follows a hierarchical "maximum risk propagation" model where the worst-case sensitivity propagates upward:
    - PD Score: NON_SENSITIVE/UNDETERMINED -> 0; HIGH_SENSITIVE -> 2; SEVERE_SENSITIVE -> 3.
    - NPD Score: NONE/LOW/UNDETERMINED -> 0; MEDIUM -> 1; HIGH -> 2; SEVERE -> 3.
    - Sheet Risk: max(PD Score, NPD Score).
    - Resource/File Risk: max(all Sheet Risks).
  - The `pii_reflection` prompt (specifically the latest version `v2.jinja`) must be updated to use the new PD classification scale (NON_SENSITIVE, HIGH_SENSITIVE, SEVERE_SENSITIVE) instead of the old MODERATE_SENSITIVE.

- [x] FR-SDD-045: Metadata-aware prompts for PII reflection and non-PII classification.
  - Expected behavior: New Jinja prompt templates `pii_reflection/v4.jinja` (reflection), `non_pii_classification/v3.jinja` (standard non-PII), and `non_pii_classification/default/v1.jinja` (default non-PII) include dataset metadata (`dataset_title`, `dataset_description`, `dataset_source`, `dataset_location`, `organization_title`) and resource metadata (`resource_name`, `resource_description`), handling missing/null metadata fields gracefully without rendering empty entries.

- [x] FR-SDD-046: Dataset and resource metadata extraction and propagation.
  - Expected behavior: The event processor extracts metadata fields from events and/or CKAN (via resource_show and package_show), maps them to a context payload, and passes them to the processing pipeline in a backward-compatible manner.

- [x] FR-SDD-047: Separate folder for default non-PII classification prompts.
  - Expected behavior: The default non-PII classification prompts are stored in a dedicated `src/prompts/non_pii_classification/default/` folder. The former non-PII default templates (`v2.jinja` without metadata and `v4.jinja` with metadata) are relocated to this directory and versioned as `v0.jinja` and `v1.jinja` respectively. When default ISP country rules are applied, the pipeline resolves these templates from `non_pii_classification/default` using auto-detection for the latest version.

- [x] FR-SDD-048: Truncation of dataset and resource descriptions.
  - Expected behavior: When extracting or passing dataset description (`dataset_description`) or resource description (`resource_description`) in the metadata payload, they must be truncated/cut off at 1000 characters if their length exceeds 1000 characters.

- [x] FR-SDD-049: Omission of dataset location when containing more than 5 locations.
  - Expected behavior: If dataset location (`dataset_location`) in the metadata payload contains more than 5 comma-separated locations, it must be omitted (set to `None`/null) from the metadata passed to LLM prompts, as many locations are not considered to add value.

- [x] FR-SDD-054: Optimize CKAN metadata retrieval to minimize API calls.
  - Expected behavior: When processing events, the pipeline should fetch package metadata via `package_show` first and extract resource-level details from the nested `resources` array, avoiding a separate `resource_show` API call unless the resource is missing from the package.

- [x] FR-SDD-055: General guidelines for Non-PII classification prompts to improve handling of administrative levels, organization operational lists, and population stats.
  - Expected behavior: Prompts must explicitly instruct the model that:
    1. Geographic administrative levels and column names can be misleading. For instance, terms like "Locality" (e.g., in Sudan) represent Admin Level 2 (ADM2), which is not below ADM2. The model should leverage its pre-trained world knowledge about geographic structures, administrative divisions, and spelling conventions of specific countries to identify the correct administrative level rather than assuming default terms (like assuming "Locality" implies ADM3).
    2. Operational presence datasets (3W/4W/5W data) showing which organizations work in which locations are NOT "organization contact lists" or "mailing lists". Contact/mailing lists must contain personal contact details (names, emails, phone numbers).
    3. Aggregate population counts (such as numbers of displaced persons, IDPs, or beneficiaries) at Admin Level 2 or higher are general population/operational statistics and do not constitute a needs assessment.

- [x] FR-SDD-059: Exclude organization email addresses from README scan PII detection.
  - Expected behavior: The README scan prompt instructs the model to ignore organization-level/functional email addresses (such as contact/info/data mailboxes of an organization) and only flag personal/individual email addresses tied to an identifiable individual.

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

- [x] FR-SDD-056: Output tokens for non-PII classification configuration.
  - Expected behavior: The output tokens (`max_tokens`) used for non-PII classification must be a minimum of 2000 output tokens. If the number of columns in the resource (sheet report) multiplied by 5 is greater than 2000, then use that number (`n_columns * 5`) as the output tokens (`max_tokens`).

- [x] FR-SDD-057: GLiNER fast PII pre-scan.
  - Expected behavior: Before the LLM-based PII classification step, the pipeline must run a fast, local GLiNER model scan over **all columns** of every data sheet to detect personal names, email addresses, and exact street addresses. If any are found, the sheet is immediately flagged as `personal_data_sensitive=True` with `personal_data_classification.sensitivity=SEVERE_SENSITIVE`, column-level sensitive flags are set, and the LLM PII classification and reflection steps are **skipped** (reusing the existing early-exit pattern). Non-PII classification continues normally. The scan must:
    - Load the GLiNER model (`gliner-community/gliner_small-v2.5` by default) once on first use and reuse it for all subsequent scans.
    - Process columns by extracting unique non-empty values (optionally capped at `GLINER_BATCH_SIZE` if greater than 0, defaulting to 0 for unlimited to scan all unique values), concatenating them into text chunks of at most 2000 characters, and running GLiNER prediction on each chunk.
    - Stop scanning a column as soon as a PII entity is detected in that column (early-exit).
    - Map the dominant detected entity label in a column to a `PIIEntityType` and assign it to the column's `pii_classification.entity_type` field.
    - Apply an email regex fast-path to detect email addresses without invoking the GLiNER model.
    - Support non-Western (Arabic, Chinese, Cyrillic, etc.) names via the multilingual mGLiNER architecture.
    - Be individually switchable via a `GLINER_SCAN` configuration flag (default `false`).
    - Expose `GLINER_THRESHOLD` (default `0.7`), `GLINER_MODEL`, and `GLINER_BATCH_SIZE` (default `0` for unlimited) as environment-driven configuration settings.
    - Record GLiNER scan evidence (column, row index, matched text, label, score) in the sheet report for auditability.
    - Provide a complete, non-truncated explanation detailing the hits grouped by column (e.g. `'col': label ×count`).

- [x] FR-SDD-058: Enhanced PII detection and phone number false positive mitigation.
  - Expected behavior: The PII detection prompt and/or pipeline must prevent false positive classification of short/geographic area codes (e.g., FAOSTAT numeric area codes like 206, country codes, or other regional identifiers) as PHONE_NUMBER. Specifically:
    1. A PHONE_NUMBER must represent actual telephone numbers, which are typically longer and formatted with country codes or local prefixes. Short numeric identifiers, region codes, and FAOSTAT area codes (e.g., Sudan former 206) are not PHONE_NUMBER.
    2. Prompt instructions must clarify that column names like "Area Code", "Country Code", or "Region Code" combined with short numeric codes should not be flagged as PHONE_NUMBER.

- [x] FR-SDD-060: Route all PII entity types (including names, phone numbers, and emails) through table-level reflection.
- [x] FR-SDD-061: Decouple ISP retrieval via IISPStrategy protocol.
  - Expected behavior: Decouple the ISP retrieval from a static file path by defining an `IISPStrategy` protocol. The pipeline will resolve ISP strategies dynamically, supporting both a local JSON strategy (`LocalJSONISPStrategy`) and a Google Sheets strategy (`GoogleSheetsISPStrategy`).
  
- [x] FR-SDD-062: Optionally cache loaded ISP rules in Redis store.
  - Expected behavior: When running in event-driven worker mode, the loaded ISP rules from the configured strategy should be cached in Redis with a key `isp_rules_cache` expiring after 12 hours, avoiding redundant calls to Google Sheets or disk.

- [x] FR-SDD-063: Sourced Google Sheets ISP rules must be parsed into forward-compatible structures.
  - Expected behavior: The `GoogleSheetsISPStrategy` will connect to Google Sheets, retrieve rows from the "Data & Information Types Dataset" worksheet, map them using `COUNTRY_MAPPING_ISO` and sensitivity scales, and structure each country's ISP rules to include both the legacy text-blob keys (`low_no_sensitivity`, `medium_sensitivity`, `high_sensitivity`, `severe_sensitivity`) and the modern `sensitivity_rules` dictionary structure required by current prompts.

## Notes for implementers

- Do not change startup logging order without explicit requirement update.
- Do not remove Slack error reporting on important processing failures without approved requirement change.
- Prefer extending existing pipeline factory/use-case flow rather than creating a parallel processing path.
