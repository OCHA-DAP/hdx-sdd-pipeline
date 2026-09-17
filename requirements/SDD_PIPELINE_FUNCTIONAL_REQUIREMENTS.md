# SDD Pipeline Functional Requirements (Implemented Baseline)

Last updated: 2026-09-17
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
  - Implemented behavior: Redis, HDX, model, processing-step toggles, directory paths, Azure/OpenAI credentials, Google Sheets settings, and Slack settings are read via a centralized config object.

- [x] FR-SDD-011: The pipeline must support separate model configuration for PII detection, PII reflection, non-PII classification, and README scan.
  - Implemented behavior: Distinct model settings exist (`PII_DETECT_MODEL`, `PII_REFLECT_MODEL`, `NON_PII_DETECT_MODEL`, `README_SCAN_MODEL`) and are passed via pipeline factory/use case wiring.

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

- [x] FR-SDD-026: Disable resource name matching fallback when package metadata is available.
  - Implemented behavior: When package_id is provided (not None/empty) and CKAN metadata is fetchable (ckan_client is available), the pipeline must NOT fall back to matching the country from the resource name if package groups matching fails. It must fall back directly to the default ISP rules. If CKAN is disabled/unavailable, resource name matching fallback is still used even if package_id is provided.

- [x] FR-SDD-027: Default Non-PII ISP classification for datasets/resources with multiple locations.
  - Implemented behavior: When resolving ISP rules for a dataset/resource, if the dataset is associated with multiple locations (e.g., more than one valid location group in CKAN package `groups`), the retriever must not arbitrarily select one specific country's ISP rules. Instead, it must resolve to the default ISP rules (`isps['default']`), which routes non-PII classification through the default non-PII classification prompt.

### Classification and sensitivity

- [x] FR-SDD-030: The dataset pipeline must execute the implemented multi-stage classification flow.
  - Implemented behavior: Data loading and sheet creation, README scan, column-level personal-data entity detection, table-level personal-data sensitivity reflection, table-level non-personal sensitivity classification, and final sensitivity flags.

- [x] FR-SDD-031: README/metadata sheets must be identified and handled as non-standard data sheets.
  - Implemented behavior: README detection path exists and those sheets are represented distinctly in reports.

- [x] FR-SDD-032: Final resource sensitivity must aggregate sheet-level outcomes.
  - Implemented behavior: Resource result is categorized as one of: not-sensitive, sensitive-pd, sensitive-non-pd, sensitive-pd-and-non-pd.

- [x] FR-SDD-033: The non-PII classification prompt must be specialized when using the default ISP rules.
  - Implemented behavior: When the ISP country is 'default', use a dedicated classification prompt in `non_pii_classification/default/` with hardcoded humanitarian data sensitivity rules instead of the standard country-specific ISP-based prompt.

- [x] FR-SDD-034: The pipeline must support detection of Geo Coordinates as a PII entity.
  - Implemented behavior: 'GEO_COORDINATES' is included in the PII entity type enumeration and the PII detection prompt. Additionally, columns named 'latitude' or 'longitude' (case-insensitive) are automatically classified as 'GEO_COORDINATES'.

- [x] FR-SDD-035: If non-personal data sensitivity classification fails or returns UNDETERMINED, the pipeline must promote the sensitivity to SEVERE_SENSITIVE as a safe default, and record the error details in the explanation field.
  - Implemented behavior: Any decoding, connectivity, safety filter, or parse errors in non-personal classification, or an explicitly returned UNDETERMINED sensitivity, triggers a fallback to SEVERE_SENSITIVE with diagnostic details.

- [x] FR-SDD-036: Unified OpenAI SDK usage.
  - Implemented behavior: All LLM models (Azure OpenAI, standard OpenAI, DeepSeek, etc.) are queried using the standard openai SDK through a unified OpenAIProvider class, throwing LLMProviderError on failure rather than returning placeholder values like 'UNDETERMINED'.

- [x] FR-SDD-037: PII entity prediction failure fallback.
  - Implemented behavior: If PII entity classification fails (raises an exception or returns UNDETERMINED/UNKNOWN), the column's entity type is set to UNKNOWN and classified as sensitive.

- [x] FR-SDD-038: PII reflection failure fallback.
  - Implemented behavior: If PII reflection classification fails (raises an exception), the sheet-level personal data classification is set to sensitive (personal_data_sensitive = True), and the reason/exception details are recorded in the explanation field.

- [x] FR-SDD-039: Non-PII classification failure fallback.
  - Implemented behavior: If non-PII classification fails (raises an exception), the sheet-level non-personal data classification is set to sensitive (SensitivityLevel.SEVERE_SENSITIVE), and the reason/exception details are recorded in the explanation field.

- [x] FR-SDD-040: Results must be persisted either to CKAN or local output depending on runtime mode.
  - Implemented behavior: CKAN update path writes SDD report and sensitivity fields to resource metadata. Local mode writes JSON report files to configured/custom output.

- [x] FR-SDD-041: Local output mode must ensure output directories exist and write structured JSON report payloads.
  - Implemented behavior: Output directories are created on demand and report payload includes resource ID, sensitivity, timestamp, and per-sheet report list.

- [x] FR-SDD-042: Logging raw response for UNDETERMINED generation outcomes.
  - Implemented behavior: Whenever PII entity detection, PII reflection, or non-PII classification yields an UNDETERMINED result, the system logs the issue in generation including the raw response string.

- [x] FR-SDD-043: Incremental chunked loading and random sampling with fixed seed for sample values.
  - Implemented behavior: SmartDataLoader loads datasets in chunks (100, 1000, 10000, 25000, 50000, and 100000 rows) using pandas. If all columns have at least 5 unique non-empty/non-null values, it stops loading. If not, it tries the next chunk size until the end of the file or max chunk size is reached. Unique values for each column are randomly sampled (5 values) with a random seed of 42.

- [x] FR-SDD-044: Risk level scoring, hierarchical maximum risk propagation, and pii_reflection prompt alignment.
  - Implemented behavior: Every sheet report receives `personal_data_risk_level` (0-3) and `non_personal_data_risk_level` (0-3) keys serialized right after `non_personal_data_sensitive`. Every resource (file) report receives a `sensitivity_level` (0-3) key serialized right after `sensitive`. Risk scoring follows a hierarchical "maximum risk propagation" model where the worst-case sensitivity propagates upward:
    - PD Score: NON_SENSITIVE/UNDETERMINED -> 0; HIGH_SENSITIVE -> 2; SEVERE_SENSITIVE -> 3.
    - NPD Score: NONE/LOW/UNDETERMINED -> 0; MEDIUM -> 1; HIGH -> 2; SEVERE -> 3.
    - Sheet Risk: max(PD Score, NPD Score).
    - Resource/File Risk: max(all Sheet Risks).

- [x] FR-SDD-045: Metadata-aware prompts for PII reflection and non-PII classification.
  - Implemented behavior: Jinja prompt templates `pii_reflection`, `non_pii_classification`, and `non_pii_classification/default` include dataset metadata (`dataset_title`, `dataset_description`, `dataset_source`, `dataset_location`, `organization_title`) and resource metadata (`resource_name`, `resource_description`), handling missing/null metadata fields gracefully without rendering empty entries.

- [x] FR-SDD-046: Dataset and resource metadata extraction and propagation.
  - Implemented behavior: The event processor extracts metadata fields from events and/or CKAN (via package_show with nested resource fallback), maps them to a context payload, and passes them to the processing pipeline in a backward-compatible manner.

- [x] FR-SDD-047: Separate folder for default non-PII classification prompts.
  - Implemented behavior: Default non-PII classification prompts are stored in a dedicated `src/prompts/non_pii_classification/default/` folder. When default ISP country rules are applied, the pipeline resolves these templates from `non_pii_classification/default` using auto-detection for the latest version.

- [x] FR-SDD-048: Truncation of dataset and resource descriptions.
  - Implemented behavior: When extracting or passing dataset description (`dataset_description`) or resource description (`resource_description`) in the metadata payload, they are truncated at 1000 characters if their length exceeds 1000 characters.

- [x] FR-SDD-049: Omission of dataset location when containing more than 5 locations.
  - Implemented behavior: If dataset location (`dataset_location`) in the metadata payload contains more than 5 comma-separated locations, it is omitted (set to `None`/null) from the metadata passed to LLM prompts.

- [x] FR-SDD-050: Critical processing failures must generate Slack notifications.
  - Implemented behavior: Important event-processing exceptions are formatted with event context and posted through Slack wrapper.

- [x] FR-SDD-051: Slack delivery failures must never block or crash processing.
  - Implemented behavior: Slack API errors are logged and suppressed.

- [x] FR-SDD-052: Processing failures must be logged with diagnostic context and returned as failure status.
  - Implemented behavior: Exceptions are logged with stack details and caller receives structured failure result.

- [x] FR-SDD-053: Optimize CKAN metadata retrieval to minimize API calls.
  - Implemented behavior: When processing events, the pipeline fetches package metadata via `package_show` first and extracts resource-level details from the nested `resources` array, avoiding a separate `resource_show` API call unless the resource is missing from the package.

- [x] FR-SDD-054: General guidelines for Non-PII classification prompts.
  - Implemented behavior: Prompts explicitly instruct the model that:
    1. Geographic administrative levels and column names can be misleading. Terms like "Locality" (e.g., in Sudan) represent Admin Level 2 (ADM2). The model leverages its world knowledge about geographic structures rather than assuming default terms imply sub-district levels.
    2. Operational presence datasets (3W/4W/5W data) showing which organizations work in which locations are NOT organization contact/mailing lists. Contact lists must contain personal contact details (names, emails, phone numbers).
    3. Aggregate population counts (such as numbers of displaced persons, IDPs, or beneficiaries) at Admin Level 2 or higher are general population/operational statistics and do not constitute a needs assessment.

- [x] FR-SDD-055: Output tokens for non-PII classification configuration.
  - Implemented behavior: The output tokens (`max_tokens`) used for non-PII classification is a minimum of 2000 output tokens. If the number of columns in the resource (sheet report) multiplied by 5 is greater than 2000, that number (`n_columns * 5`) is used as `max_tokens`.

- [x] FR-SDD-056: Enhanced PII detection and phone number false positive mitigation.
  - Implemented behavior: The PII detection prompt prevents false positive classification of short/geographic area codes (e.g., FAOSTAT numeric area codes like 206, country codes, or other regional identifiers) as PHONE_NUMBER. Short numeric identifiers and area/region codes are not PHONE_NUMBER unless confirmed by sample values.

- [x] FR-SDD-057: Support for GPT-5 / GPT-5.4 reasoning effort and temperature handling.
  - Implemented behavior:
    1. In OpenAIProvider, if a model is identified as a reasoning model (any model name containing 'gpt-5'), reasoning_effort is configurable (e.g., 'low' for column-level PII detection and README scans; 'medium' for PII reflection and non-PII classification).
    2. When reasoning is active (reasoning_effort is not 'none'), temperature and top_p parameters are stripped from the API payload to prevent validation errors.
    3. Max completion tokens allocate a safety buffer (`max_tokens + 8192`) for reasoning models.

- [x] FR-SDD-058: Route all PII entity types through table-level reflection.
  - Implemented behavior: All recognized PII entity types (including names, phone numbers, and emails) are evaluated through table-level sensitivity reflection to assess re-identification risk in context, rather than immediately marking columns sensitive.

- [x] FR-SDD-059: Exclude organization email addresses from README scan PII detection.
  - Implemented behavior: The README scan prompt instructs the model to ignore organization-level/functional email addresses (such as contact/info/data mailboxes) and only flag personal/individual email addresses tied to an identifiable individual.

- [x] FR-SDD-060: Exclude organization-level email addresses from PII reflection sensitivity classification.
  - Implemented behavior: The PII reflection prompt instructs the model that organization-level/functional email addresses (e.g. generic info@, contact@, data@, support@, or shared team inboxes not tied to one named person) are not personal data and do not count toward re-identification risk at any sensitivity level.

- [x] FR-SDD-061: Decouple ISP retrieval via IISPStrategy protocol.
  - Implemented behavior: ISP retrieval is decoupled from a static file path by defining an `IISPStrategy` protocol, supporting both `LocalJSONISPStrategy` and `GoogleSheetsISPStrategy`.

- [x] FR-SDD-062: Cache loaded ISP rules in Redis store.
  - Implemented behavior: In worker mode, loaded ISP rules from the configured strategy are cached in Redis with key `isp_rules_cache` expiring after 12 hours.

- [x] FR-SDD-063: Sourced Google Sheets ISP rules structure.
  - Implemented behavior: `GoogleSheetsISPStrategy` connects to Google Sheets, retrieves rows from "Data & Information Types Dataset", maps them using `COUNTRY_MAPPING_ISO` and sensitivity scales, and structures rules to include both legacy text-blob keys and the modern `sensitivity_rules` dictionary structure.

- [x] FR-SDD-064: Require sample-value confirmation for PERSON_NAME, EMAIL_ADDRESS, and PHONE_NUMBER PII classification.
  - Implemented behavior: The PII detection prompt explicitly instructs the model that classification for PERSON_NAME, EMAIL_ADDRESS, and PHONE_NUMBER must not rely on the column name alone and must be confirmed by actual matching values in the samples.

- [x] FR-SDD-065: Personal data risk focus and step-by-step evaluation rules in PII reflection prompt.
  - Implemented behavior: The PII reflection prompt instructs the model to assess personal data risk only (identifying an individual human being) using a 3-step evaluation process: (1) determine unit of analysis, (2) identify individual-level columns, (3) assign sensitivity level based on those columns only.

- [x] FR-SDD-066: Exclusion of disabled or inactive rules in Google Sheets ISP strategy.
  - Implemented behavior: `GoogleSheetsISPStrategy` skips rows where 'Enabled' is 'no' and skips rows where 'ISP Status' is 'under development' or 'not used'.

- [x] FR-SDD-067: Deterministic LLM execution via fixed random seed 42.
  - Implemented behavior: `OpenAIProvider` passes `seed=42` in OpenAI API completion requests to ensure deterministic response generation across pipeline executions.

- [x] FR-SDD-068: Dynamic Google Sheets Prompt Strategy with local fallback.
  - Implemented behavior: The system supports fetching prompt instructions and rules for all categories (`personal_data_detection`, `personal_data_reflection`, `non_personal_data_classificatio`, `non_personal_data_default_class`, `readme`) from Google Sheets via `SpreadsheetPromptStrategy` with local Excel (`prompts_dev.xlsx`) and local Jinja template fallbacks.

- [x] FR-SDD-069: Unified prompt rule ordering by section_id.
  - Implemented behavior: Prompt strategies load rules dynamically from Google Sheets or Excel, filter out disabled rows, and strictly order enabled rows by `section_id` using numerical/float parsing.

- [x] FR-SDD-070: Cache loaded prompt templates and rules in Redis store.
  - Implemented behavior: When running with worker mode / Redis store enabled, loaded template strings and parsed prompt rules for all prompt categories (with key suffix `_rules`) are cached in Redis with a TTL of 12 hours (`expire_in_seconds=43200`).

- [x] FR-SDD-071: Unified environment-configurable Google Sheets URL.
  - Implemented behavior: The pipeline consolidates all Google Sheet URL configuration under a single environment variable `GOOGLE_SHEET_URL` (defaulting to the central spreadsheet URL), removing separate URL variables across ISP retrieval and all prompt strategies.

## Notes for implementers

- Do not change startup logging order without explicit requirement update.
- Do not remove Slack error reporting on important processing failures without approved requirement change.
- Prefer extending existing pipeline factory/use-case flow rather than creating a parallel processing path.

