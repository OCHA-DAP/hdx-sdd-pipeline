# HDX Sensitive Data Detection (SDD) Pipeline
## Internal Workings & Evaluation Briefing for Quality Assessors

---

## 1: Introduction & Purpose

### What is the SDD Pipeline?
An automated system designed to scan, analyze, and score humanitarian datasets on HDX to protect vulnerable populations and ensure compliance with Information Sensitivity Protocols (ISPs).

### Objectives for Quality Assessors (QAs)
- Understand **how** the pipeline evaluates datasets across column, table, and README levels.
- Learn **why** specific sensitivity ratings (0–3) are assigned.
- Differentiate between **Personal Data (PII)** sensitivity, **Non-Personal Data (NPD)** sensitivity, and **README sheet** scanning.
- Know how **Google Spreadsheets** are used to manage prompts & ISP protocols live.
- Recognize **fail-safe mechanisms** and system rules when auditing automated classification results.

---

## 2: Pipeline Architecture Overview

The pipeline executes a multi-stage deterministic workflow for every resource file and sheet:

```
                  ┌───────────────────────────────┐
                  │ 1. Data Loading & Sampling    │
                  └───────────────┬───────────────┘
                                  │
                  ┌───────────────┴───────────────┐
                  ▼                               ▼
       [Data / Matrix Sheets]            [README / Info Sheets]
                  │                               │
    ┌─────────────┴─────────────┐                 │
    │ 2. Column PII Detection   │                 │
    └─────────────┬─────────────┘                 │
                  │                               │
    ┌─────────────┴─────────────┐                 │
    │ 3. Table PII Reflection   │       ┌─────────┴─────────┐
    └─────────────┬─────────────┘       │  README PII Scan  │
                  │                     └─────────┬─────────┘
    ┌─────────────┴─────────────┐                 │
    │ 4. Non-PII Classification │                 │
    └─────────────┬─────────────┘                 │
                  │                               │
                  └───────────────┬───────────────┘
                                  ▼
                  ┌───────────────────────────────┐
                  │ 5. Risk Scoring & Aggregation │
                  └───────────────────────────────┘
```

> **Key Design Principle**: Decouples evaluation logic from prompt definitions by fetching prompts and country ISP rules live from **Google Spreadsheets**.

---

## 3: Stage 1 – Loading, Sampling & Normalization

### Incremental Chunked Loading
- Avoids memory limits on large datasets by loading progressively in chunk steps (`100` → `1,000` → `10,000` → `25,000` → `50,000` → `100,000` rows).
- Parsing stops as soon as every column has **5 unique non-empty values**.

### Deterministic Sampling
- Picks **5 representative sample values** per column using a fixed random seed (`seed=42`).
- Ensures **100% reproducible evaluations** across re-runs.

### Numeric Normalization
- Cleans string-formatted numbers by removing thousands separators (commas, spaces, `NBSP`, `NNBSP`).
- Standardizes data types into standard integers and floats prior to LLM evaluation.

---

## 4: Stage 2 – Personal Data (PII) Entity Detection

### Scope: Column-Level Evaluation
Analyzes column names alongside representative sample values to detect 12 entity types:

| Entity Type | Description / Examples |
| :--- | :--- |
| `NAME` | Full names, individual first/last names |
| `EMAIL` | Personal email addresses |
| `PHONE` | Mobile or landline numbers |
| `ADDRESS` | Street or residential physical addresses |
| `ID_NUMBER` | Passports, national IDs, beneficiary IDs |
| `DATE_OF_BIRTH` | Birth dates or exact ages |
| `GEO_COORDINATES` | Latitude, longitude, GPS waypoints (`FR-SDD-034`) |
| `FINANCIAL` / `HEALTH` / `BIOMETRIC` | Account details, diagnoses, biometric attributes |
| `NONE` / `UNDETERMINED` | General data or ambiguous entity type |

### Essential Rules for QAs to Know
1. **Sample-Value Confirmation (`FR-SDD-064`)**: Header names alone are insufficient; sample values must confirm actual PII presence.
2. **Phone Number False-Positive Guard (`FR-SDD-058`)**: Short geographic area codes, country codes, or FAOSTAT codes (e.g. `206`) are **not** flagged as `PHONE`.
3. **Geo Auto-Mapping**: Columns named `latitude` or `longitude` are automatically assigned `GEO_COORDINATES`.

---

## 5: Special Handling – README & Metadata Sheet Scanning

### Sheet Identification
Sheets containing keywords such as `readme`, `instructions`, `metadata`, or `info` are automatically routed to the specialized **README Scanning Pipeline**.

### Extraction & Scanning Process
1. **Text Extraction**: Combines all cell contents across the sheet into a unified text block.
2. **Dedicated Prompting (`readme_scan`)**: Analyzes text for embedded PII using `README_SCAN_MODEL` (e.g. `gpt-4.1-nano`) with `reasoning_effort='low'`.
3. **Targeted Detection**: Checks for personal contact information, author personal emails, phone numbers, or private address details mistakenly embedded in documentation.

### Impact on Risk Scoring
- If PII is detected in a README sheet, `personal_data_sensitive` is flagged as `True` and `personal_data_classification.sensitivity` is elevated to `HIGH_SENSITIVE`.
- Standard organizational contacts (`info@`, `contact@`, official agency support emails) are excluded.

---

## 6: Stage 3 – Personal Data Sensitivity Reflection

### Scope: Table-Level Human Re-Identification Risk
Assesses whether detected PII entities enable re-identification of individual human beings within the table context.

### Categorical Ratings

```
                    ┌─────────────────────────┐
                    │      NON_SENSITIVE      │  Cannot identify individuals
                    │        (Risk: 0)        │  (e.g., aggregate stats, broad age groups)
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │     HIGH_SENSITIVE      │  Quasi-identifiers / microdata
                    │        (Risk: 2)        │  (indirect re-identification risk)
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │    SEVERE_SENSITIVE     │  Direct identifiers present
                    │        (Risk: 3)        │  (Names, phone numbers, exact IDs)
                    └─────────────────────────┘
```

### Critical Rules for QAs
- **Organizational Email Exclusion (`FR-SDD-067`)**: Generic or functional mailboxes (`info@`, `contact@`, `data@`, shared team inboxes) are **not** personal data and do not trigger sensitivity.
- **Human-Only Focus**: Assesses human individual risk only. Geographical/facility precision is evaluated under Non-Personal Data rules.

---

## 7: Stage 4 – Non-Personal Data (NPD) Classification & Country ISPs

### Scope: Table-Level Information Sensitivity Protocol (ISP)
Evaluates table contents against country-specific ISP rules or global fallback defaults.

### Categorical Ratings & Risk Scale

| Categorical Level | Risk Score | Operational Meaning & Examples |
| :--- | :---: | :--- |
| **`NON_SENSITIVE`** | **0** | Public CODs, national 3W/4W, standard admin boundaries |
| **`MEDIUM_SENSITIVE`** | **1** | District-level operational assessments, aggregate access data |
| **`HIGH_SENSITIVE`** | **2** | Community/village survey microdata, aid worker lists, facility coords |
| **`SEVERE_SENSITIVE`** | **3** | Beneficiary lists, GBV/SEA cases, raw microdata, locations of vulnerable groups |

### Domain Context Guidelines
- **Geographic Precision**: Uses country-specific admin hierarchy (e.g., recognizes Sudan ADM2 vs ADM3+).
- **3W/4W Operational Data**: Standard operational presence datasets are **not** treated as contact lists unless they contain personal phone/name lists.
- **Dynamic Token Budget (`FR-SDD-056`)**: LLM token allocation scales dynamically (`max(2000, n_columns * 5)`) to ensure complete explanations without truncation.

---

## 8: Live Management via Google Spreadsheets

The pipeline integrates directly with **Google Spreadsheets** to fetch evaluation prompts and country ISP protocols dynamically. If no custom environment variable is set in `.env`, the pipeline automatically falls back to default Google Sheet URLs configured in [`config/config.py`](file:///Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/config/config.py).

### 1. PII Detection & Reflection Prompts Google Sheet
- **Worksheets**: `PII detection` and `PII reflection`
- **Fallback URL Config**: `DEFAULT_PII_DETECTION_GOOGLE_SHEET_URL` & `DEFAULT_PII_REFLECTION_GOOGLE_SHEET_URL`
- **Strategy**: `GoogleSheetsPIIPromptStrategy` & `GoogleSheetsPIIReflectionPromptStrategy`
- 🔗 **Spreadsheet Link**: [HDX PII Detection & Reflection Prompts](https://docs.google.com/spreadsheets/d/1vbn0d3tqZB0dGJTUdBPfn-oRU9m7xPeIwjXH4HW0eYI/edit?gid=0#gid=0)

### 2. Country ISP Protocols & Sensitivity Rules Google Sheet
- **Worksheet**: `Data & Information Types Dataset`
- **Fallback URL Config**: `DEFAULT_ISP_GOOGLE_SHEET_URL`
- **Strategy**: `GoogleSheetsISPStrategy` (with 12-hour Redis caching)
- 🔗 **Spreadsheet Link**: [HDX Information Sensitivity Protocols (ISP) Data & Information Types](https://docs.google.com/spreadsheets/d/1Z5wj6H6WV2E8VN9r6y8AfdgOltTNL-z2KIlbnNzzPok/edit?gid=886310371#gid=886310371)

---

## 9: Stage 5 – Risk Level Scoring & Propagation

Categorical outputs are mapped to numeric scores (0 to 3) and propagated hierarchically.

### 1. Sheet-Level Risk
Every sheet calculates independent Personal Data and Non-Personal Data risk scores:

$$\text{Sheet Risk} = \max(\text{personal\_data\_risk\_level}, \text{non\_personal\_data\_risk\_level})$$

### 2. Resource-Level Aggregation
The dataset resource file takes the maximum risk score across all contained sheets:

$$\text{Resource Sensitivity Level} = \max_{s \in \text{Sheets}}(\text{Sheet Risk}_s)$$

### Resource Categorization Flags
- `not-sensitive` (Risk = 0)
- `sensitive-pd` (Personal Data risk > 0)
- `sensitive-non-pd` (Non-Personal Data risk > 0)
- `sensitive-pd-and-non-pd` (Both risks > 0)

---

## 10: Fail-Safes & Error Fallback Logic

When auditing flagged datasets, QAs should be aware of automated safety defaults:

### 1. Fail-Safe Escalation (`FR-SDD-035`, `FR-SDD-039`)
If classification fails, encounters an exception, or returns `UNDETERMINED`, the pipeline automatically promotes non-personal data sensitivity to **`SEVERE_SENSITIVE`** (Risk Level 3).

### 2. Default ISP Fallback (`FR-SDD-033`, `FR-SDD-047`)
If no country-specific ISP protocol exists for the dataset location, the system applies the global default ISP prompt template.

### 3. Comprehensive Logging
Every exception or `UNDETERMINED` flag records diagnostic context directly into the report's explanation field for QA review.

---

## 11: Key Summary & Takeaways

1. **Live Google Sheets Integration**: Prompts and country ISP rules are loaded dynamically from Google Sheets, allowing real-time policy adjustments by QAs and domain experts.
2. **Three Scanning Paths**: Datasets are evaluated for **Personal Data**, **Non-Personal Data (ISP)**, and **README / Metadata Sheets** (`readme_scan`).
3. **Deterministic & Auditable**: Seeded random sampling (`seed=42`) and structured explanations ensure evaluations can be audited and reproduced.
4. **Safety First**: System defaults to `SEVERE_SENSITIVE` on ambiguity or system failure to protect vulnerable communities.
5. **Context Matters**: Header names alone do not trigger PII flags—actual sample values, table context, and documentation text determine sensitivity.
