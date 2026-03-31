# Agent Instructions

These rules apply to all AI-assisted code changes in this repository.

## Core Principles

1. Understand existing logic before editing.
2. Preserve current behavior unless a request explicitly requires a behavior change.
3. Prefer the smallest possible diff that solves the request.
4. Extend existing modules, helpers, and patterns before adding new abstractions.
5. Avoid unrelated refactors, renames, or formatting-only edits.
6. Keep public interfaces and config contracts stable unless change is required.
7. If behavior must change, clearly explain what changed and why.

## Required Workflow

1. Read the relevant files and existing implementation first.
2. Read the requirements baselines in `requirements/SDD_PIPELINE_FUNCTIONAL_REQUIREMENTS.md` and `requirements/LLM_EVALUATION_FUNCTIONAL_REQUIREMENTS.md` before implementing related changes.
3. Identify the minimal edit location.
4. Before implementation, update the relevant requirements file first and get human approval.
5. Implement only the necessary code changes.
6. Run targeted validation (tests, lint, or smoke checks) for the touched area.
7. Summarize the change and any assumptions.

## Requirements Baselines

- The requirements files in `requirements/` are the source of truth for functional task scope.
- When adding a feature, update the relevant requirements file first, get human approval, then implement.
- Requirement status markers must be used consistently:
  - `[x]` = implemented
  - `[ ]` = planned/not yet implemented

## Constraints

- Do not rewrite working code when a local patch is sufficient.
- Do not introduce new dependencies unless there is a clear need.
- Do not remove logs, error handling, or guards without replacing equivalent behavior.
- Do not change environment variable names or defaults without explicit request.

## When Uncertain

- State assumptions explicitly.
- Ask a clarifying question before making broad or risky changes.
