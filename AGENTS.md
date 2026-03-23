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
2. Identify the minimal edit location.
3. Implement only the necessary code changes.
4. Run targeted validation (tests, lint, or smoke checks) for the touched area.
5. Summarize the change and any assumptions.

## Constraints

- Do not rewrite working code when a local patch is sufficient.
- Do not introduce new dependencies unless there is a clear need.
- Do not remove logs, error handling, or guards without replacing equivalent behavior.
- Do not change environment variable names or defaults without explicit request.

## When Uncertain

- State assumptions explicitly.
- Ask a clarifying question before making broad or risky changes.
