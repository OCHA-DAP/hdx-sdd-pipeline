"""Pydantic schemas for the research router."""

from __future__ import annotations
from typing import Any
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# SDD report building blocks
# ---------------------------------------------------------------------------


class PersonalData(BaseModel):
    sensitivity: str = 'UNDETERMINED'
    explanation: str | None = None


class NonPersonalData(BaseModel):
    sensitivity: str = 'UNDETERMINED'
    explanation: str | None = None
    sensitive_columns: list[str] = []
    cited_isp_rules: list[str] = []
    isp_name: str | None = None


class ColumnReport(BaseModel):
    column_name: str
    sample_values: list[Any] = []
    personal_data: dict[str, Any] = {}


class SheetReport(BaseModel):
    resource_id: str | None = None
    file_name: str | None = None
    file_url: str | None = None
    sheet_name: str
    processing_timestamp: str | None = None
    processing_success: bool = True
    n_records: int = 0
    n_columns: int = 0
    completion_tokens: int = 0
    prompt_tokens: int = 0
    personal_data_sensitive: bool = False
    non_personal_data_sensitive: bool = False
    personal_data: PersonalData = PersonalData()
    non_personal_data: NonPersonalData = NonPersonalData()
    columns: list[ColumnReport] = []
    is_readme: bool = False


class SDDReport(BaseModel):
    resource_id: str
    sensitive: str
    timestamp: str
    sdd_report: list[SheetReport]


# ---------------------------------------------------------------------------
# API response shapes
# ---------------------------------------------------------------------------


class UploadResponse(BaseModel):
    message: str
    filename: str
    size: int
    template_path: str
    dataset_path: str


class BatchStartResponse(BaseModel):
    message: str
    datasets_count: int
    models_count: int
    skip_existing: bool


class BatchStatus(BaseModel):
    is_running: bool
    current_model: str | None
    completed_models: list[str]
    failed_models: list[str]
    started_at: str | None
    progress: int
