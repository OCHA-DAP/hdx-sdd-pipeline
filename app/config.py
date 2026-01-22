"""
Configuration management for the application.
This module centralizes all configuration settings, making them easy to modify
and test. It follows the Single Responsibility Principle by handling only
configuration concerns.
"""

from pathlib import Path
from typing import Set
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    # API Settings
    api_title: str = "HDX SSD Pipeline API"
    api_version: str = "1.0.0"
    api_prefix: str = "/api"

    # CORS Settings
    cors_origins: list[str] = ["http://localhost:3000"]

    # File Upload Settings
    allowed_extensions: Set[str] = {'.csv', '.xlsx'}
    max_upload_size: int = 100 * 1024 * 1024  # 100MB

    # Directory Paths (can be overridden via environment variables)
    datasets_dir: Path = Path(__file__).parent.parent / "research" / "data"
    reports_dir: Path = Path(__file__).parent.parent / "research" / "results" / "test_results"

    # Groundtruth Settings
    groundtruth_dir_name: str = "groundtruth2"
    excluded_model_dirs: Set[str] = {"groundtruth", "groundtruth2"}

    # Logging
    log_level: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "allow"

    @property
    def groundtruth_dir(self) -> Path:
        """Get the groundtruth directory path."""
        return self.reports_dir / self.groundtruth_dir_name

    def ensure_directories(self) -> None:
        """Ensure all required directories exist."""
        self.datasets_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.groundtruth_dir.mkdir(parents=True, exist_ok=True)


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Using lru_cache ensures we only create one Settings instance,
    improving performance and consistency.
    """
    return Settings()
