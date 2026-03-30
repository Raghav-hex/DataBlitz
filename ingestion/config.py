"""
datablitz.ingestion.config
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Centralised settings loaded from environment / .env file.
All source adapters receive this settings object — no ad-hoc os.getenv() calls.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── USA ──────────────────────────────────────────────────────────────────
    fred_api_key: str = Field(default="", description="FRED API key")
    bls_api_key: str = Field(default="", description="BLS v2 API key")
    noaa_cdo_token: str = Field(default="", description="NOAA CDO token")

    # ── India ──────────────────────────────────────────────────────────────
    data_gov_in_key: str = Field(default="", description="data.gov.in API key")

    # ── Cache ──────────────────────────────────────────────────────────────
    cache_db_path: str = Field(default="./data/cache/datablitz.sqlite")
    cache_ttl_hours: int = Field(default=48)

    # ── HTTP ───────────────────────────────────────────────────────────────
    http_timeout_seconds: float = Field(default=30.0)
    http_max_connections: int = Field(default=10)
