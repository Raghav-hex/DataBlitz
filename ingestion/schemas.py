"""
datablitz.ingestion.schemas
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Canonical Pydantic v2 schemas that ALL data sources must normalise into.
Strict validation everywhere — if a source returns bad data we fail loud
rather than silently propagating garbage into the AI narrative layer.
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# ─── Enums ────────────────────────────────────────────────────────────────────

class Country(str, Enum):
    USA    = "usa"
    UK     = "uk"
    INDIA  = "india"
    BRAZIL = "brazil"


class Category(str, Enum):
    ECONOMIC = "economic"
    HEALTH   = "health"
    CLIMATE  = "climate"
    SOCIAL   = "social"


class Frequency(str, Enum):
    DAILY     = "daily"
    WEEKLY    = "weekly"
    MONTHLY   = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL    = "annual"


class FetchStatus(str, Enum):
    LIVE  = "live"    # fresh from API
    STALE = "stale"   # served from cache after API failure
    ERROR = "error"   # total failure, no cache available


# ─── Core data point ──────────────────────────────────────────────────────────

class DataPoint(BaseModel):
    """A single time-series observation."""
    model_config = ConfigDict(frozen=True)

    date: date
    value: float

    @field_validator("value", mode="before")
    @classmethod
    def coerce_value(cls, v: Any) -> float:
        """
        Handle the '.' sentinel that FRED (and others) use for missing values.
        We raise ValueError so the parent normaliser can filter these out.
        """
        if v == "." or v is None:
            raise ValueError("missing sentinel value")
        return float(v)


# ─── Indicator (a named time series) ─────────────────────────────────────────

class Indicator(BaseModel):
    """
    A named, sourced time series. This is what every source adapter must
    produce. One Indicator = one metric (e.g. UK CPI, Brazil SELIC rate).
    """
    model_config = ConfigDict(frozen=True)

    # Identity
    id: str = Field(
        description="Globally unique slug, e.g. 'usa.fred.UNRATE'",
        pattern=r"^[a-z]+\.[a-z_]+\.[A-Za-z0-9_\-\.]+$",
    )
    name: str = Field(description="Human-readable name, e.g. 'US Unemployment Rate'")
    source_name: str = Field(description="Source organisation, e.g. 'FRED'")
    source_url: str = Field(description="Direct URL to the dataset or API endpoint used")

    # Classification
    country: Country
    category: Category
    frequency: Frequency
    unit: str = Field(description="Unit of measurement, e.g. 'percent', 'index_2015=100'")

    # Data
    observations: list[DataPoint] = Field(
        min_length=1,
        description="Chronologically sorted observations, oldest first",
    )

    # Fetch metadata
    fetched_at: datetime = Field(description="UTC timestamp when data was retrieved")
    status: FetchStatus = FetchStatus.LIVE

    @model_validator(mode="after")
    def observations_sorted(self) -> Indicator:
        """Enforce chronological sort — callers shouldn't have to worry."""
        sorted_obs = sorted(self.observations, key=lambda dp: dp.date)
        object.__setattr__(self, "observations", sorted_obs)
        return self

    # ── Convenience properties ─────────────────────────────────────────────

    @property
    def latest(self) -> DataPoint:
        return self.observations[-1]

    @property
    def previous(self) -> DataPoint | None:
        return self.observations[-2] if len(self.observations) >= 2 else None

    @property
    def pct_change(self) -> float | None:
        """Period-on-period % change between the last two observations."""
        if self.previous is None or self.previous.value == 0:
            return None
        return round(
            ((self.latest.value - self.previous.value) / abs(self.previous.value)) * 100,
            4,
        )

    @property
    def last_n(self) -> list[DataPoint]:
        """Last 12 observations (or fewer if series is short)."""
        return self.observations[-12:]


# ─── Country digest ───────────────────────────────────────────────────────────

class CountryDigest(BaseModel):
    """
    All indicators for a single country, as fetched in one pipeline run.
    This is what gets serialised to Cloudflare KV and handed to the AI layer.
    """
    model_config = ConfigDict(frozen=True)

    country: Country
    run_id: str = Field(description="ISO datetime string used as run identifier")
    indicators: list[Indicator]
    errors: list[str] = Field(
        default_factory=list,
        description="Non-fatal fetch errors encountered (source skipped, served stale)",
    )

    @property
    def by_category(self) -> dict[Category, list[Indicator]]:
        result: dict[Category, list[Indicator]] = {}
        for ind in self.indicators:
            result.setdefault(ind.category, []).append(ind)
        return result


# ─── Global digest (all countries) ───────────────────────────────────────────

class GlobalDigest(BaseModel):
    """
    The top-level object passed to the AI narrative engine.
    Contains all four country digests for a single weekly run.
    """
    model_config = ConfigDict(frozen=True)

    run_id: str
    generated_at: datetime
    digests: list[CountryDigest]

    @property
    def by_country(self) -> dict[Country, CountryDigest]:
        return {d.country: d for d in self.digests}

    def get(self, country: Country) -> CountryDigest | None:
        return self.by_country.get(country)
