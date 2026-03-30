"""
datablitz.ingestion.sources.usa.fred
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
FRED (Federal Reserve Economic Data) adapter.

API: https://api.stlouisfed.org/fred/series/observations
Auth: API key (query param)
Rate limit: 120 req/min — well within our needs
Format: JSON

Series fetched:
  UNRATE   - US Unemployment Rate (monthly, %)
  CPIAUCSL - CPI All Urban Consumers (monthly, index)
  GDP      - Real GDP (quarterly, billions USD)
  FEDFUNDS - Federal Funds Effective Rate (monthly, %)
  T10Y2Y   - 10-Year minus 2-Year Treasury spread (daily → monthly)
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone

import httpx

from ...base import BaseSource, with_retries
from ...config import Settings
from ...schemas import Category, Country, DataPoint, Frequency, FetchStatus, Indicator

logger = logging.getLogger(__name__)

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# ─── Series catalogue ─────────────────────────────────────────────────────────
# Each entry: (series_id, human_name, category, frequency, unit)
SERIES = [
    ("UNRATE",   "US Unemployment Rate",              Category.ECONOMIC, Frequency.MONTHLY,   "percent"),
    ("CPIAUCSL", "US CPI All Urban Consumers",        Category.ECONOMIC, Frequency.MONTHLY,   "index_1982-84=100"),
    ("GDP",      "US Real GDP",                       Category.ECONOMIC, Frequency.QUARTERLY, "billions_usd"),
    ("FEDFUNDS", "US Federal Funds Effective Rate",   Category.ECONOMIC, Frequency.MONTHLY,   "percent"),
    ("T10Y2Y",   "US 10Y-2Y Treasury Spread",         Category.ECONOMIC, Frequency.DAILY,     "percent"),
]

# How many recent observations to keep per series
OBS_LIMIT = 24  # 2 years for monthly; adjust per-series if needed


class FREDSource(BaseSource):
    country = Country.USA
    source_name = "FRED"

    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        super().__init__(client, settings)
        if not settings.fred_api_key:
            raise ValueError("FRED_API_KEY is required but not set in environment")
        self._api_key = settings.fred_api_key

    async def fetch_indicators(self) -> list[Indicator]:
        indicators: list[Indicator] = []

        for series_id, name, category, frequency, unit in SERIES:
            try:
                indicator = await self._fetch_series(series_id, name, category, frequency, unit)
                indicators.append(indicator)
                logger.debug("fred_series_ok", extra={"series": series_id, "obs": len(indicator.observations)})
            except Exception as exc:
                logger.warning("fred_series_failed", extra={"series": series_id, "error": str(exc)})
                # Don't abort — keep fetching remaining series

        return indicators

    async def _fetch_series(
        self,
        series_id: str,
        name: str,
        category: Category,
        frequency: Frequency,
        unit: str,
    ) -> Indicator:
        params = {
            "series_id": series_id,
            "api_key": self._api_key,
            "file_type": "json",
            "sort_order": "desc",      # newest first so we can slice cheaply
            "limit": OBS_LIMIT,
        }

        # For daily series, aggregate to monthly to keep digest concise
        if frequency == Frequency.DAILY:
            params["frequency"] = "m"
            params["aggregation_method"] = "avg"

        data = await self.get_json(BASE_URL, params=params)

        observations = self._parse_observations(data.get("observations", []))

        if not observations:
            raise ValueError(f"FRED returned zero valid observations for {series_id}")

        return Indicator(
            id=f"usa.fred.{series_id}",
            name=name,
            source_name=self.source_name,
            source_url=f"https://fred.stlouisfed.org/series/{series_id}",
            country=self.country,
            category=category,
            frequency=frequency if frequency != Frequency.DAILY else Frequency.MONTHLY,
            unit=unit,
            observations=observations,
            fetched_at=self.now_utc(),
            status=FetchStatus.LIVE,
        )

    @staticmethod
    def _parse_observations(raw: list[dict]) -> list[DataPoint]:
        """
        Parse FRED observation list. FRED uses "." for missing values —
        DataPoint.coerce_value handles this via ValueError which we catch here.
        """
        points: list[DataPoint] = []
        for obs in raw:
            try:
                points.append(
                    DataPoint(
                        date=date.fromisoformat(obs["date"]),
                        value=obs["value"],
                    )
                )
            except (ValueError, KeyError):
                # Missing sentinel "." or malformed entry — skip silently
                continue
        return points
