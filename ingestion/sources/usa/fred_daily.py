"""
datablitz.ingestion.sources.usa.fred_daily
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
FRED daily series — extends the existing monthly FRED adapter.

These 5 daily series are game-changers because they move BEFORE
government monthly data: WTI moves before CPI, gold moves before
USD confidence breaks, yield spread inverts before recession prints.

Uses your existing FRED_API_KEY — no new credentials needed.

Series:
  DCOILWTICO   WTI Crude Oil (daily, USD/barrel)
  GOLDAMGBD228NLBM  Gold price daily fix (USD/troy oz)
  DTWEXBGS     USD Trade-Weighted Index Broad (daily, index)
  DHHNGSP      Natural Gas spot Henry Hub (daily, USD/MMBtu)
  T10Y2Y       10Y-2Y Treasury spread (daily, %)  ← already monthly, now daily

Why each matters:
  WTI      → energy cost for all 4 countries, inflation leading indicator
  Gold     → safe-haven flow signal; spikes = stress somewhere in system
  USD      → when USD strengthens, EM currencies (BRL, INR) weaken
  Gas      → UK/EU energy crisis signal; India industrial cost
  T10Y2Y   → yield curve daily — inversion is a real-time recession signal
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone

import httpx

from ...base import BaseSource
from ...config import Settings
from ...schemas import Category, Country, DataPoint, Frequency, FetchStatus, Indicator

logger = logging.getLogger(__name__)

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# (series_id, name, unit, category, indicator_id_suffix)
DAILY_SERIES = [
    ("DCOILWTICO",          "WTI Crude Oil Price",             "usd_per_barrel",   Category.ECONOMIC, "WTI"),
    ("GOLDAMGBD228NLBM",    "Gold Price (Daily Fix)",          "usd_per_troy_oz",  Category.ECONOMIC, "GOLD"),
    ("DTWEXBGS",            "US Dollar Trade-Weighted Index",  "index",            Category.ECONOMIC, "USD_INDEX"),
    ("DHHNGSP",             "Natural Gas Spot (Henry Hub)",    "usd_per_mmbtu",    Category.ECONOMIC, "NATGAS"),
    ("T10Y2Y",              "US 10Y-2Y Treasury Spread",       "percent",          Category.ECONOMIC, "T10Y2Y_DAILY"),
]

DAYS_BACK = 30  # last 30 trading days


class FREDDailySource(BaseSource):
    """
    Fetches daily FRED series. Separate from FREDSource (monthly)
    so the two don't interfere — monthly is cached differently.
    """
    country = Country.USA
    source_name = "FRED Daily"

    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        super().__init__(client, settings)
        if not settings.fred_api_key:
            raise ValueError("FRED_API_KEY is required but not set")
        self._api_key = settings.fred_api_key

    async def fetch_indicators(self) -> list[Indicator]:
        indicators: list[Indicator] = []
        for series_id, name, unit, category, suffix in DAILY_SERIES:
            try:
                ind = await self._fetch_daily(series_id, name, unit, category, suffix)
                indicators.append(ind)
            except Exception as exc:
                logger.warning(f"FRED daily [{series_id}]: {exc}")
        return indicators

    async def _fetch_daily(
        self, series_id: str, name: str, unit: str,
        category: Category, suffix: str,
    ) -> Indicator:
        params = {
            "series_id":        series_id,
            "api_key":          self._api_key,
            "file_type":        "json",
            "sort_order":       "desc",
            "limit":            DAYS_BACK,
            "observation_start": (datetime.now(tz=timezone.utc).date()
                                   .replace(day=1)).isoformat(),
        }
        data = await self.get_json(BASE_URL, params=params)
        raw  = data.get("observations", [])

        observations: list[DataPoint] = []
        for obs in raw:
            try:
                if obs["value"] in (".", "", None):
                    continue
                observations.append(DataPoint(
                    date=date.fromisoformat(obs["date"]),
                    value=float(obs["value"]),
                ))
            except (ValueError, KeyError):
                continue

        if not observations:
            raise ValueError(f"FRED daily: no valid observations for {series_id}")

        return Indicator(
            id=f"usa.fred.daily.{suffix}",
            name=name,
            source_name=self.source_name,
            source_url=f"https://fred.stlouisfed.org/series/{series_id}",
            country=self.country,
            category=category,
            frequency=Frequency.DAILY,
            unit=unit,
            observations=observations,
            fetched_at=self.now_utc(),
            status=FetchStatus.LIVE,
        )


def format_daily_for_prompt(indicators: list[Indicator]) -> str:
    """Format daily FRED series as a compact prompt section."""
    if not indicators:
        return ""

    lines = ["DAILY FINANCIAL CONDITIONS (FRED, last 30 days):"]
    for ind in indicators:
        if not ind.observations:
            continue
        latest = ind.latest
        pct    = ind.pct_change
        arrow  = "▲" if pct and pct > 0 else "▼" if pct and pct < 0 else "—"
        pct_s  = f" ({arrow}{abs(pct):.2f}% vs prev day)" if pct else ""
        # 5-day trend
        trend_obs = ind.observations[-5:]
        spark = " → ".join(f"{o.value:.2f}" for o in trend_obs)
        lines.append(
            f"  {ind.name}: {latest.value:.2f} {ind.unit}{pct_s}\n"
            f"    5d trend: {spark}"
        )

    return "\n".join(lines)
