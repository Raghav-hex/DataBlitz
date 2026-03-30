"""
datablitz.ingestion.sources.india.worldbank
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
World Bank API adapter for India indicators.

Replaces: RBI DBIE (no REST API) and HMIS/NHP (download-only).

API: https://api.worldbank.org/v2/country/{country}/indicator/{indicator}
No auth required. Format: JSON (format=json param).
Returns paginated results — we fetch page 1 (most recent first).

Indicators for India:
  NY.GDP.MKTP.KD.ZG  - GDP growth rate (annual, %)
  FP.CPI.TOTL.ZG     - CPI inflation annual % (cross-check)
  SL.UEM.TOTL.ZS     - Unemployment (% of total labor force)
  SP.POP.TOTL         - Population (absolute)
  SH.STA.MMRT         - Maternal mortality ratio
  SH.XPD.CHEX.GD.ZS  - Health expenditure (% of GDP)
"""

from __future__ import annotations

import logging
from datetime import date

import httpx

from ...base import BaseSource
from ...config import Settings
from ...schemas import Category, Country, DataPoint, Frequency, FetchStatus, Indicator

logger = logging.getLogger(__name__)

BASE = "https://api.worldbank.org/v2/country/IN/indicator"

INDICATORS = [
    {
        "id": "india.wb.GDP_GROWTH",
        "code": "NY.GDP.MKTP.KD.ZG",
        "name": "India GDP Growth Rate (annual)",
        "category": Category.ECONOMIC,
        "unit": "percent",
    },
    {
        "id": "india.wb.INFLATION",
        "code": "FP.CPI.TOTL.ZG",
        "name": "India CPI Inflation (annual %)",
        "category": Category.ECONOMIC,
        "unit": "percent",
    },
    {
        "id": "india.wb.UNEMPLOYMENT",
        "code": "SL.UEM.TOTL.ZS",
        "name": "India Unemployment Rate",
        "category": Category.SOCIAL,
        "unit": "percent_labor_force",
    },
    {
        "id": "india.wb.HEALTH_EXP",
        "code": "SH.XPD.CHEX.GD.ZS",
        "name": "India Health Expenditure (% GDP)",
        "category": Category.HEALTH,
        "unit": "percent_of_gdp",
    },
]


class WorldBankIndiaSource(BaseSource):
    country = Country.INDIA
    source_name = "World Bank"

    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        super().__init__(client, settings)

    async def fetch_indicators(self) -> list[Indicator]:
        indicators: list[Indicator] = []
        for ind_meta in INDICATORS:
            try:
                ind = await self._fetch_indicator(ind_meta)
                indicators.append(ind)
            except Exception as exc:
                logger.warning("wb_india_failed", extra={"code": ind_meta["code"], "error": str(exc)})
        return indicators

    async def _fetch_indicator(self, meta: dict) -> Indicator:
        url = f"{BASE}/{meta['code']}"
        params = {
            "format": "json",
            "mrv": 15,          # Most recent 15 values
            "per_page": 15,
            "date": "2010:2024",
        }

        data = await self.get_json(url, params=params)

        # World Bank returns [metadata_dict, [observations...]]
        if not data or len(data) < 2 or not data[1]:
            raise ValueError(f"World Bank returned no data for {meta['code']}")

        observations = self._parse_wb(data[1])

        if not observations:
            raise ValueError(f"WB India: no valid observations for {meta['id']}")

        return Indicator(
            id=meta["id"],
            name=meta["name"],
            source_name=self.source_name,
            source_url=f"https://data.worldbank.org/indicator/{meta['code']}?locations=IN",
            country=self.country,
            category=meta["category"],
            frequency=Frequency.ANNUAL,
            unit=meta["unit"],
            observations=observations,
            fetched_at=self.now_utc(),
            status=FetchStatus.LIVE,
        )

    @staticmethod
    def _parse_wb(rows: list[dict]) -> list[DataPoint]:
        points: list[DataPoint] = []
        for row in rows:
            try:
                year_str = row.get("date", "")
                value = row.get("value")
                if value is None or year_str == "":
                    continue
                # Annual data — use Jan 1 of that year
                year = int(year_str[:4])
                points.append(DataPoint(date=date(year, 1, 1), value=float(value)))
            except (ValueError, TypeError, KeyError):
                continue
        return points
