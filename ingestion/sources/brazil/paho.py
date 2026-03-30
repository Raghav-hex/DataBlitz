"""
datablitz.ingestion.sources.brazil.paho
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
PAHO PLISA (Pan American Health Organization) adapter for Brazil health data.

Replaces: DATASUS (no REST API, form-based TabNet system).

API: https://opendata.paho.org/api/v1/ (PAHO Open Data Portal)
Also: World Bank Health data for Brazil as fallback.
No auth required.

We pull:
  - Life expectancy at birth (Brazil)
  - Under-5 mortality rate
  - Tuberculosis incidence
  (Annual data — PAHO publishes with ~1-2 year lag, normal for health stats)
"""

from __future__ import annotations

import logging
from datetime import date

import httpx

from ...base import BaseSource
from ...config import Settings
from ...schemas import Category, Country, DataPoint, Frequency, FetchStatus, Indicator

logger = logging.getLogger(__name__)

# We use World Bank health indicators for Brazil as the clean API source
WB_BASE = "https://api.worldbank.org/v2/country/BR/indicator"

INDICATORS = [
    {
        "id": "brazil.health.LIFE_EXPECTANCY",
        "code": "SP.DYN.LE00.IN",
        "name": "Brazil Life Expectancy at Birth",
        "unit": "years",
    },
    {
        "id": "brazil.health.UNDER5_MORTALITY",
        "code": "SH.DYN.MORT",
        "name": "Brazil Under-5 Mortality Rate",
        "unit": "per_1000_live_births",
    },
    {
        "id": "brazil.health.TB_INCIDENCE",
        "code": "SH.TBS.INCD",
        "name": "Brazil Tuberculosis Incidence",
        "unit": "per_100k_population",
    },
]


class PAHOBrazilSource(BaseSource):
    country = Country.BRAZIL
    source_name = "PAHO / World Bank Health"

    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        super().__init__(client, settings)

    async def fetch_indicators(self) -> list[Indicator]:
        indicators: list[Indicator] = []
        for meta in INDICATORS:
            try:
                ind = await self._fetch(meta)
                indicators.append(ind)
            except Exception as exc:
                logger.warning("paho_brazil_failed", extra={"id": meta["id"], "error": str(exc)})
        return indicators

    async def _fetch(self, meta: dict) -> Indicator:
        url = f"{WB_BASE}/{meta['code']}"
        params = {"format": "json", "mrv": 15, "per_page": 15}
        data = await self.get_json(url, params=params)

        if not data or len(data) < 2 or not data[1]:
            raise ValueError(f"WB/PAHO no data for {meta['code']}")

        observations = []
        for row in data[1]:
            try:
                value = row.get("value")
                year = int(row["date"][:4])
                if value is None:
                    continue
                observations.append(DataPoint(date=date(year, 1, 1), value=float(value)))
            except (ValueError, KeyError, TypeError):
                continue

        if not observations:
            raise ValueError(f"No valid observations for {meta['id']}")

        return Indicator(
            id=meta["id"],
            name=meta["name"],
            source_name=self.source_name,
            source_url=f"https://data.worldbank.org/indicator/{meta['code']}?locations=BR",
            country=self.country,
            category=Category.HEALTH,
            frequency=Frequency.ANNUAL,
            unit=meta["unit"],
            observations=observations,
            fetched_at=self.now_utc(),
            status=FetchStatus.LIVE,
        )
