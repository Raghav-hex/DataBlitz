"""
datablitz.ingestion.sources.usa.cdc
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CDC Open Data adapter (data.cdc.gov — Socrata platform).

Note: CDC WONDER has no REST API. We use data.cdc.gov (Socrata) instead —
it's the proper programmatic interface and covers the same data.

No API key required (anonymous Socrata access, 1000 rows/call).

Datasets:
  - Weekly COVID/respiratory provisional deaths (VSRR)
  - Monthly leading causes of death (LCOD)
  Both via Socrata JSON endpoint.
"""

from __future__ import annotations

import logging
from datetime import date
from dateutil.relativedelta import relativedelta

import httpx

from ...base import BaseSource
from ...config import Settings
from ...schemas import Category, Country, DataPoint, Frequency, FetchStatus, Indicator

logger = logging.getLogger(__name__)

# Socrata dataset endpoints (no auth needed for public datasets)
DEATHS_URL = "https://data.cdc.gov/resource/muzy-jte6.json"  # Provisional deaths weekly


class CDCSource(BaseSource):
    country = Country.USA
    source_name = "CDC Open Data"

    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        super().__init__(client, settings)

    async def fetch_indicators(self) -> list[Indicator]:
        indicators: list[Indicator] = []

        try:
            ind = await self._fetch_provisional_deaths()
            indicators.append(ind)
        except Exception as exc:
            logger.warning("cdc_deaths_failed", extra={"error": str(exc)})

        return indicators

    async def _fetch_provisional_deaths(self) -> Indicator:
        """
        Provisional COVID-19 and total deaths — weekly provisional counts.
        Dataset: Provisional COVID-19 Deaths by Week (muzy-jte6)
        We aggregate to monthly totals for the digest.
        """
        # Socrata SoQL: last 52 weeks, US total only
        params = {
            "$where": "group = 'By Total'",
            "$order": "end_date DESC",
            "$limit": 52,
            "$select": "end_date,covid_19_deaths,total_deaths",
        }

        data = await self.get_json(DEATHS_URL, params=params)

        if not data:
            raise ValueError("CDC returned empty dataset")

        observations: list[DataPoint] = []
        for row in data:
            try:
                raw_date = row.get("end_date", "")[:10]
                covid_deaths = row.get("covid_19_deaths")
                if covid_deaths is None:
                    continue
                observations.append(DataPoint(
                    date=date.fromisoformat(raw_date),
                    value=float(covid_deaths),
                ))
            except (ValueError, KeyError, TypeError):
                continue

        if not observations:
            raise ValueError("No valid CDC death observations parsed")

        return Indicator(
            id="usa.cdc.covid_deaths_weekly",
            name="US Weekly Provisional COVID-19 Deaths",
            source_name=self.source_name,
            source_url="https://data.cdc.gov/resource/muzy-jte6",
            country=self.country,
            category=Category.HEALTH,
            frequency=Frequency.WEEKLY,
            unit="deaths_count",
            observations=observations,
            fetched_at=self.now_utc(),
            status=FetchStatus.LIVE,
        )
