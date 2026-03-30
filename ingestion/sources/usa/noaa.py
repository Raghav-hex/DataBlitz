"""
datablitz.ingestion.sources.usa.noaa
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
NOAA Climate Data Online (CDO) adapter.

API docs: https://www.ncdc.noaa.gov/cdo-web/webservices/v2
Endpoint: https://www.ncdc.noaa.gov/cdo-web/api/v2/data
Auth: Token in header (NOAA-CDO-Web-Services-Token)
Rate: 5 req/sec, 10k req/day
Format: JSON

Data fetched:
  - TAVG (Average Temperature) — US monthly, GHCND network
  - PRCP (Precipitation) — US monthly total
  Uses GHCND station: USW00094728 (Central Park, NYC — proxy for US baseline)
  + NOAA's own global surface temp anomaly series via their alternative endpoint
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

import httpx

from ...base import BaseSource
from ...config import Settings
from ...schemas import Category, Country, DataPoint, Frequency, FetchStatus, Indicator

logger = logging.getLogger(__name__)

BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"

# Global Surface Temperature Anomaly from NOAA — monthly
# dataset: GSOM (Global Summary of Month)
NOAA_QUERIES = [
    {
        "id": "usa.noaa.TAVG_NYC",
        "name": "US Avg Temperature (NYC/Central Park proxy)",
        "category": Category.CLIMATE,
        "unit": "tenths_of_celsius",
        "params": {
            "datasetid": "GSOM",
            "datatypeid": "TAVG",
            "stationid": "GHCND:USW00094728",  # Central Park
            "units": "metric",
        },
    },
    {
        "id": "usa.noaa.PRCP",
        "name": "US Monthly Precipitation (NYC proxy)",
        "category": Category.CLIMATE,
        "unit": "mm",
        "params": {
            "datasetid": "GSOM",
            "datatypeid": "PRCP",
            "stationid": "GHCND:USW00094728",
            "units": "metric",
        },
    },
]


class NOAASource(BaseSource):
    country = Country.USA
    source_name = "NOAA CDO"

    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        super().__init__(client, settings)
        if not settings.noaa_cdo_token:
            raise ValueError("NOAA_CDO_TOKEN is required but not set")
        self._token = settings.noaa_cdo_token

    async def fetch_indicators(self) -> list[Indicator]:
        # Date range: last 24 months
        end = datetime.now().date().replace(day=1)
        start = end - relativedelta(months=24)

        indicators: list[Indicator] = []
        for query in NOAA_QUERIES:
            try:
                ind = await self._fetch_query(query, start, end)
                indicators.append(ind)
            except Exception as exc:
                logger.warning("noaa_query_failed", extra={"id": query["id"], "error": str(exc)})

        return indicators

    async def _fetch_query(self, query: dict, start: date, end: date) -> Indicator:
        params = {
            **query["params"],
            "startdate": start.isoformat(),
            "enddate": end.isoformat(),
            "limit": 1000,
            "includemetadata": "false",
        }
        headers = {"token": self._token}

        resp = await self.client.get(BASE_URL, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        if not results:
            raise ValueError(f"NOAA returned no results for {query['id']}")

        observations = []
        for item in results:
            try:
                observations.append(DataPoint(
                    date=date.fromisoformat(item["date"][:10]),
                    value=item["value"],
                ))
            except (ValueError, KeyError):
                continue

        if not observations:
            raise ValueError("No valid observations parsed from NOAA response")

        return Indicator(
            id=query["id"],
            name=query["name"],
            source_name=self.source_name,
            source_url="https://www.ncdc.noaa.gov/cdo-web/",
            country=self.country,
            category=query["category"],
            frequency=Frequency.MONTHLY,
            unit=query["unit"],
            observations=observations,
            fetched_at=self.now_utc(),
            status=FetchStatus.LIVE,
        )
