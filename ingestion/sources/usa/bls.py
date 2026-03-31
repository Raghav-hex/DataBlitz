"""
datablitz.ingestion.sources.usa.bls
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
BLS (Bureau of Labor Statistics) adapter.

v1 API: No key required. 25 queries/day, 10 years history.
v2 API: Registration key → 500 queries/day, 20 years, net/pct calc.
        Register free at: https://data.bls.gov/registrationEngine/

We use v1 for zero-config operation, upgrade to v2 when key is set.

Series fetched:
  CES0000000001  - Total Nonfarm Payrolls (monthly, thousands)
  LNS14000000    - Unemployment Rate SA (monthly, %)
  CES0500000003  - Avg Hourly Earnings Private (monthly, USD)
"""

from __future__ import annotations

import logging
from datetime import date, datetime

import httpx

from ...base import BaseSource
from ...config import Settings
from ...schemas import Category, Country, DataPoint, Frequency, FetchStatus, Indicator

logger = logging.getLogger(__name__)

V1_URL = "https://api.bls.gov/publicAPI/v1/timeseries/data/"
V2_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

SERIES = [
    ("CES0000000001", "US Nonfarm Payrolls",             Category.ECONOMIC, "thousands_of_persons"),
    ("LNS14000000",   "US Unemployment Rate (SA)",        Category.ECONOMIC, "percent"),
    ("CES0500000003", "US Avg Hourly Earnings (Private)", Category.SOCIAL,   "usd_per_hour"),
]


class BLSSource(BaseSource):
    country = Country.USA
    source_name = "BLS"

    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        super().__init__(client, settings)
        self._api_key = settings.bls_api_key  # empty string = use v1

    @property
    def _using_v2(self) -> bool:
        return bool(self._api_key)

    async def fetch_indicators(self) -> list[Indicator]:
        if self._using_v2:
            return await self._fetch_v2()
        else:
            logger.info("BLS: no key set — using v1 (no key required)")
            return await self._fetch_v1_all()

    async def _fetch_v1_all(self) -> list[Indicator]:
        """V1: one request per series (no batch), no key needed."""
        indicators = []
        for series_id, name, category, unit in SERIES:
            try:
                ind = await self._fetch_v1_series(series_id, name, category, unit)
                indicators.append(ind)
            except Exception as exc:
                logger.warning("bls_v1_failed", extra={"series": series_id, "error": str(exc)})
        return indicators

    async def _fetch_v1_series(self, series_id, name, category, unit) -> Indicator:
        url = f"{V1_URL}{series_id}"
        data = await self.get_json(url)

        if data.get("status") != "REQUEST_SUCCEEDED":
            raise ValueError(f"BLS v1 error for {series_id}: {data.get('message')}")

        raw = data.get("Results", {}).get("series", [{}])[0].get("data", [])
        observations = self._parse_observations(raw)

        if not observations:
            raise ValueError(f"BLS: no observations for {series_id}")

        return Indicator(
            id=f"usa.bls.{series_id}",
            name=name,
            source_name=self.source_name,
            source_url=f"https://data.bls.gov/timeseries/{series_id}",
            country=self.country,
            category=category,
            frequency=Frequency.MONTHLY,
            unit=unit,
            observations=observations[-24:],
            fetched_at=self.now_utc(),
            status=FetchStatus.LIVE,
        )

    async def _fetch_v2(self) -> list[Indicator]:
        """V2: batch all series in one POST."""
        payload = {
            "seriesid": [s[0] for s in SERIES],
            "registrationkey": self._api_key,
            "startyear": str(datetime.now().year - 2),
            "endyear": str(datetime.now().year),
        }
        resp = await self.client.post(V2_URL, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != "REQUEST_SUCCEEDED":
            raise ValueError(f"BLS v2 error: {data.get('message')}")

        raw_map = {
            s["seriesID"]: s.get("data", [])
            for s in data.get("Results", {}).get("series", [])
        }
        indicators = []
        for series_id, name, category, unit in SERIES:
            raw = raw_map.get(series_id, [])
            if not raw:
                continue
            try:
                observations = self._parse_observations(raw)
                indicators.append(Indicator(
                    id=f"usa.bls.{series_id}",
                    name=name,
                    source_name=self.source_name,
                    source_url=f"https://data.bls.gov/timeseries/{series_id}",
                    country=self.country,
                    category=category,
                    frequency=Frequency.MONTHLY,
                    unit=unit,
                    observations=observations[-24:],
                    fetched_at=self.now_utc(),
                    status=FetchStatus.LIVE,
                ))
            except Exception as exc:
                logger.warning("bls_v2_parse_failed", extra={"series": series_id, "error": str(exc)})
        return indicators

    @staticmethod
    def _parse_observations(raw: list[dict]) -> list[DataPoint]:
        """BLS format: {"year": "2024", "period": "M01", "value": "158413"}"""
        points = []
        for item in raw:
            period = item.get("period", "")
            if not period.startswith("M") or period == "M13":
                continue
            try:
                month = int(period[1:])
                year = int(item["year"])
                points.append(DataPoint(date=date(year, month, 1), value=item["value"]))
            except (ValueError, KeyError):
                continue
        return points
