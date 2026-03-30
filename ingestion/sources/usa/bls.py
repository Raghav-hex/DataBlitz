"""
datablitz.ingestion.sources.usa.bls
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
BLS (Bureau of Labor Statistics) v2 API adapter.

API docs: https://www.bls.gov/developers/api_python.htm
Endpoint: https://api.bls.gov/publicAPI/v2/timeseries/data/
Auth: Registration key (query param) — 500 series/day, 50 series/request
Format: POST with JSON body

Series fetched:
  CES0000000001  - Total Nonfarm Payrolls (monthly, thousands)
  LNS14000000    - Unemployment Rate (seasonally adjusted, monthly, %)
  CES0500000003  - Average Hourly Earnings, Private (monthly, USD)
  CUUR0000SA0    - CPI-U All Items (monthly, index — cross-check vs FRED)
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone

import httpx

from ...base import BaseSource
from ...config import Settings
from ...schemas import Category, Country, DataPoint, Frequency, FetchStatus, Indicator

logger = logging.getLogger(__name__)

BASE_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

SERIES = [
    ("CES0000000001", "US Nonfarm Payrolls",               Category.ECONOMIC, "thousands_of_persons"),
    ("LNS14000000",   "US Unemployment Rate (SA)",          Category.ECONOMIC, "percent"),
    ("CES0500000003", "US Avg Hourly Earnings (Private)",   Category.SOCIAL,   "usd_per_hour"),
]


class BLSSource(BaseSource):
    country = Country.USA
    source_name = "BLS"

    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        super().__init__(client, settings)
        if not settings.bls_api_key:
            raise ValueError("BLS_API_KEY is required but not set")
        self._api_key = settings.bls_api_key

    async def fetch_indicators(self) -> list[Indicator]:
        """
        BLS v2: POST a list of series IDs in one call (max 50 per request).
        We batch all series in a single POST to stay within rate limits.
        """
        series_ids = [s[0] for s in SERIES]
        payload = {
            "seriesid": series_ids,
            "registrationkey": self._api_key,
            "latest": "false",
            "startyear": str(datetime.now().year - 2),
            "endyear": str(datetime.now().year),
        }

        resp = await self.client.post(BASE_URL, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != "REQUEST_SUCCEEDED":
            msg = data.get("message", ["Unknown BLS error"])
            raise ValueError(f"BLS API error: {msg}")

        # Build lookup from series_id -> raw data
        raw_map: dict[str, list[dict]] = {}
        for series_data in data.get("Results", {}).get("series", []):
            raw_map[series_data["seriesID"]] = series_data.get("data", [])

        indicators: list[Indicator] = []
        for series_id, name, category, unit in SERIES:
            raw = raw_map.get(series_id, [])
            if not raw:
                logger.warning("bls_no_data", extra={"series": series_id})
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
                    observations=observations,
                    fetched_at=self.now_utc(),
                    status=FetchStatus.LIVE,
                ))
            except Exception as exc:
                logger.warning("bls_parse_failed", extra={"series": series_id, "error": str(exc)})

        return indicators

    @staticmethod
    def _parse_observations(raw: list[dict]) -> list[DataPoint]:
        """
        BLS format: {"year": "2024", "period": "M01", "value": "158413"}
        Periods: M01-M12 = Jan-Dec. M13 = annual avg (skip).
        """
        points: list[DataPoint] = []
        for item in raw:
            period = item.get("period", "")
            if not period.startswith("M") or period == "M13":
                continue
            month = int(period[1:])
            year = int(item["year"])
            try:
                points.append(DataPoint(
                    date=date(year, month, 1),
                    value=item["value"],
                ))
            except (ValueError, KeyError):
                continue
        return points
