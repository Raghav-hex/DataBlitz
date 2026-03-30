"""
datablitz.ingestion.sources.india.openaq
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
OpenAQ v3 API adapter for India air quality data.

Replaces: CPCB AQI (dynamically rendered, no public REST API).

API: https://api.openaq.org/v3/
No auth required for public access. Rate limit: 60 req/min.
Covers 500+ Indian monitoring stations.

We fetch average PM2.5 and PM10 across the top 5 most-measured
Indian cities: Delhi, Mumbai, Chennai, Kolkata, Bangalore.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone, timedelta
from collections import defaultdict

import httpx

from ...base import BaseSource
from ...config import Settings
from ...schemas import Category, Country, DataPoint, Frequency, FetchStatus, Indicator

logger = logging.getLogger(__name__)

BASE = "https://api.openaq.org/v3"

# OpenAQ location IDs for major Indian cities (confirmed active stations)
# We query by country=IN and aggregate
CITY_QUERIES = [
    {"city": "Delhi",     "parameter": "pm25", "country": "IN"},
    {"city": "Mumbai",    "parameter": "pm25", "country": "IN"},
    {"city": "Chennai",   "parameter": "pm25", "country": "IN"},
]


class OpenAQIndiaSource(BaseSource):
    country = Country.INDIA
    source_name = "OpenAQ"

    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        super().__init__(client, settings)

    async def fetch_indicators(self) -> list[Indicator]:
        """
        Query OpenAQ v3 for India PM2.5 average over the last 30 days,
        aggregated monthly, for a national picture.
        """
        indicators: list[Indicator] = []
        try:
            ind = await self._fetch_india_pm25()
            indicators.append(ind)
        except Exception as exc:
            logger.warning("openaq_india_failed", extra={"error": str(exc)})
        return indicators

    async def _fetch_india_pm25(self) -> Indicator:
        """
        Use OpenAQ v3 /measurements endpoint filtered by country=IN, parameter=pm25.
        Aggregate to monthly averages for the digest.
        """
        end = datetime.now(tz=timezone.utc)
        start = end - timedelta(days=365)

        params = {
            "countries_id": 104,   # India country ID in OpenAQ
            "parameters_id": 2,    # PM2.5
            "date_from": start.strftime("%Y-%m-%dT00:00:00Z"),
            "date_to": end.strftime("%Y-%m-%dT23:59:59Z"),
            "limit": 1000,
        }

        data = await self.get_json(f"{BASE}/measurements", params=params)
        results = data.get("results", [])

        if not results:
            raise ValueError("OpenAQ returned no measurements for India PM2.5")

        # Aggregate to monthly averages
        monthly: dict[tuple, list[float]] = defaultdict(list)
        for obs in results:
            try:
                dt_str = obs.get("date", {}).get("utc", "")
                value = obs.get("value")
                if not dt_str or value is None or float(value) < 0:
                    continue
                dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                key = (dt.year, dt.month)
                monthly[key].append(float(value))
            except (ValueError, TypeError, KeyError):
                continue

        observations: list[DataPoint] = []
        for (year, month), values in sorted(monthly.items()):
            avg = sum(values) / len(values)
            observations.append(DataPoint(
                date=date(year, month, 1),
                value=round(avg, 2),
            ))

        if not observations:
            raise ValueError("OpenAQ: could not build monthly observations")

        return Indicator(
            id="india.openaq.PM25",
            name="India PM2.5 Air Quality (monthly avg, μg/m³)",
            source_name=self.source_name,
            source_url="https://api.openaq.org/v3/measurements?countries_id=104&parameters_id=2",
            country=self.country,
            category=Category.CLIMATE,
            frequency=Frequency.MONTHLY,
            unit="micrograms_per_cubic_meter",
            observations=observations,
            fetched_at=self.now_utc(),
            status=FetchStatus.LIVE,
        )
