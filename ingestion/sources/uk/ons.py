"""
datablitz.ingestion.sources.uk.ons
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
ONS (Office for National Statistics) Beta API adapter.

IMPORTANT: ONS v0 API was RETIRED November 2024.
Must use: https://api.beta.ons.gov.uk/v1/
No authentication required.
Rate limit: 120 req/10s, 200 req/min.

Dataset IDs confirmed active (2025):
  - cpih01         → CPIH Inflation (monthly)
  - employmentuk   → UK Employment Rate (monthly, %)
  - gdp            → UK GDP index (quarterly)
  - unemp-16plus   → UK Unemployment Rate (monthly, %)
"""

from __future__ import annotations

import logging
from datetime import date
import re

import httpx

from ...base import BaseSource
from ...config import Settings
from ...schemas import Category, Country, DataPoint, Frequency, FetchStatus, Indicator

logger = logging.getLogger(__name__)

BASE = "https://api.beta.ons.gov.uk/v1"

DATASETS = [
    {
        "id": "uk.ons.CPIH",
        "dataset": "cpih01",
        "edition": "time-series",
        "name": "UK CPIH Inflation",
        "category": Category.ECONOMIC,
        "frequency": Frequency.MONTHLY,
        "unit": "percent_change_12m",
    },
    {
        "id": "uk.ons.UNEMPLOYMENT",
        "dataset": "unemp-16plus",
        "edition": "time-series",
        "name": "UK Unemployment Rate (16+)",
        "category": Category.ECONOMIC,
        "frequency": Frequency.MONTHLY,
        "unit": "percent",
    },
]


class ONSSource(BaseSource):
    country = Country.UK
    source_name = "ONS Beta API"

    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        super().__init__(client, settings)

    async def fetch_indicators(self) -> list[Indicator]:
        indicators: list[Indicator] = []

        for ds in DATASETS:
            try:
                ind = await self._fetch_dataset(ds)
                indicators.append(ind)
            except Exception as exc:
                logger.warning("ons_dataset_failed", extra={"id": ds["id"], "error": str(exc)})

        return indicators

    async def _fetch_dataset(self, ds: dict) -> Indicator:
        """
        ONS Beta API dataset observations endpoint.
        Pattern: /v1/datasets/{id}/editions/{edition}/versions/{latest}/observations
        We first hit /versions to find the latest version number.
        """
        # Get latest version number
        versions_url = f"{BASE}/datasets/{ds['dataset']}/editions/{ds['edition']}/versions"
        versions_data = await self.get_json(versions_url)

        items = versions_data.get("items", [])
        if not items:
            raise ValueError(f"ONS: no versions found for {ds['dataset']}")

        # Sort by version number descending, take latest
        latest_ver = max(items, key=lambda v: v.get("version", 0))
        ver_num = latest_ver["version"]

        # Fetch observations
        obs_url = (
            f"{BASE}/datasets/{ds['dataset']}/editions/{ds['edition']}"
            f"/versions/{ver_num}/observations"
        )
        # ONS paginates — fetch up to 300 entries (well covers 24 months monthly)
        params = {"time": "*", "geography": "K02000001", "aggregate": "*"}

        obs_data = await self.get_json(obs_url, params=params)

        observations = self._parse_ons_observations(obs_data)

        if not observations:
            raise ValueError(f"ONS: no observations parsed for {ds['id']}")

        return Indicator(
            id=ds["id"],
            name=ds["name"],
            source_name=self.source_name,
            source_url=f"https://www.ons.gov.uk/datasets/{ds['dataset']}",
            country=self.country,
            category=ds["category"],
            frequency=ds["frequency"],
            unit=ds["unit"],
            observations=observations[-24:],  # keep last 24
            fetched_at=self.now_utc(),
            status=FetchStatus.LIVE,
        )

    @staticmethod
    def _parse_ons_observations(data: dict) -> list[DataPoint]:
        """
        ONS Beta API returns observations in a nested structure.
        Time dimensions come as strings like "2024 JAN", "2024 Q1".
        """
        points: list[DataPoint] = []
        month_map = {
            "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
            "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
            "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
        }
        quarter_map = {"Q1": 1, "Q2": 4, "Q3": 7, "Q4": 10}

        for obs in data.get("observations", []):
            try:
                time_str = obs.get("dimensions", {}).get("time", {}).get("id", "")
                value = obs.get("observation")
                if value is None or value == "":
                    continue

                # "2024 JAN" format
                m = re.match(r"(\d{4})\s+([A-Z]{3})$", time_str)
                if m:
                    yr, mon = int(m.group(1)), month_map[m.group(2)]
                    points.append(DataPoint(date=date(yr, mon, 1), value=value))
                    continue

                # "2024 Q1" format
                m2 = re.match(r"(\d{4})\s+(Q[1-4])$", time_str)
                if m2:
                    yr, mon = int(m2.group(1)), quarter_map[m2.group(2)]
                    points.append(DataPoint(date=date(yr, mon, 1), value=value))

            except (ValueError, KeyError, TypeError):
                continue

        return points
