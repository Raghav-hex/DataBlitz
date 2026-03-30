"""
datablitz.ingestion.sources.brazil.bcb
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
BCB (Banco Central do Brasil) SGS API adapter.

API: https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados
No auth required. Returns JSON. Stable, well-maintained.

Series codes (BCB SGS):
  432   - SELIC Target Rate (daily → monthly %)
  13522 - IPCA Inflation (monthly %, 12m accumulated)
  1     - USD/BRL Exchange Rate (daily → monthly avg)
  4192  - GDP Growth Rate YoY (quarterly, %)
  24363 - Unemployment Rate PNAD (monthly, %)
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

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados"

SERIES = [
    {
        "id": "brazil.bcb.SELIC",
        "series_id": 432,
        "name": "Brazil SELIC Target Rate",
        "category": Category.ECONOMIC,
        "frequency": Frequency.MONTHLY,
        "unit": "percent_per_year",
    },
    {
        "id": "brazil.bcb.IPCA",
        "series_id": 13522,
        "name": "Brazil IPCA Inflation (12m accumulated)",
        "category": Category.ECONOMIC,
        "frequency": Frequency.MONTHLY,
        "unit": "percent",
    },
    {
        "id": "brazil.bcb.USDBRL",
        "series_id": 1,
        "name": "USD/BRL Exchange Rate",
        "category": Category.ECONOMIC,
        "frequency": Frequency.DAILY,
        "unit": "brl_per_usd",
    },
    {
        "id": "brazil.bcb.UNEMPLOYMENT",
        "series_id": 24363,
        "name": "Brazil Unemployment Rate (PNAD)",
        "category": Category.SOCIAL,
        "frequency": Frequency.MONTHLY,
        "unit": "percent",
    },
]


class BCBSource(BaseSource):
    country = Country.Brazil if hasattr(Country, "Brazil") else Country.BRAZIL
    source_name = "BCB (Banco Central)"

    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        super().__init__(client, settings)
        # Date range: last 24 months
        end = datetime.now().date()
        start = end - relativedelta(months=24)
        self._date_range = (start, end)

    async def fetch_indicators(self) -> list[Indicator]:
        indicators: list[Indicator] = []
        for series in SERIES:
            try:
                ind = await self._fetch_series(series)
                indicators.append(ind)
            except Exception as exc:
                logger.warning("bcb_series_failed", extra={"id": series["id"], "error": str(exc)})
        return indicators

    async def _fetch_series(self, series: dict) -> Indicator:
        url = BASE_URL.format(series_id=series["series_id"])
        start, end = self._date_range
        params = {
            "formato": "json",
            "dataInicial": start.strftime("%d/%m/%Y"),
            "dataFinal": end.strftime("%d/%m/%Y"),
        }

        data = await self.get_json(url, params=params)

        if not data:
            raise ValueError(f"BCB returned empty data for series {series['series_id']}")

        # For daily series, resample to monthly average
        raw_points = self._parse_bcb(data)
        if series["frequency"] == Frequency.DAILY:
            observations = self._monthly_avg(raw_points)
            freq = Frequency.MONTHLY
        else:
            observations = raw_points
            freq = series["frequency"]

        if not observations:
            raise ValueError(f"BCB: no valid observations for {series['id']}")

        return Indicator(
            id=series["id"],
            name=series["name"],
            source_name=self.source_name,
            source_url=f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series['series_id']}/dados",
            country=Country.BRAZIL,
            category=series["category"],
            frequency=freq,
            unit=series["unit"],
            observations=observations,
            fetched_at=self.now_utc(),
            status=FetchStatus.LIVE,
        )

    @staticmethod
    def _parse_bcb(raw: list[dict]) -> list[DataPoint]:
        """BCB format: {"data": "01/01/2024", "valor": "11.75"}"""
        points: list[DataPoint] = []
        for item in raw:
            try:
                d = datetime.strptime(item["data"], "%d/%m/%Y").date()
                points.append(DataPoint(date=d, value=item["valor"]))
            except (ValueError, KeyError):
                continue
        return points

    @staticmethod
    def _monthly_avg(points: list[DataPoint]) -> list[DataPoint]:
        """Aggregate daily points to monthly averages."""
        from collections import defaultdict
        monthly: dict[tuple, list[float]] = defaultdict(list)
        for pt in points:
            key = (pt.date.year, pt.date.month)
            monthly[key].append(pt.value)
        result = []
        for (year, month), values in sorted(monthly.items()):
            avg = sum(values) / len(values)
            result.append(DataPoint(date=date(year, month, 1), value=round(avg, 4)))
        return result
