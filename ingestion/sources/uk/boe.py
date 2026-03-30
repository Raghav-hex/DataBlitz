"""
datablitz.ingestion.sources.uk.boe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Bank of England (BoE) Statistical Interactive Database adapter.

API: https://www.bankofengland.co.uk/boeapps/database/
No auth required. Returns XML or CSV.

We use the CSV download endpoint which is stable and well-known:
  https://www.bankofengland.co.uk/boeapps/database/fromshowcolumns.asp
  ?Travel=NIxRSxSUx&FromSeries=1&ToSeries=50&DAT=RNG&
   FD=1&FM=Jan&FY=2022&TD=1&TM=Jan&TY=2026&VFD=Y&html.x=66&html.y=26&
   C=<SERIES_CODE>&Filter=N

Series codes (BoE IADB):
  IUMABEDR  - BoE Base Rate (monthly)
  LPMVWYR   - M4 Money Supply YoY growth (monthly)
  XUMLBK67  - GBP/USD exchange rate (monthly avg)
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import date

import httpx

from ...base import BaseSource
from ...config import Settings
from ...schemas import Category, Country, DataPoint, Frequency, FetchStatus, Indicator

logger = logging.getLogger(__name__)

BOE_CSV_URL = "https://www.bankofengland.co.uk/boeapps/database/fromshowcolumns.asp"

SERIES = [
    {
        "id": "uk.boe.BASE_RATE",
        "code": "IUMABEDR",
        "name": "UK Bank of England Base Rate",
        "category": Category.ECONOMIC,
        "unit": "percent",
    },
    {
        "id": "uk.boe.GBPUSD",
        "code": "XUMLBK67",
        "name": "GBP/USD Exchange Rate (monthly avg)",
        "category": Category.ECONOMIC,
        "unit": "usd_per_gbp",
    },
]


class BoESource(BaseSource):
    country = Country.UK
    source_name = "Bank of England"

    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        super().__init__(client, settings)

    async def fetch_indicators(self) -> list[Indicator]:
        indicators: list[Indicator] = []
        for series in SERIES:
            try:
                ind = await self._fetch_series(series)
                indicators.append(ind)
            except Exception as exc:
                logger.warning("boe_series_failed", extra={"code": series["code"], "error": str(exc)})
        return indicators

    async def _fetch_series(self, series: dict) -> Indicator:
        params = {
            "Travel": "NIxRSxSUx",
            "FromSeries": "1",
            "ToSeries": "50",
            "DAT": "RNG",
            "FD": "1", "FM": "Jan", "FY": "2022",
            "TD": "1", "TM": "Dec", "TY": "2025",
            "VFD": "Y",
            "html.x": "66", "html.y": "26",
            "C": series["code"],
            "Filter": "N",
            "csv.x": "yes",  # Request CSV format
        }

        text = await self.get_text(BOE_CSV_URL, params=params)
        observations = self._parse_boe_csv(text)

        if not observations:
            raise ValueError(f"BoE: no observations for {series['code']}")

        return Indicator(
            id=series["id"],
            name=series["name"],
            source_name=self.source_name,
            source_url=f"https://www.bankofengland.co.uk/boeapps/database/Bank-Stats.asp",
            country=self.country,
            category=series["category"],
            frequency=Frequency.MONTHLY,
            unit=series["unit"],
            observations=observations[-24:],
            fetched_at=self.now_utc(),
            status=FetchStatus.LIVE,
        )

    @staticmethod
    def _parse_boe_csv(text: str) -> list[DataPoint]:
        """
        BoE CSV format:
          DATE,VALUE
          01 Jan 2022,0.25
          01 Feb 2022,0.50
        """
        month_map = {
            "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
            "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
            "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
        }
        points: list[DataPoint] = []
        reader = csv.reader(io.StringIO(text))
        next(reader, None)  # skip header
        for row in reader:
            if len(row) < 2:
                continue
            try:
                # "01 Jan 2024"
                parts = row[0].strip().split()
                if len(parts) != 3:
                    continue
                day, mon_str, year = int(parts[0]), parts[1], int(parts[2])
                month = month_map.get(mon_str)
                if not month:
                    continue
                points.append(DataPoint(
                    date=date(year, month, day),
                    value=row[1].strip(),
                ))
            except (ValueError, IndexError):
                continue
        return points
