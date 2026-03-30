"""
datablitz.ingestion.sources.brazil.ibge
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
IBGE SIDRA API adapter.

API: https://apisidra.ibge.gov.br/values/t/{table}/...
No auth required. Free JSON. Well maintained by Brazil's stats bureau.

Tables used:
  1737  - IPCA monthly index (sanity cross-check vs BCB)
  6381  - PNAD Contínua unemployment (quarterly)
  5932  - Gross Domestic Product quarterly (BRL billions)
"""

from __future__ import annotations

import logging
from datetime import date

import httpx

from ...base import BaseSource
from ...config import Settings
from ...schemas import Category, Country, DataPoint, Frequency, FetchStatus, Indicator

logger = logging.getLogger(__name__)

BASE = "https://apisidra.ibge.gov.br/values"

TABLES = [
    {
        "id": "brazil.ibge.GDP",
        "table": "5932",
        "variable": "6564",   # GDP volume index
        "period": "last 12",
        "classification": "/C11255/90707",  # GDP at market prices
        "name": "Brazil GDP Volume Index (quarterly)",
        "category": Category.ECONOMIC,
        "frequency": Frequency.QUARTERLY,
        "unit": "index_1995=100",
    },
    {
        "id": "brazil.ibge.UNEMPLOYMENT",
        "table": "6381",
        "variable": "4099",   # unemployment rate
        "period": "last 12",
        "classification": "/C829/46304",    # total Brazil
        "name": "Brazil Unemployment Rate (PNAD Contínua)",
        "category": Category.SOCIAL,
        "frequency": Frequency.QUARTERLY,
        "unit": "percent",
    },
]


class IBGESource(BaseSource):
    country = Country.BRAZIL
    source_name = "IBGE SIDRA"

    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        super().__init__(client, settings)

    async def fetch_indicators(self) -> list[Indicator]:
        indicators: list[Indicator] = []
        for table in TABLES:
            try:
                ind = await self._fetch_table(table)
                indicators.append(ind)
            except Exception as exc:
                logger.warning("ibge_table_failed", extra={"id": table["id"], "error": str(exc)})
        return indicators

    async def _fetch_table(self, table: dict) -> Indicator:
        """
        SIDRA URL pattern:
        /values/t/{table}/n1/all/v/{variable}/p/{period}{classification}
        n1/all = national level
        """
        url = (
            f"{BASE}/t/{table['table']}/n1/all"
            f"/v/{table['variable']}"
            f"/p/{table['period'].replace(' ', '%20')}"
            f"{table['classification']}"
        )

        data = await self.get_json(url)

        # SIDRA returns a list; first element is the header row
        if not data or len(data) < 2:
            raise ValueError(f"IBGE SIDRA returned no data for table {table['table']}")

        observations = self._parse_sidra(data[1:])  # skip header row

        if not observations:
            raise ValueError(f"IBGE: no valid observations for {table['id']}")

        return Indicator(
            id=table["id"],
            name=table["name"],
            source_name=self.source_name,
            source_url=f"https://sidra.ibge.gov.br/tabela/{table['table']}",
            country=self.country,
            category=table["category"],
            frequency=table["frequency"],
            unit=table["unit"],
            observations=observations,
            fetched_at=self.now_utc(),
            status=FetchStatus.LIVE,
        )

    @staticmethod
    def _parse_sidra(rows: list[dict]) -> list[DataPoint]:
        """
        SIDRA row example: {"D3C": "2024", "D3N": "4th quarter 2024", "V": "98.3"}
        Period strings: "1st quarter 2024", "2nd quarter 2024", etc.
        """
        quarter_map = {
            "1st quarter": 1, "2nd quarter": 4,
            "3rd quarter": 7, "4th quarter": 10,
        }
        points: list[DataPoint] = []
        for row in rows:
            try:
                period_str = row.get("D3N", "").lower().strip()
                value = row.get("V", "")
                if value in ("", "-", "...", None):
                    continue

                # Try "Nth quarter YYYY"
                for q_str, month in quarter_map.items():
                    if q_str in period_str:
                        year_part = period_str.replace(q_str, "").strip()
                        year = int(year_part)
                        points.append(DataPoint(date=date(year, month, 1), value=value))
                        break

            except (ValueError, KeyError, TypeError):
                continue

        return points
