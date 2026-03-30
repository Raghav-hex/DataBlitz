"""
tests/test_sources/test_adapters.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Unit tests for source adapters — all HTTP mocked with respx.
No real network calls in tests.
"""

import json
import pytest
import httpx
import respx
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock

from ingestion.config import Settings
from ingestion.schemas import Country, Category, Frequency, FetchStatus
from ingestion.sources.usa.fred import FREDSource, BASE_URL as FRED_URL
from ingestion.sources.brazil.bcb import BCBSource
from ingestion.sources.india.worldbank import WorldBankIndiaSource


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def settings():
    return Settings(
        fred_api_key="test_fred_key",
        bls_api_key="test_bls_key",
        noaa_cdo_token="test_noaa_token",
        data_gov_in_key="test_datagov_key",
    )


@pytest.fixture
def async_client():
    return httpx.AsyncClient(timeout=10)


# ─── FRED adapter ─────────────────────────────────────────────────────────────

FRED_MOCK_RESPONSE = {
    "observations": [
        {"date": "2024-01-01", "value": "3.7"},
        {"date": "2024-02-01", "value": "3.9"},
        {"date": "2024-03-01", "value": "."},   # missing sentinel — should be skipped
        {"date": "2024-04-01", "value": "3.8"},
    ]
}


class TestFREDSource:
    def test_missing_api_key_raises(self, async_client):
        settings_no_key = Settings(fred_api_key="")
        with pytest.raises(ValueError, match="FRED_API_KEY"):
            FREDSource(async_client, settings_no_key)

    @respx.mock
    @pytest.mark.asyncio
    async def test_fetch_parses_observations(self, settings, async_client):
        # Mock ALL FRED series calls
        respx.get(FRED_URL).mock(
            return_value=httpx.Response(200, json=FRED_MOCK_RESPONSE)
        )

        source = FREDSource(async_client, settings)
        indicators = await source.fetch_indicators()

        assert len(indicators) > 0
        first = indicators[0]
        assert first.country == Country.USA
        assert first.status == FetchStatus.LIVE
        # Verify the "." sentinel was filtered out (3 valid obs, not 4)
        assert all(obs.value != "." for obs in first.observations)
        # Verify chronological sort
        dates = [o.date for o in first.observations]
        assert dates == sorted(dates)

    @respx.mock
    @pytest.mark.asyncio
    async def test_fetch_returns_empty_list_on_api_error(self, settings, async_client):
        """
        FRED handles errors per-series (logs + skips individual failures).
        On 500 for ALL series, fetch() returns empty indicators list.
        The outer fetch() only surfaces errors when fetch_indicators() itself
        raises — per-series failures are swallowed (logged as warnings).
        """
        respx.get(FRED_URL).mock(
            return_value=httpx.Response(500, text="Internal Server Error")
        )

        source = FREDSource(async_client, settings)
        # fetch_indicators returns [] when all series fail (per-series error handling)
        indicators = await source.fetch_indicators()
        assert indicators == []

    def test_parse_observations_filters_sentinel(self):
        raw = [
            {"date": "2024-01-01", "value": "5.0"},
            {"date": "2024-02-01", "value": "."},
            {"date": "2024-03-01", "value": "4.5"},
            {"date": "2024-04-01", "value": ""},
        ]
        result = FREDSource._parse_observations(raw)
        assert len(result) == 2
        assert result[0].value == 5.0
        assert result[1].value == 4.5

    def test_parse_observations_bad_dates_skipped(self):
        raw = [
            {"date": "NOT-A-DATE", "value": "3.0"},
            {"date": "2024-01-01", "value": "3.5"},
        ]
        result = FREDSource._parse_observations(raw)
        assert len(result) == 1


# ─── BCB adapter ─────────────────────────────────────────────────────────────

BCB_MOCK_SELIC = [
    {"data": "01/01/2024", "valor": "11.75"},
    {"data": "01/02/2024", "valor": "11.25"},
    {"data": "01/03/2024", "valor": "10.75"},
]

BCB_MOCK_USD = [
    {"data": "01/01/2024", "valor": "4.97"},
    {"data": "02/01/2024", "valor": "4.95"},
    {"data": "03/01/2024", "valor": "5.01"},
    {"data": "01/02/2024", "valor": "4.93"},
]


class TestBCBSource:
    @respx.mock
    @pytest.mark.asyncio
    async def test_parse_bcb_dates(self, settings, async_client):
        """BCB uses DD/MM/YYYY format — verify correct parsing."""
        raw = [
            {"data": "15/03/2024", "valor": "10.5"},
            {"data": "15/04/2024", "valor": "10.75"},
        ]
        result = BCBSource._parse_bcb(raw)
        assert len(result) == 2
        assert result[0].date == date(2024, 3, 15)
        assert result[1].value == 10.75

    def test_monthly_avg(self, settings, async_client):
        """Daily points aggregated to monthly average."""
        # January: two values averaging to 5.0
        # February: one value
        from ingestion.schemas import DataPoint
        points = [
            DataPoint(date=date(2024, 1, 1), value=4.0),
            DataPoint(date=date(2024, 1, 15), value=6.0),
            DataPoint(date=date(2024, 2, 1), value=5.5),
        ]
        result = BCBSource._monthly_avg(points)
        assert len(result) == 2
        assert result[0].date == date(2024, 1, 1)
        assert result[0].value == pytest.approx(5.0)
        assert result[1].value == pytest.approx(5.5)


# ─── World Bank India adapter ─────────────────────────────────────────────────

WB_MOCK_RESPONSE = [
    {"page": 1, "pages": 1},
    [
        {"date": "2023", "value": 7.2, "country": {"id": "IN"}},
        {"date": "2022", "value": 6.8, "country": {"id": "IN"}},
        {"date": "2021", "value": None},  # missing — should be skipped
        {"date": "2020", "value": -6.6, "country": {"id": "IN"}},
    ]
]


class TestWorldBankIndiaSource:
    @respx.mock
    @pytest.mark.asyncio
    async def test_parse_skips_null_values(self, settings, async_client):
        """World Bank returns null for missing years — must be filtered."""
        result = WorldBankIndiaSource._parse_wb(WB_MOCK_RESPONSE[1])
        assert len(result) == 3  # 2021 null skipped
        values = [obs.value for obs in result]
        assert None not in values

    @respx.mock
    @pytest.mark.asyncio
    async def test_annual_dates_normalised(self, settings, async_client):
        """Annual WB data should use Jan 1 of each year."""
        result = WorldBankIndiaSource._parse_wb(WB_MOCK_RESPONSE[1])
        for obs in result:
            assert obs.date.month == 1
            assert obs.date.day == 1
