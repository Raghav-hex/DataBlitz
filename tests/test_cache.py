"""
tests/test_cache.py
~~~~~~~~~~~~~~~~~~~~
Tests for the async SQLite stale-cache layer.
Uses a temporary in-memory-like SQLite path per test.
"""

import pytest
import tempfile
import os
from datetime import date, datetime, timezone, timedelta

from ingestion.cache import CacheLayer
from ingestion.schemas import (
    Country, Category, Frequency, FetchStatus,
    DataPoint, Indicator,
)


def make_indicator(id="usa.fred.UNRATE", status=FetchStatus.LIVE) -> Indicator:
    return Indicator(
        id=id,
        name="Test Indicator",
        source_name="TEST",
        source_url="https://example.com",
        country=Country.USA,
        category=Category.ECONOMIC,
        frequency=Frequency.MONTHLY,
        unit="percent",
        observations=[DataPoint(date=date(2024, 1, 1), value=3.7)],
        fetched_at=datetime.now(tz=timezone.utc),
        status=status,
    )


@pytest.fixture
def tmp_cache(tmp_path):
    db_path = tmp_path / "test_cache.sqlite"
    return CacheLayer(db_path=str(db_path), ttl_hours=48)


class TestCacheLayer:
    @pytest.mark.asyncio
    async def test_save_and_load(self, tmp_cache):
        ind = make_indicator()
        await tmp_cache.save(ind)

        result = await tmp_cache.load("usa.fred.UNRATE")
        assert result is not None
        assert result.id == "usa.fred.UNRATE"
        assert result.status == FetchStatus.STALE  # tagged stale on load

    @pytest.mark.asyncio
    async def test_miss_returns_none(self, tmp_cache):
        result = await tmp_cache.load("does.not.exist")
        assert result is None

    @pytest.mark.asyncio
    async def test_expired_entry_returns_none(self, tmp_cache):
        """Cache entries older than TTL should not be served."""
        # Use TTL of 0 hours to force immediate expiry
        zero_ttl_cache = CacheLayer(
            db_path=tmp_cache.db_path,
            ttl_hours=0,
        )
        ind = make_indicator()
        await zero_ttl_cache.save(ind)

        # Even immediately loading should be expired (TTL=0)
        result = await zero_ttl_cache.load("usa.fred.UNRATE")
        assert result is None

    @pytest.mark.asyncio
    async def test_overwrite_updates_entry(self, tmp_cache):
        ind1 = make_indicator()
        await tmp_cache.save(ind1)

        ind2 = make_indicator()
        # Different value
        ind2_new = ind1.model_copy(update={
            "observations": [DataPoint(date=date(2024, 2, 1), value=99.9)]
        })
        await tmp_cache.save(ind2_new)

        result = await tmp_cache.load("usa.fred.UNRATE")
        assert result.observations[0].value == pytest.approx(99.9)

    @pytest.mark.asyncio
    async def test_save_many_and_load_many(self, tmp_cache):
        ids = ["usa.fred.UNRATE", "usa.fred.GDP", "uk.ons.CPIH"]
        indicators = [make_indicator(id=id_) for id_ in ids]
        await tmp_cache.save_many(indicators)

        results = await tmp_cache.load_many(ids)
        assert len(results) == 3
        for id_ in ids:
            assert results[id_] is not None
            assert results[id_].status == FetchStatus.STALE

    @pytest.mark.asyncio
    async def test_load_many_missing_returns_none_for_missing(self, tmp_cache):
        await tmp_cache.save(make_indicator(id="usa.fred.UNRATE"))
        results = await tmp_cache.load_many(["usa.fred.UNRATE", "does.not.exist"])
        assert results["usa.fred.UNRATE"] is not None
        assert results["does.not.exist"] is None
