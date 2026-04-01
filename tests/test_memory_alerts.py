"""
tests/test_memory_alerts.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Tests for the memory/RAG layer and threshold alert system.
"""

import json
import pytest
from datetime import date, datetime, timezone

from ingestion.schemas import (
    Category, Country, DataPoint, Frequency, FetchStatus,
    Indicator, CountryDigest, GlobalDigest,
)
from ingestion.alerts import check_alerts, format_alerts_for_prompt, Alert
from ingestion.memory import (
    MemoryLayer, _similarity_score,
    format_historical_context, format_week_over_week,
)


NOW = datetime.now(tz=timezone.utc)


# ─── Alert tests ──────────────────────────────────────────────────────────────

def make_digest_dict(indicators: dict) -> dict:
    """Build minimal digest dict for alert testing."""
    return {
        "digests": [{
            "country": "usa",
            "indicators": [
                {
                    "id": ind_id,
                    "name": f"Test {ind_id}",
                    "observations": [{"date": "2026-01-01", "value": val}],
                }
                for ind_id, val in indicators.items()
            ],
            "errors": [],
        }]
    }


class TestAlerts:
    def test_no_alerts_below_threshold(self):
        digest = make_digest_dict({"usa.fred.UNRATE": 3.5})
        alerts = check_alerts(digest)
        assert all(a.indicator_id != "usa.fred.UNRATE" for a in alerts)

    def test_warning_fires_above_threshold(self):
        digest = make_digest_dict({"usa.fred.UNRATE": 5.5})
        alerts = check_alerts(digest)
        fired = [a for a in alerts if a.indicator_id == "usa.fred.UNRATE"]
        assert len(fired) >= 1
        assert any(a.level == "warning" for a in fired)

    def test_critical_fires_at_higher_threshold(self):
        digest = make_digest_dict({"usa.fred.UNRATE": 6.5})
        alerts = check_alerts(digest)
        fired = [a for a in alerts if a.indicator_id == "usa.fred.UNRATE"]
        assert any(a.level == "critical" for a in fired)

    def test_brazil_selic_critical(self):
        digest = make_digest_dict({"brazil.bcb.SELIC": 14.5})
        alerts = check_alerts(digest)
        fired = [a for a in alerts if a.indicator_id == "brazil.bcb.SELIC"]
        assert any(a.level == "critical" for a in fired)

    def test_india_pm25_severe(self):
        digest = make_digest_dict({"india.openaq.PM25": 160.0})
        alerts = check_alerts(digest)
        fired = [a for a in alerts if a.indicator_id == "india.openaq.PM25"]
        assert any(a.level == "critical" for a in fired)

    def test_yield_curve_inversion(self):
        digest = make_digest_dict({"usa.fred.T10Y2Y": -0.2})
        alerts = check_alerts(digest)
        fired = [a for a in alerts if a.indicator_id == "usa.fred.T10Y2Y"]
        assert len(fired) == 1
        assert fired[0].level == "critical"

    def test_format_empty_alerts(self):
        assert format_alerts_for_prompt([]) == ""

    def test_format_sorts_critical_first(self):
        alerts = [
            Alert("x.y.Z", "info msg",     "info",     1.0, 0.5),
            Alert("x.y.A", "critical msg", "critical", 2.0, 1.5),
            Alert("x.y.B", "warning msg",  "warning",  1.5, 1.0),
        ]
        result = format_alerts_for_prompt(alerts)
        assert result.index("CRITICAL") < result.index("WARNING")
        assert result.index("WARNING") < result.index("INFO")

    def test_format_includes_all_info(self):
        alerts = [Alert("usa.fred.UNRATE", "Unemployment exceeded 6%", "critical", 6.5, 6.0)]
        result = format_alerts_for_prompt(alerts)
        assert "CRITICAL" in result
        assert "usa.fred.UNRATE" in result
        assert "6.5" in result


# ─── Memory layer tests ───────────────────────────────────────────────────────

@pytest.fixture
async def tmp_memory(tmp_path):
    return MemoryLayer(db_path=tmp_path / "test_memory.sqlite")


class TestSimilarityScore:
    def test_identical_snapshots_score_one(self):
        snap = {
            "ind1": {"value": 5.0, "direction": "rising"},
            "ind2": {"value": 3.0, "direction": "flat"},
        }
        assert _similarity_score(snap, snap) == 1.0

    def test_empty_snapshots_score_zero(self):
        assert _similarity_score({}, {}) == 0.0
        assert _similarity_score({"a": {"value": 1.0}}, {}) == 0.0

    def test_different_directions_reduce_score(self):
        current = {"ind1": {"value": 5.0, "direction": "rising"}}
        stored  = {"ind1": {"value": 5.0, "direction": "falling"}}
        score = _similarity_score(current, stored)
        assert 0 < score < 1.0

    def test_value_within_20pct_scores_higher(self):
        current  = {"ind1": {"value": 10.0, "direction": "rising"}}
        close    = {"ind1": {"value": 10.5, "direction": "rising"}}  # 5% diff
        far      = {"ind1": {"value": 15.0, "direction": "rising"}}  # 50% diff
        assert _similarity_score(current, close) > _similarity_score(current, far)

    def test_no_shared_keys_score_zero(self):
        assert _similarity_score(
            {"usa.fred.UNRATE": {"value": 4.0, "direction": "flat"}},
            {"brazil.bcb.SELIC": {"value": 14.0, "direction": "rising"}},
        ) == 0.0


class TestMemoryLayer:
    @pytest.mark.asyncio
    async def test_store_and_retrieve(self, tmp_memory):
        await tmp_memory.store_week(
            week_id="2026-W01", country="usa", run_id="test",
            indicators=[{
                "id": "usa.fred.UNRATE", "name": "US Unemployment",
                "observations": [{"date": "2026-01-01", "value": 4.1}],
                "unit": "percent", "category": "economic",
            }],
            country_brief="US unemployment at 4.1% in January",
            headline="US labour market softens",
        )
        last = await tmp_memory.get_last_week("usa")
        assert last is not None
        assert last["week_id"] == "2026-W01"
        assert "usa.fred.UNRATE" in last["snapshots"]
        assert last["snapshots"]["usa.fred.UNRATE"]["value"] == pytest.approx(4.1)

    @pytest.mark.asyncio
    async def test_find_similar_weeks(self, tmp_memory):
        # Store two past weeks
        for week, selic_val in [("2026-W01", 12.25), ("2026-W02", 13.25)]:
            await tmp_memory.store_week(
                week_id=week, country="brazil", run_id="test",
                indicators=[{
                    "id": "brazil.bcb.SELIC", "name": "Brazil SELIC",
                    "observations": [
                        {"date": "2025-12-01", "value": selic_val - 0.5},
                        {"date": "2026-01-01", "value": selic_val},
                    ],
                    "unit": "percent", "category": "economic",
                }],
                country_brief=f"SELIC at {selic_val}%",
            )

        # Query with current snapshot similar to W02
        similar = await tmp_memory.find_similar_weeks(
            country="brazil",
            current_snapshots={"brazil.bcb.SELIC": {"value": 13.0, "direction": "rising"}},
            n=2, exclude_week="2026-W03",
        )
        assert len(similar) >= 1
        # W02 (13.25) is closer to 13.0 than W01 (12.25)
        assert similar[0]["week_id"] == "2026-W02"

    @pytest.mark.asyncio
    async def test_weeks_stored_count(self, tmp_memory):
        assert await tmp_memory.weeks_stored("uk") == 0
        await tmp_memory.store_week(
            "2026-W01", "uk", "test",
            [{"id": "uk.ons.CPIH", "name": "UK CPIH", "observations": [{"date": "2026-01-01", "value": 3.0}], "unit": "percent", "category": "economic"}],
        )
        assert await tmp_memory.weeks_stored("uk") == 1

    @pytest.mark.asyncio
    async def test_upsert_replaces_existing(self, tmp_memory):
        base = {"id": "usa.fred.UNRATE", "name": "UNRATE",
                "observations": [{"date": "2026-01-01", "value": 4.0}],
                "unit": "percent", "category": "economic"}
        await tmp_memory.store_week("2026-W01", "usa", "r1", [base])
        base["observations"] = [{"date": "2026-01-01", "value": 4.2}]
        await tmp_memory.store_week("2026-W01", "usa", "r2", [base])
        last = await tmp_memory.get_last_week("usa")
        assert last["snapshots"]["usa.fred.UNRATE"]["value"] == pytest.approx(4.2)


# ─── Format helpers ──────────────────────────────────────────────────────────

class TestFormatHelpers:
    def test_format_historical_empty(self):
        assert format_historical_context({}) == ""

    def test_format_historical_contains_week_id(self):
        ctx = {
            "usa": [{"week_id": "2026-W01", "score": 0.8,
                     "summary": "Fed holds rates at 4.33%",
                     "headline": "US labour market softens", "snapshots": {}}]
        }
        result = format_historical_context(ctx)
        assert "2026-W01" in result
        assert "80%" in result
        assert "USA" in result

    def test_format_wow_empty(self):
        assert format_week_over_week({}) == ""

    def test_format_wow_shows_arrows(self):
        deltas = {
            "brazil": [{"name": "SELIC", "prev": 13.25, "current": 14.25,
                        "pct": 7.55, "direction": "up"}]
        }
        result = format_week_over_week(deltas)
        assert "▲" in result
        assert "SELIC" in result
        assert "13.25" in result
        assert "14.25" in result
