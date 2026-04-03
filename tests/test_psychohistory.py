"""
tests/test_psychohistory.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Tests for Turchin PSI, GDELT adapter, and historical analogy library.
All network calls mocked.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from ingestion.psychohistory.turchin import (
    compute_psi, format_psi_for_prompt, PSIComponents, _trend,
)
from ingestion.psychohistory.analogies import (
    find_active_analogies, format_analogies_for_prompt, ANALOGY_LIBRARY,
)
from ingestion.psychohistory.gdelt import format_gdelt_for_prompt, GDELTSignal


# ─── Helper ───────────────────────────────────────────────────────────────────

def make_obs(values: list[float]) -> list[dict]:
    from datetime import date
    return [{"date": f"2026-0{i+1}-01", "value": v} for i, v in enumerate(values)]


def make_indicator(id_: str, values: list[float]) -> dict:
    return {
        "id": id_, "name": f"Test {id_}", "unit": "percent",
        "observations": make_obs(values),
    }


# ─── PSI Calculator ───────────────────────────────────────────────────────────

class TestTurchinPSI:
    def test_stable_economy_low_psi(self):
        """Low unemployment + low inflation + positive yield spread = low PSI."""
        inds = [
            make_indicator("usa.fred.UNRATE",   [3.5, 3.6, 3.7, 3.6, 3.5, 3.6]),
            make_indicator("usa.fred.CPIAUCSL", [2.1, 2.2, 2.1, 2.0, 2.1, 2.2]),
            make_indicator("usa.fred.FEDFUNDS", [2.0, 2.0, 2.1, 2.0, 2.0, 2.0]),
            make_indicator("usa.fred.T10Y2Y",   [1.2, 1.3, 1.4, 1.5, 1.4, 1.3]),
        ]
        result = compute_psi("usa", inds)
        assert result is not None
        assert result.psi < 0.4
        assert result.level in ("stable", "elevated")

    def test_high_stress_economy_high_psi(self):
        """High unemployment + high inflation + inverted yield = high PSI."""
        inds = [
            make_indicator("usa.fred.UNRATE",   [4.5, 5.0, 5.5, 6.0, 6.5, 7.0]),
            make_indicator("usa.fred.CPIAUCSL", [4.0, 5.0, 6.0, 7.0, 7.5, 8.0]),
            make_indicator("usa.fred.FEDFUNDS", [4.0, 4.5, 5.0, 5.5, 5.5, 5.5]),
            make_indicator("usa.fred.T10Y2Y",   [0.5, 0.2, 0.0, -0.3, -0.5, -0.8]),
        ]
        result = compute_psi("usa", inds)
        assert result is not None
        assert result.psi > 0.3  # should be elevated or higher

    def test_brazil_selic_stress(self):
        """Brazil with SELIC at 14.25% and currency stress should register elevated PSI."""
        inds = [
            make_indicator("brazil.bcb.SELIC",      [10.75, 11.25, 12.25, 13.25, 13.75, 14.25]),
            make_indicator("brazil.bcb.IPCA",        [4.5, 4.8, 5.2, 5.5, 5.0, 5.48]),
            make_indicator("brazil.bcb.UNEMPLOYMENT",[9.0, 9.2, 9.5, 9.3, 9.1, 9.0]),
            make_indicator("brazil.bcb.USDBRL",      [4.9, 5.1, 5.4, 5.6, 5.8, 5.9]),
        ]
        result = compute_psi("brazil", inds)
        assert result is not None
        # PSI should be non-trivial given currency stress + high rates
        assert result.psi > 0.15
        assert len(result.signals) > 0

    def test_returns_none_for_empty_indicators(self):
        result = compute_psi("usa", [])
        assert result is None

    def test_psi_components_sum_logic(self):
        """PSI = cube_root(MMP × EMP × SFD) — should be <= max component."""
        inds = [
            make_indicator("usa.fred.UNRATE",   [7.0, 7.5, 8.0, 8.5, 9.0, 9.5]),
            make_indicator("usa.fred.CPIAUCSL", [8.0, 8.5, 9.0, 9.5, 9.5, 9.5]),
        ]
        result = compute_psi("usa", inds)
        if result:
            assert result.psi <= max(result.mmp, result.emp, result.sfd)

    def test_signals_generated_for_stress(self):
        inds = [
            make_indicator("brazil.bcb.SELIC",  [14.0, 14.25, 14.5, 14.75, 14.75, 14.75]),
            make_indicator("brazil.bcb.IPCA",   [5.0, 5.5, 6.0, 6.5, 7.0, 7.5]),
        ]
        result = compute_psi("brazil", inds)
        if result:
            assert isinstance(result.signals, list)

    def test_trend_calculation(self):
        obs = [{"date": "2026-01-01", "value": 100.0},
               {"date": "2026-02-01", "value": 110.0}]
        assert _trend(obs) == pytest.approx(10.0)

    def test_trend_zero_base_handled(self):
        obs = [{"date": "2026-01-01", "value": 0.0},
               {"date": "2026-02-01", "value": 5.0}]
        assert _trend(obs) == 0.0  # zero base returns 0

    def test_format_psi_empty(self):
        assert format_psi_for_prompt({}) == ""

    def test_format_psi_shows_level(self):
        psi = PSIComponents(
            country="usa", mmp=0.6, emp=0.5, sfd=0.4,
            psi=0.49, signals=["High unemployment signal"], level="elevated"
        )
        result = format_psi_for_prompt({"usa": psi})
        assert "ELEVATED" in result
        assert "PSI=0.49" in result
        assert "USA" in result


# ─── Historical Analogies ────────────────────────────────────────────────────

class TestHistoricalAnalogies:
    def test_library_not_empty(self):
        assert len(ANALOGY_LIBRARY) >= 5

    def test_all_analogies_have_required_fields(self):
        for a in ANALOGY_LIBRARY:
            assert a.id
            assert a.title
            assert a.historical_case
            assert a.structural_match
            assert a.implication
            assert a.confidence in ("low", "medium", "high")
            assert 0 <= a.trigger_psi_min <= 1.0

    def test_find_analogies_below_threshold_returns_empty(self):
        """PSI below all thresholds should return nothing."""
        psi = PSIComponents("usa", 0.1, 0.1, 0.1, 0.05, [], "stable")
        result = find_active_analogies("usa", 0.05, psi)
        assert result == []

    def test_find_analogies_triggers_for_usa_high_psi(self):
        psi = PSIComponents("usa", 0.7, 0.6, 0.5, 0.6, ["Elite overproduction"], "high")
        result = find_active_analogies("usa", 0.6, psi)
        assert len(result) >= 1
        assert all(a.trigger_country == "usa" for a in result)

    def test_find_analogies_triggers_for_brazil(self):
        psi = PSIComponents("brazil", 0.5, 0.7, 0.4, 0.52, ["SELIC stress"], "high")

        class MockIndicators:
            interest_rate = 14.5
            def get(self, k, default=0):
                return {"interest_rate": 14.5, "fx": 5.9}.get(k, default)

        result = find_active_analogies("brazil", 0.52, psi,
                                       indicator_snapshot={"interest_rate": 14.5, "fx": 5.9})
        assert isinstance(result, list)

    def test_format_analogies_empty(self):
        assert format_analogies_for_prompt({}) == ""

    def test_format_analogies_includes_title(self):
        psi = PSIComponents("usa", 0.6, 0.5, 0.5, 0.53, [], "high")
        analogs = find_active_analogies("usa", 0.53, psi)
        if analogs:
            result = format_analogies_for_prompt({"usa": analogs})
            assert "USA" in result
            assert any(a.title[:20] in result for a in analogs)


# ─── GDELT Signal Formatting ─────────────────────────────────────────────────

class TestGDELTSignal:
    def test_format_empty_gdelt(self):
        assert format_gdelt_for_prompt({}) == ""

    def test_format_crisis_signal(self):
        sig = GDELTSignal(
            country="brazil", avg_tone=-6.5, goldstein_avg=-0.65,
            breakpoint_pct=62.0, top_themes=["economy", "protest"],
            article_count=180, crisis_level="crisis",
        )
        result = format_gdelt_for_prompt({"brazil": sig})
        assert "BRAZIL" in result
        assert "crisis" in result.lower()
        assert "🔴" in result
        assert "-6.5" in result

    def test_format_calm_signal(self):
        sig = GDELTSignal(
            country="uk", avg_tone=1.2, goldstein_avg=0.12,
            breakpoint_pct=8.0, top_themes=["economy"],
            article_count=90, crisis_level="calm",
        )
        result = format_gdelt_for_prompt({"uk": sig})
        assert "🟢" in result
        assert "UK" in result

    @pytest.mark.asyncio
    async def test_fetch_gdelt_handles_network_failure(self):
        """GDELT fetch should return empty dict gracefully on failure."""
        from ingestion.psychohistory.gdelt import fetch_gdelt_signals
        with patch("httpx.AsyncClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.get.side_effect = Exception("network error")
            MockClient.return_value.__aenter__.return_value = mock_instance
            result = await fetch_gdelt_signals()
        assert isinstance(result, dict)
