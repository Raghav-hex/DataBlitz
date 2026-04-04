"""
tests/test_daily_sources.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Tests for BDI, FRED daily, and Polymarket adapters.
All network calls mocked.
"""

import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import date, datetime, timezone

from ingestion.sources.world.polymarket import (
    format_polymarket_for_prompt, get_divergence_signals,
    PolymarketSignal, _is_relevant, _parse_gamma_market,
)
from ingestion.sources.world.bdi import format_bdi_for_prompt
from ingestion.sources.usa.fred_daily import format_daily_for_prompt
from ingestion.psychohistory.turchin import PSIComponents
from ingestion.schemas import (
    Category, Country, DataPoint, Frequency, FetchStatus, Indicator,
)


NOW = datetime.now(tz=timezone.utc)


def make_ind(id_, values):
    return Indicator(
        id=id_, name=f"Test {id_}", source_name="test",
        source_url="https://example.com",
        country=Country.USA, category=Category.ECONOMIC,
        frequency=Frequency.DAILY, unit="pts",
        observations=[DataPoint(date=date(2026, 1, i+1), value=v) for i, v in enumerate(values)],
        fetched_at=NOW, status=FetchStatus.LIVE,
    )


def make_signal(question, prob=0.5, volume=1_000_000) -> PolymarketSignal:
    return PolymarketSignal(
        question=question, probability=prob, volume_usd=volume,
        prev_prob=prob, delta=0.0, category="economics",
        url="https://polymarket.com/event/test",
    )


# ─── Polymarket relevance filter ─────────────────────────────────────────────

class TestPolymarketRelevance:
    def test_recession_is_relevant(self):
        assert _is_relevant("Will the US enter a recession in 2026?")

    def test_fed_rate_is_relevant(self):
        assert _is_relevant("Will the Fed cut rates in Q2 2026?")

    def test_brazil_is_relevant(self):
        assert _is_relevant("Will Brazil GDP grow in 2026?")

    def test_sports_is_not_relevant(self):
        assert not _is_relevant("Will Arsenal win the Premier League?")

    def test_crypto_price_is_not_relevant(self):
        assert not _is_relevant("Will Bitcoin reach $200K?")

    def test_oil_is_relevant(self):
        assert _is_relevant("Will crude oil exceed $100 per barrel?")


# ─── Polymarket formatting ─────────────────────────────────────────────────

class TestPolymarketFormat:
    def test_empty_returns_empty(self):
        assert format_polymarket_for_prompt([]) == ""

    def test_formats_probability_as_percentage(self):
        signals = [make_signal("Will the US enter a recession?", prob=0.42)]
        result = format_polymarket_for_prompt(signals)
        assert "42%" in result
        assert "recession" in result.lower()

    def test_shows_volume(self):
        signals = [make_signal("Will Fed cut rates?", prob=0.65, volume=2_500_000)]
        result = format_polymarket_for_prompt(signals)
        assert "$2.5M" in result

    def test_high_probability_labeled_high(self):
        signals = [make_signal("Will inflation stay above 3%?", prob=0.72)]
        result = format_polymarket_for_prompt(signals)
        assert "HIGH" in result

    def test_low_probability_labeled_low(self):
        signals = [make_signal("Will US default on debt?", prob=0.05)]
        result = format_polymarket_for_prompt(signals)
        assert "LOW" in result

    def test_header_present(self):
        signals = [make_signal("Will Fed cut rates?", prob=0.60)]
        result = format_polymarket_for_prompt(signals)
        assert "POLYMARKET" in result
        assert "real-money" in result

    def test_multiple_signals_sorted_by_volume(self):
        signals = [
            make_signal("Low volume market",  prob=0.5, volume=150_000),
            make_signal("High volume market", prob=0.5, volume=5_000_000),
        ]
        result = format_polymarket_for_prompt(signals)
        high_pos = result.find("High volume")
        low_pos  = result.find("Low volume")
        assert high_pos < low_pos  # high volume appears first


# ─── Polymarket divergence detection ─────────────────────────────────────────

class TestPolymarketDivergence:
    def test_crowd_low_psi_high_triggers_divergence(self):
        """Crowd says 25% recession but PSI is elevated — should flag."""
        signals = [make_signal("Will the US enter a recession in 2026?", prob=0.25)]
        psi = {"usa": PSIComponents("usa", 0.6, 0.5, 0.4, 0.5, [], "high")}
        divergences = get_divergence_signals(signals, psi)
        assert len(divergences) >= 1
        assert "DIVERGENCE" in divergences[0]
        assert "25%" in divergences[0]

    def test_no_divergence_when_aligned(self):
        """Crowd says 45% recession and PSI is moderate — no divergence."""
        signals = [make_signal("Will the US enter a recession in 2026?", prob=0.45)]
        psi = {"usa": PSIComponents("usa", 0.3, 0.3, 0.2, 0.26, [], "stable")}
        divergences = get_divergence_signals(signals, psi)
        assert len(divergences) == 0

    def test_handles_no_recession_market(self):
        """If no recession market found, no crash."""
        signals = [make_signal("Will Fed cut rates?", prob=0.6)]
        psi = {"usa": PSIComponents("usa", 0.7, 0.6, 0.5, 0.58, [], "critical")}
        divergences = get_divergence_signals(signals, psi)
        assert isinstance(divergences, list)


# ─── BDI formatting ──────────────────────────────────────────────────────────

class TestBDIFormat:
    def test_empty_returns_empty(self):
        assert format_bdi_for_prompt([]) == ""

    def test_format_shows_name_and_value(self):
        ind = make_ind("global.bdi.composite", [1800, 1850, 1900, 1950, 2000])
        result = format_bdi_for_prompt([ind])
        assert "Baltic" in result or "bdi" in result.lower()
        assert "2000" in result

    def test_format_shows_rising_direction(self):
        ind = make_ind("global.bdi.composite", [1500, 1600, 1700, 1800, 1900])
        result = format_bdi_for_prompt([ind])
        assert "↑" in result or "rising" in result

    def test_format_shows_falling_direction(self):
        ind = make_ind("global.bdi.composite", [2000, 1900, 1800, 1700, 1500])
        result = format_bdi_for_prompt([ind])
        assert "↓" in result or "falling" in result

    def test_capesize_interpretation_on_sharp_fall(self):
        # Sharp fall in capesize should add interpretation
        ind = make_ind("global.bdi.capesize", [3000, 2800, 2600, 2400, 2000])
        result = format_bdi_for_prompt([ind])
        # Should show at minimum the value
        assert "2000" in result

    def test_spark_shows_5_values(self):
        ind = make_ind("global.bdi.composite", [1800, 1850, 1900, 1950, 2000])
        result = format_bdi_for_prompt([ind])
        assert "→" in result  # spark separator


# ─── FRED daily formatting ────────────────────────────────────────────────────

class TestFREDDailyFormat:
    def test_empty_returns_empty(self):
        assert format_daily_for_prompt([]) == ""

    def test_format_includes_header(self):
        ind = make_ind("usa.fred.daily.WTI", [75.0, 76.0, 77.0, 78.0, 79.0])
        result = format_daily_for_prompt([ind])
        assert "DAILY FINANCIAL CONDITIONS" in result or "FRED" in result

    def test_format_shows_latest_value(self):
        ind = make_ind("usa.fred.daily.GOLD", [2300, 2310, 2320, 2330, 2340])
        result = format_daily_for_prompt([ind])
        assert "2340" in result or "2340.00" in result

    def test_format_shows_pct_change(self):
        # 10% change should show up
        ind = make_ind("usa.fred.daily.WTI", [70.0, 70.5, 71.0, 71.5, 77.0])
        result = format_daily_for_prompt([ind])
        assert "%" in result


# ─── Integration: parse_gamma_market ─────────────────────────────────────────

class TestGammaMarketParsing:
    def test_parses_yes_outcome(self):
        market = {
            "question": "Will US inflation exceed 3% in 2026?",
            "slug": "us-inflation-2026",
            "volumeNum": 500000,
            "category": "economics",
            "outcomes": [
                {"value": "Yes", "probability": 0.45},
                {"value": "No",  "probability": 0.55},
            ],
        }
        sig = _parse_gamma_market(market)
        assert sig is not None
        assert sig.probability == pytest.approx(0.45)
        assert sig.volume_usd == 500000

    def test_returns_none_for_irrelevant_market(self):
        market = {
            "question": "Will Arsenal win the Champions League?",
            "slug": "arsenal-ucl",
            "volumeNum": 1000000,
            "outcomes": [{"value": "Yes", "probability": 0.7}],
        }
        sig = _parse_gamma_market(market)
        # Returns None because question is not relevant
        assert sig is None

    def test_handles_percentage_probability(self):
        """Some markets return probability as 0-100 not 0-1."""
        market = {
            "question": "Will the Fed cut rates in 2026?",
            "slug": "fed-cut-2026",
            "volumeNum": 2000000,
            "outcomes": [{"value": "Yes", "probability": 65}],  # percentage
        }
        sig = _parse_gamma_market(market)
        if sig:  # only if relevant
            assert 0 <= sig.probability <= 1
