"""
tests/test_sources/test_schemas.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Unit tests for all Pydantic v2 schemas.
No HTTP calls — pure model validation tests.
"""

import pytest
from datetime import date, datetime, timezone
from pydantic import ValidationError

from ingestion.schemas import (
    Category, Country, DataPoint, Frequency, FetchStatus,
    Indicator, CountryDigest, GlobalDigest,
)

NOW = datetime.now(tz=timezone.utc)


# ─── DataPoint ────────────────────────────────────────────────────────────────

class TestDataPoint:
    def test_valid(self):
        dp = DataPoint(date=date(2024, 1, 1), value=3.7)
        assert dp.value == 3.7

    def test_coerce_string_value(self):
        dp = DataPoint(date=date(2024, 1, 1), value="3.7")
        assert dp.value == 3.7

    def test_fred_missing_sentinel(self):
        with pytest.raises(ValidationError):
            DataPoint(date=date(2024, 1, 1), value=".")

    def test_none_value_rejected(self):
        with pytest.raises(ValidationError):
            DataPoint(date=date(2024, 1, 1), value=None)

    def test_frozen(self):
        dp = DataPoint(date=date(2024, 1, 1), value=1.0)
        with pytest.raises(Exception):
            dp.value = 2.0


# ─── Indicator ────────────────────────────────────────────────────────────────

def make_indicator(**kwargs) -> Indicator:
    defaults = dict(
        id="usa.fred.UNRATE",
        name="US Unemployment Rate",
        source_name="FRED",
        source_url="https://fred.stlouisfed.org/series/UNRATE",
        country=Country.USA,
        category=Category.ECONOMIC,
        frequency=Frequency.MONTHLY,
        unit="percent",
        observations=[DataPoint(date=date(2024, m, 1), value=float(m)) for m in range(1, 5)],
        fetched_at=NOW,
    )
    defaults.update(kwargs)
    return Indicator(**defaults)


class TestIndicator:
    def test_valid(self):
        ind = make_indicator()
        assert ind.id == "usa.fred.UNRATE"

    def test_id_slug_validation(self):
        with pytest.raises(ValidationError):
            make_indicator(id="INVALID ID WITH SPACES")

    def test_observations_sorted(self):
        # Deliberately pass out-of-order observations
        obs = [
            DataPoint(date=date(2024, 3, 1), value=3.9),
            DataPoint(date=date(2024, 1, 1), value=3.7),
            DataPoint(date=date(2024, 2, 1), value=3.8),
        ]
        ind = make_indicator(observations=obs)
        dates = [o.date for o in ind.observations]
        assert dates == sorted(dates), "Observations must be sorted chronologically"

    def test_latest(self):
        ind = make_indicator()
        assert ind.latest.date == date(2024, 4, 1)

    def test_previous(self):
        ind = make_indicator()
        assert ind.previous.date == date(2024, 3, 1)

    def test_pct_change(self):
        obs = [
            DataPoint(date=date(2024, 1, 1), value=100.0),
            DataPoint(date=date(2024, 2, 1), value=110.0),
        ]
        ind = make_indicator(observations=obs)
        assert ind.pct_change == pytest.approx(10.0)

    def test_pct_change_single_obs(self):
        ind = make_indicator(observations=[DataPoint(date=date(2024, 1, 1), value=5.0)])
        assert ind.pct_change is None

    def test_pct_change_zero_denominator(self):
        obs = [
            DataPoint(date=date(2024, 1, 1), value=0.0),
            DataPoint(date=date(2024, 2, 1), value=5.0),
        ]
        ind = make_indicator(observations=obs)
        assert ind.pct_change is None

    def test_last_n_capped_at_12(self):
        obs = [DataPoint(date=date(2024, 1, 1), value=float(i)) for i in range(20)]
        ind = make_indicator(observations=obs)
        assert len(ind.last_n) == 12

    def test_empty_observations_rejected(self):
        with pytest.raises(ValidationError):
            make_indicator(observations=[])

    def test_stale_status_copy(self):
        ind = make_indicator()
        stale = ind.model_copy(update={"status": FetchStatus.STALE})
        assert stale.status == FetchStatus.STALE
        assert ind.status == FetchStatus.LIVE  # original unchanged

    def test_frozen(self):
        ind = make_indicator()
        with pytest.raises(Exception):
            ind.name = "changed"


# ─── CountryDigest ────────────────────────────────────────────────────────────

class TestCountryDigest:
    def test_valid(self):
        ind = make_indicator()
        cd = CountryDigest(
            country=Country.USA,
            run_id="2024-01-01T00:00:00",
            indicators=[ind],
        )
        assert len(cd.indicators) == 1

    def test_by_category(self):
        ind_eco = make_indicator(id="usa.fred.GDP", category=Category.ECONOMIC)
        ind_cli = make_indicator(id="usa.noaa.TEMP", category=Category.CLIMATE)
        cd = CountryDigest(
            country=Country.USA,
            run_id="2024-01-01T00:00:00",
            indicators=[ind_eco, ind_cli],
        )
        cats = cd.by_category
        assert Category.ECONOMIC in cats
        assert Category.CLIMATE in cats
        assert len(cats[Category.ECONOMIC]) == 1

    def test_empty_errors_default(self):
        cd = CountryDigest(
            country=Country.UK,
            run_id="2024-01-01T00:00:00",
            indicators=[make_indicator(country=Country.UK)],
        )
        assert cd.errors == []


# ─── GlobalDigest ─────────────────────────────────────────────────────────────

class TestGlobalDigest:
    def test_by_country(self):
        cd_usa = CountryDigest(country=Country.USA, run_id="r1", indicators=[make_indicator()])
        cd_uk = CountryDigest(
            country=Country.UK, run_id="r1",
            indicators=[make_indicator(
                id="uk.ons.CPIH", country=Country.UK,
                source_url="https://ons.gov.uk"
            )]
        )
        gd = GlobalDigest(
            run_id="r1",
            generated_at=NOW,
            digests=[cd_usa, cd_uk],
        )
        assert gd.get(Country.USA) is cd_usa
        assert gd.get(Country.UK) is cd_uk
        assert gd.get(Country.INDIA) is None
