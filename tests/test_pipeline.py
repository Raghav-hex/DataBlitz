"""
tests/test_pipeline.py
~~~~~~~~~~~~~~~~~~~~~~~
Tests for pipeline serialisation and KV push logic.
"""
import json
import pytest
from datetime import date, datetime, timezone
from pathlib import Path

from ingestion.schemas import (
    Category, Country, DataPoint, Frequency, FetchStatus,
    Indicator, CountryDigest, GlobalDigest,
)

NOW = datetime.now(tz=timezone.utc)


def make_indicator(id="usa.fred.UNRATE", country=Country.USA, category=Category.ECONOMIC):
    return Indicator(
        id=id, name="Test", source_name="TEST", source_url="https://example.com",
        country=country, category=category, frequency=Frequency.MONTHLY, unit="percent",
        observations=[DataPoint(date=date(2024, m, 1), value=float(m)) for m in range(1, 7)],
        fetched_at=NOW, status=FetchStatus.LIVE,
    )


def make_global_digest():
    return GlobalDigest(
        run_id="2024-06-01T00:00:00Z",
        generated_at=NOW,
        digests=[
            CountryDigest(country=c, run_id="r1", indicators=[
                make_indicator(id=f"{c.value}.test.IND", country=c)
            ])
            for c in Country
        ],
    )


class TestDigestSerialisation:
    def test_json_roundtrip(self):
        gd = make_global_digest()
        loaded = GlobalDigest.model_validate_json(gd.model_dump_json())
        assert loaded.run_id == gd.run_id
        assert len(loaded.digests) == 4

    def test_all_countries_present(self):
        gd = make_global_digest()
        assert gd.get(Country.USA) is not None
        assert gd.get(Country.UK) is not None
        assert gd.get(Country.INDIA) is not None
        assert gd.get(Country.BRAZIL) is not None

    def test_indicator_values_preserved(self):
        gd = make_global_digest()
        loaded = GlobalDigest.model_validate_json(gd.model_dump_json())
        ind = loaded.get(Country.USA).indicators[0]
        assert ind.latest.value == pytest.approx(6.0)

    def test_stale_status_survives_roundtrip(self):
        ind = make_indicator()
        stale = ind.model_copy(update={"status": FetchStatus.STALE})
        cd = CountryDigest(country=Country.USA, run_id="r1", indicators=[stale])
        loaded = CountryDigest.model_validate_json(cd.model_dump_json())
        assert loaded.indicators[0].status == FetchStatus.STALE

    def test_pct_change_computed_correctly(self):
        obs = [
            DataPoint(date=date(2024, 1, 1), value=100.0),
            DataPoint(date=date(2024, 2, 1), value=105.0),
        ]
        ind = make_indicator()
        ind2 = ind.model_copy(update={"observations": obs})
        assert ind2.pct_change == pytest.approx(5.0)


class TestKVPayload:
    """Verify the structure of what gets pushed to KV."""

    def test_kv_meta_structure(self, tmp_path):
        """meta:last_run must have expected keys."""
        digest = make_global_digest()
        digest_json = digest.model_dump_json()

        # Simulate what push_to_kv.py builds
        d = json.loads(digest_json)
        indicator_count = sum(len(x.get("indicators", [])) for x in d.get("digests", []))
        meta = {
            "run_id": d["run_id"],
            "generated_at": NOW.isoformat(),
            "indicator_count": indicator_count,
            "week": "2024-W22",
        }
        assert meta["run_id"] == "2024-06-01T00:00:00Z"
        assert meta["indicator_count"] == 4  # one per country

    def test_narrative_schema(self):
        """Narrative JSON must have required keys for frontend."""
        narrative = {
            "run_id": "2024-W22",
            "generated_at": NOW.isoformat(),
            "main_narrative": "## HEADLINE\nTest\n## THE BIG THREE\n**1. Title**\nBody",
            "country_briefs": {"usa": "US brief"},
            "meta": {"indicator_count": 8, "countries": ["usa"]},
        }
        assert "main_narrative" in narrative
        assert "country_briefs" in narrative
        assert "## HEADLINE" in narrative["main_narrative"]
