"""
tests/test_pipeline.py
~~~~~~~~~~~~~~~~~~~~~~~
Integration tests for the pipeline runner and delivery formatter.
All HTTP mocked — no real network calls.
"""

import json
import pytest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from ingestion.schemas import (
    Category, Country, DataPoint, Frequency, FetchStatus,
    Indicator, CountryDigest, GlobalDigest,
)
from delivery.formatter import (
    narrative_to_markdown,
    narrative_to_email_html,
    _parse_sections,
    _inline_md,
)

NOW = datetime.now(tz=timezone.utc)

# ─── Fixtures ─────────────────────────────────────────────────────────────────

def make_indicator(
    id="usa.fred.UNRATE",
    country=Country.USA,
    category=Category.ECONOMIC,
    name="US Unemployment Rate",
):
    return Indicator(
        id=id,
        name=name,
        source_name="FRED",
        source_url="https://fred.stlouisfed.org",
        country=country,
        category=category,
        frequency=Frequency.MONTHLY,
        unit="percent",
        observations=[
            DataPoint(date=date(2024, m, 1), value=3.5 + m * 0.1)
            for m in range(1, 7)
        ],
        fetched_at=NOW,
        status=FetchStatus.LIVE,
    )


def make_global_digest():
    return GlobalDigest(
        run_id="2024-06-01T00:00:00Z",
        generated_at=NOW,
        digests=[
            CountryDigest(
                country=Country.USA,
                run_id="2024-06-01T00:00:00Z",
                indicators=[make_indicator()],
            ),
            CountryDigest(
                country=Country.UK,
                run_id="2024-06-01T00:00:00Z",
                indicators=[make_indicator(
                    id="uk.ons.CPIH",
                    country=Country.UK,
                    name="UK CPIH Inflation",
                )],
            ),
            CountryDigest(
                country=Country.INDIA,
                run_id="2024-06-01T00:00:00Z",
                indicators=[make_indicator(
                    id="india.wb.GDP_GROWTH",
                    country=Country.INDIA,
                    name="India GDP Growth",
                )],
            ),
            CountryDigest(
                country=Country.BRAZIL,
                run_id="2024-06-01T00:00:00Z",
                indicators=[make_indicator(
                    id="brazil.bcb.SELIC",
                    country=Country.BRAZIL,
                    name="Brazil SELIC Rate",
                    category=Category.ECONOMIC,
                )],
            ),
        ],
    )


# ─── Formatter tests ──────────────────────────────────────────────────────────

MOCK_NARRATIVE = {
    "run_id": "2024-06-01T00:00:00Z",
    "generated_at": "2024-06-01T12:00:00Z",
    "main_narrative": """## HEADLINE
Global inflation diverges sharply as Brazil tightens further

## THE BIG THREE

**1. Brazil's SELIC Squeeze**

Brazil's central bank held its SELIC rate at 10.75%, defying market expectations
for a cut. This marks the third consecutive hold as inflation remains above target.

So what? Businesses dependent on credit face continued pressure.

**2. UK Unemployment Creeps Up**

UK unemployment ticked to 4.2%, the highest in 18 months, as hiring freezes
spread across financial services and tech.

So what? BoE may delay rate cuts further, squeezing mortgage holders.

**3. India GDP Holds Strong**

India posted 7.2% annual GDP growth, outpacing every G20 economy for the third
consecutive quarter.

So what? Global capital flows increasingly favour India over China alternatives.

## CROSS-COUNTRY SIGNAL
All four economies show diverging labour market trajectories this week.
USA unemployment at 3.7% remains near historic lows. India's formal employment
data suggests continued absorption. UK and Brazil both show stress signs.

## DATA WATCH
- US unemployment steady at 3.7%
- UK inflation at 2.3%, approaching target
- Brazil IPCA at 4.5%, above 3% target
- India PM2.5 remains elevated at 89 μg/m³

## NUMBERS OF THE WEEK
**7.2%** — India's annual GDP growth rate, highest among G20 nations
**10.75%** — Brazil SELIC rate, unchanged for third consecutive meeting
**4.2%** — UK unemployment rate, 18-month high
**3.7%** — US unemployment, near historic lows

## METHODOLOGY NOTE
All data sourced from official government APIs. India health data uses
World Bank proxy indicators (1-2 year lag is normal for health stats).
""",
    "country_briefs": {
        "usa": "US economy holds steady with unemployment at 3.7%.",
        "uk": "UK labour market shows stress as unemployment reaches 4.2%.",
        "india": "India GDP growth at 7.2% continues to lead G20.",
        "brazil": "Brazil SELIC holds at 10.75% for third consecutive meeting.",
    },
    "meta": {
        "indicators_total": 12,
        "countries": ["usa", "uk", "india", "brazil"],
        "model": "claude-sonnet-4-5",
        "prompt_chars": 4200,
    },
}


class TestFormatter:
    def test_parse_sections_extracts_all_headings(self):
        sections = _parse_sections(MOCK_NARRATIVE["main_narrative"])
        assert "HEADLINE" in sections
        assert "THE BIG THREE" in sections
        assert "CROSS-COUNTRY SIGNAL" in sections
        assert "DATA WATCH" in sections
        assert "NUMBERS OF THE WEEK" in sections
        assert "METHODOLOGY NOTE" in sections

    def test_headline_extracted_correctly(self):
        sections = _parse_sections(MOCK_NARRATIVE["main_narrative"])
        assert "Brazil" in sections["HEADLINE"]
        assert "inflation" in sections["HEADLINE"].lower()

    def test_inline_md_bold(self):
        result = _inline_md("**GDP** grew by **3.2%** this quarter")
        assert "<strong>GDP</strong>" in result
        assert "<strong>3.2%</strong>" in result

    def test_inline_md_italic(self):
        result = _inline_md("*caveat*: data is preliminary")
        assert "<em>caveat</em>" in result

    def test_narrative_to_markdown_contains_headline(self):
        md = narrative_to_markdown(MOCK_NARRATIVE)
        assert "DataBlitz Weekly Digest" in md
        assert "Brazil" in md

    def test_narrative_to_markdown_contains_country_briefs(self):
        md = narrative_to_markdown(MOCK_NARRATIVE)
        assert "### USA" in md
        assert "### UK" in md
        assert "### INDIA" in md
        assert "### BRAZIL" in md

    def test_narrative_to_markdown_includes_all_sections(self):
        md = narrative_to_markdown(MOCK_NARRATIVE)
        assert "The Big Three" in md or "BIG THREE" in md
        assert "Country Snapshots" in md

    def test_narrative_to_email_html_valid_structure(self):
        html = narrative_to_email_html(MOCK_NARRATIVE)
        assert "<html>" in html
        assert "</html>" in html
        assert "DataBlitz" in html
        assert "Brazil" in html

    def test_narrative_to_email_html_headline_in_h1(self):
        html = narrative_to_email_html(MOCK_NARRATIVE)
        assert "<h1" in html
        assert "Brazil" in html

    def test_narrative_to_email_html_inline_styles(self):
        """Email HTML must use inline styles — no external CSS."""
        html = narrative_to_email_html(MOCK_NARRATIVE)
        assert 'style="' in html
        assert "<link" not in html
        assert "<style>" not in html or html.count("<style>") == 0

    def test_narrative_to_email_html_run_id_in_footer(self):
        html = narrative_to_email_html(MOCK_NARRATIVE)
        assert "2024-06-01T00:00:00Z" in html

    def test_formatter_roundtrip_from_file(self, tmp_path):
        """Write narrative to temp file, read it back, format it."""
        f = tmp_path / "narrative.json"
        f.write_text(json.dumps(MOCK_NARRATIVE))

        from delivery.formatter import load_narrative
        loaded = load_narrative(str(f))
        md = narrative_to_markdown(loaded)
        html = narrative_to_email_html(loaded)

        assert len(md) > 100
        assert len(html) > 100
        assert "DataBlitz" in md
        assert "DataBlitz" in html


# ─── Schema serialisation roundtrip ──────────────────────────────────────────

class TestDigestSerialisation:
    def test_global_digest_json_roundtrip(self):
        """GlobalDigest must survive JSON serialise → deserialise intact."""
        gd = make_global_digest()
        json_str = gd.model_dump_json()
        loaded = GlobalDigest.model_validate_json(json_str)

        assert loaded.run_id == gd.run_id
        assert len(loaded.digests) == 4
        assert loaded.get(Country.USA) is not None
        assert loaded.get(Country.BRAZIL) is not None

    def test_indicator_values_preserved(self):
        gd = make_global_digest()
        json_str = gd.model_dump_json()
        loaded = GlobalDigest.model_validate_json(json_str)

        usa = loaded.get(Country.USA)
        assert usa is not None
        ind = usa.indicators[0]
        assert len(ind.observations) == 6
        assert ind.latest.value == pytest.approx(3.5 + 6 * 0.1)

    def test_stale_status_preserved(self):
        ind = make_indicator()
        stale = ind.model_copy(update={"status": FetchStatus.STALE})
        cd = CountryDigest(
            country=Country.USA, run_id="r1", indicators=[stale]
        )
        json_str = cd.model_dump_json()
        loaded = CountryDigest.model_validate_json(json_str)
        assert loaded.indicators[0].status == FetchStatus.STALE
