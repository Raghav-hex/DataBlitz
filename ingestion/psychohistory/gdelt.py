"""
datablitz.ingestion.psychohistory.gdelt
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
GDELT (Global Database of Events, Language, and Tone) adapter.

GDELT monitors news in 100+ languages worldwide and assigns:
  - Goldstein Scale: conflict/cooperation intensity (-10 to +10)
  - Tone: average emotional tone of coverage
  - EventCode: CAMEO event taxonomy (protest, attack, statement, etc.)

This is the Jiang Xueqin "structural sickness" detection layer.
High negative tone + high conflict events = "hubris + desperation" signal.

API: https://api.gdeltproject.org/api/v2/doc/doc
Free, no key required. Rate limited but generous for weekly cadence.

We query the last 7 days of GDELT data per country and compute:
  1. Average Goldstein Scale (negative = conflict trending)
  2. Average tone
  3. Top conflict event categories
  4. "Breakpoint score" — % of articles with negative Goldstein < -5
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Any

import httpx

logger = logging.getLogger(__name__)

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"

# Country-specific GDELT query terms (maps to our 4 countries)
COUNTRY_QUERIES = {
    "usa":    "United States economy Federal Reserve inflation",
    "uk":     "United Kingdom economy Bank of England inflation",
    "india":  "India economy GDP Reserve Bank",
    "brazil": "Brazil economy SELIC Banco Central inflation",
}

# CAMEO event codes indicating high conflict (Goldstein < -5)
HIGH_CONFLICT_CODES = {"190", "191", "192", "193", "194", "195", "196",  # assault
                        "180", "181", "182",  # coerce
                        "200", "201", "202",  # fight
                        "172", "173", "174",  # protest}
                        }


@dataclass
class GDELTSignal:
    country: str
    avg_tone: float        # average news tone (-100 to +100)
    goldstein_avg: float   # average conflict intensity
    breakpoint_pct: float  # % articles with severe conflict (Goldstein < -5)
    top_themes: list[str]  # dominant GDELT themes this week
    article_count: int
    crisis_level: str      # calm | tense | volatile | crisis


async def fetch_gdelt_signals() -> dict[str, GDELTSignal]:
    """
    Fetch GDELT tone signals for all 4 countries.
    Returns {country: GDELTSignal} — empty dict on failure (non-fatal).
    """
    results: dict[str, GDELTSignal] = {}

    async with httpx.AsyncClient(timeout=20) as client:
        tasks = {
            country: _fetch_country_gdelt(client, country, query)
            for country, query in COUNTRY_QUERIES.items()
        }
        for country, task in tasks.items():
            try:
                signal = await task
                if signal:
                    results[country] = signal
            except Exception as exc:
                logger.warning(f"GDELT [{country}]: {exc}")

    return results


async def _fetch_country_gdelt(
    client: httpx.AsyncClient, country: str, query: str
) -> GDELTSignal | None:
    """
    Fetch GDELT Article List API for a country query.
    Uses the ArtList endpoint which returns tone + metadata per article.
    """
    end   = datetime.now(tz=timezone.utc)
    start = end - timedelta(days=7)

    params = {
        "query":    query,
        "mode":     "artlist",
        "maxrecords": 250,
        "startdatetime": start.strftime("%Y%m%d%H%M%S"),
        "enddatetime":   end.strftime("%Y%m%d%H%M%S"),
        "format":   "json",
        "sort":     "datedesc",
    }

    try:
        resp = await client.get(GDELT_DOC_API, params=params)
        if resp.status_code != 200:
            logger.warning(f"GDELT {country}: HTTP {resp.status_code}")
            return None

        data = resp.json()
    except Exception as exc:
        logger.warning(f"GDELT {country} request failed: {exc}")
        return None

    articles = data.get("articles", [])
    if not articles:
        # GDELT sometimes returns no results — not an error
        return GDELTSignal(
            country=country, avg_tone=0.0, goldstein_avg=0.0,
            breakpoint_pct=0.0, top_themes=[], article_count=0,
            crisis_level="calm",
        )

    # Extract tone scores
    tones = []
    for art in articles:
        tone_str = art.get("tone", "")
        if tone_str:
            try:
                # GDELT tone field: "tone,positive,negative,polarity,activity,self/group"
                tone_val = float(tone_str.split(",")[0])
                tones.append(tone_val)
            except (ValueError, IndexError):
                continue

    avg_tone      = sum(tones) / len(tones) if tones else 0.0
    negative_pct  = sum(1 for t in tones if t < -5) / len(tones) if tones else 0.0

    # Goldstein proxy: map tone to -10/+10 scale
    goldstein_avg = avg_tone * 0.1  # approximate mapping

    # Classify crisis level
    if negative_pct > 0.6 or avg_tone < -8:
        crisis_level = "crisis"
    elif negative_pct > 0.4 or avg_tone < -4:
        crisis_level = "volatile"
    elif negative_pct > 0.2 or avg_tone < -1:
        crisis_level = "tense"
    else:
        crisis_level = "calm"

    # Extract top themes from article metadata
    theme_counts: dict[str, int] = {}
    for art in articles[:50]:  # sample first 50
        for theme in art.get("socialimage", "").split(";"):
            t = theme.strip()
            if t and len(t) > 3:
                theme_counts[t] = theme_counts.get(t, 0) + 1

    top_themes = sorted(theme_counts, key=theme_counts.get, reverse=True)[:4]

    return GDELTSignal(
        country=country,
        avg_tone=round(avg_tone, 2),
        goldstein_avg=round(goldstein_avg, 2),
        breakpoint_pct=round(negative_pct * 100, 1),
        top_themes=top_themes,
        article_count=len(articles),
        crisis_level=crisis_level,
    )


def format_gdelt_for_prompt(signals: dict[str, GDELTSignal]) -> str:
    """Format GDELT signals as compact prompt section."""
    if not signals:
        return ""

    icon_map = {"crisis": "🔴", "volatile": "🟠", "tense": "🟡", "calm": "🟢"}
    lines = ["GDELT MEDIA TONE SIGNALS (global news sentiment, last 7 days):"]

    for country, sig in sorted(signals.items(), key=lambda x: x[1].avg_tone):
        icon = icon_map.get(sig.crisis_level, "⚪")
        lines.append(
            f"  {icon} {country.upper()}: tone={sig.avg_tone:+.1f} "
            f"[{sig.crisis_level}] | negative_coverage={sig.breakpoint_pct:.0f}%"
            f" | n={sig.article_count} articles"
        )

    return "\n".join(lines)
