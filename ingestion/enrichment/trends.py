"""
datablitz.ingestion.enrichment.trends
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Fetches Google Trends data per country for economic/financial search terms.
Free, no API key, no auth. Uses pytrends (unofficial Google Trends API wrapper).

What this adds: If SELIC is hiking and "inflação" is spiking in Brazil searches,
that's corroboration from citizen behaviour that the numbers aren't just
statistical artefacts — people are feeling it.

Returns {country: {term: relative_interest_score}} where score is 0-100
(100 = peak interest in the time period).

We query a curated set of terms per country that map to what DataBlitz tracks:
economic stress, inflation, unemployment, housing costs, etc.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Terms to track per country. Chosen to map to our indicator categories.
# Use the local-language version when more representative.
TREND_QUERIES: dict[str, list[str]] = {
    "usa":    ["inflation", "unemployment", "interest rates", "recession", "gas prices"],
    "uk":     ["inflation UK", "cost of living", "mortgage rates", "unemployment UK", "energy bills"],
    "india":  ["inflation India", "petrol price", "unemployment India", "GDP India", "air quality Delhi"],
    "brazil": ["inflação", "desemprego", "selic", "dólar", "custo de vida"],
}

# Geo codes for Google Trends
GEO_CODES: dict[str, str] = {
    "usa":    "US",
    "uk":     "GB",
    "india":  "IN",
    "brazil": "BR",
}


def _fetch_trends_sync(country: str, terms: list[str], geo: str) -> dict[str, int]:
    """
    Fetch Google Trends interest scores synchronously.
    Returns {term: avg_interest_0_to_100_last_7_days}.
    """
    try:
        from pytrends.request import TrendReq

        pytrends = TrendReq(hl="en-US", tz=0, timeout=(10, 25))
        pytrends.build_payload(terms, timeframe="now 7-d", geo=geo)
        data = pytrends.interest_over_time()

        if data.empty:
            return {}

        # Average interest over the 7-day window, rounded to int
        result: dict[str, int] = {}
        for term in terms:
            if term in data.columns:
                avg = int(data[term].mean())
                if avg > 0:  # only include terms with actual signal
                    result[term] = avg

        return result

    except Exception as exc:
        logger.warning("trends_failed", extra={"country": country, "error": str(exc)})
        return {}


async def fetch_trends_context() -> dict[str, dict[str, int]]:
    """
    Fetch Google Trends for all 4 countries concurrently.
    Returns {country: {search_term: interest_score_0_100}}
    """
    loop = asyncio.get_event_loop()

    tasks = {
        country: loop.run_in_executor(
            None,
            _fetch_trends_sync,
            country,
            terms,
            GEO_CODES[country],
        )
        for country, terms in TREND_QUERIES.items()
    }

    results: dict[str, dict[str, int]] = {}
    for country, task in tasks.items():
        try:
            result = await task
            if result:
                results[country] = result
        except Exception as exc:
            logger.warning("trends_gather_failed", extra={"country": country, "error": str(exc)})

    # Log summary
    for country, scores in results.items():
        if scores:
            top = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:2]
            logger.info(f"Trends [{country}]: top terms = {top}")

    return results


def format_trends_for_prompt(trends: dict[str, dict[str, int]]) -> str:
    """Format trends data as a compact string for the AI prompt."""
    if not trends:
        return ""

    lines = []
    for country, scores in trends.items():
        if not scores:
            continue
        sorted_terms = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        terms_str = ", ".join(f'"{t}" ({s}/100)' for t, s in sorted_terms[:4])
        lines.append(f"  {country.upper()}: {terms_str}")

    if not lines:
        return ""

    return "GOOGLE SEARCH TRENDS (citizen interest, past 7 days, 100=peak):\n" + "\n".join(lines)
