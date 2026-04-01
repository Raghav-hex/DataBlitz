"""
datablitz.ingestion.enrichment.rss
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Fetches recent news headlines per country via free RSS feeds.
No auth, no API keys, no cost. Pure XML parsing via feedparser.

Returns a dict of {country: [headline strings]} injected into the AI prompt
so Claude understands *why* the numbers moved, not just *what* they are.

Sources chosen for reliability, English availability, and relevance to
the economic/health/climate categories we track:

  USA  — Reuters Business, AP News Top Stories
  UK   — BBC News, The Guardian World
  India — The Hindu, NDTV India
  Brazil — Reuters LatAm, Agencia Brasil (English feed)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import NamedTuple

import feedparser
import httpx

logger = logging.getLogger(__name__)

# Each entry: (country_code, feed_name, url)
FEEDS: list[tuple[str, str, str]] = [
    # USA
    ("usa", "Reuters Business",
     "https://feeds.reuters.com/reuters/businessNews"),
    ("usa", "AP Top Stories",
     "https://feeds.apnews.com/rss/apf-topnews"),

    # UK
    ("uk", "BBC News",
     "https://feeds.bbci.co.uk/news/rss.xml"),
    ("uk", "The Guardian",
     "https://www.theguardian.com/world/rss"),

    # India
    ("india", "The Hindu",
     "https://www.thehindu.com/news/national/feeder/default.rss"),
    ("india", "NDTV India",
     "https://feeds.feedburner.com/ndtvnews-india-news"),

    # Brazil
    ("brazil", "Reuters LatAm",
     "https://feeds.reuters.com/reuters/latamTopNews"),
    ("brazil", "Agencia Brasil",
     "https://agenciabrasil.ebc.com.br/en/rss/ultimasnoticias/feed.xml"),
]

# How many headlines to keep per country (token budget conscious)
MAX_HEADLINES_PER_COUNTRY = 6

# Only headlines from the last N days
MAX_AGE_DAYS = 7


class Headline(NamedTuple):
    title:  str
    source: str
    published: str  # human-readable


def _parse_feed_sync(url: str, source: str) -> list[Headline]:
    """Parse a single RSS feed synchronously (feedparser is sync)."""
    try:
        feed = feedparser.parse(url)
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=MAX_AGE_DAYS)
        headlines: list[Headline] = []

        for entry in feed.entries[:10]:
            title = entry.get("title", "").strip()
            if not title or len(title) < 10:
                continue

            # Try to parse published date
            pub_str = "recent"
            try:
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    import time
                    pub_dt = datetime.fromtimestamp(
                        time.mktime(entry.published_parsed), tz=timezone.utc
                    )
                    if pub_dt < cutoff:
                        continue  # too old
                    pub_str = pub_dt.strftime("%d %b")
            except Exception:
                pass

            headlines.append(Headline(
                title=title,
                source=source,
                published=pub_str,
            ))

        return headlines
    except Exception as exc:
        logger.warning("rss_feed_failed", extra={"source": source, "url": url, "error": str(exc)})
        return []


async def fetch_news_context() -> dict[str, list[str]]:
    """
    Fetch headlines from all RSS feeds concurrently.
    Returns {country_code: [formatted_headline_strings]}
    """
    loop = asyncio.get_event_loop()

    # Run all feed fetches concurrently in thread pool (feedparser is sync)
    tasks = [
        loop.run_in_executor(None, _parse_feed_sync, url, source)
        for country, source, url in FEEDS
    ]
    countries_for_tasks = [country for country, source, url in FEEDS]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Aggregate by country
    by_country: dict[str, list[Headline]] = {}
    for country, headlines in zip(countries_for_tasks, results):
        if isinstance(headlines, Exception):
            logger.warning("rss_task_failed", extra={"country": country})
            continue
        if country not in by_country:
            by_country[country] = []
        by_country[country].extend(headlines)

    # Deduplicate by title prefix, cap at MAX_HEADLINES_PER_COUNTRY
    formatted: dict[str, list[str]] = {}
    for country, headlines in by_country.items():
        seen_prefixes: set[str] = set()
        kept: list[str] = []
        for h in headlines:
            prefix = h.title[:40].lower()
            if prefix not in seen_prefixes:
                seen_prefixes.add(prefix)
                kept.append(f"[{h.published}] {h.title} ({h.source})")
            if len(kept) >= MAX_HEADLINES_PER_COUNTRY:
                break
        if kept:
            formatted[country] = kept

    total = sum(len(v) for v in formatted.values())
    logger.info(f"RSS: fetched {total} headlines across {len(formatted)} countries")
    return formatted
