"""
datablitz.ingestion.sources.global.polymarket
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Polymarket CLOB REST API adapter — no API key, no auth required.

Polymarket is the world's largest prediction market ($3.6B+ trading volume).
Market prices are real-money probabilities — academically validated to be
more accurate than expert polls and news analysis.

API: https://clob.polymarket.com/markets  (completely open, no key)

We fetch markets relevant to our 4 countries' economic conditions:
  - Fed rate decisions (USA)
  - Recession probabilities (USA, UK, global)
  - Central bank policy (Brazil BCB, BoE)
  - Geopolitical risk (affecting all 4 countries)
  - Commodity/energy shocks

Why this is a game-changer:
  Jiang's "Pizza Index" is a proxy for crowd wisdom. Polymarket IS crowd
  wisdom — financially incentivised, real-money, continuously updated.
  When "US recession 2026" probability moves from 35% → 52% overnight,
  that's more meaningful than any survey or analyst note.

The DAILY DELTA is the signal: probability movement, not absolute level.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

CLOB_API = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"  # higher-level market metadata

# Search terms to find relevant markets — Polymarket has 1500+ active markets
RELEVANT_QUERIES = [
    "recession",
    "Fed rate",
    "Brazil",
    "inflation",
    "interest rate cut",
    "oil price",
    "unemployment",
    "GDP",
    "India economy",
    "UK economy",
]

# Hard-coded slugs for the most consistently relevant markets
# (more stable than search — these are perennial markets)
PINNED_MARKET_SLUGS = [
    "will-the-us-enter-a-recession-in-2026",
    "fed-cut-rates-2026",
    "will-us-inflation-exceed-3-percent-in-2026",
    "will-oil-exceed-100-in-2026",
    "will-brazil-gdp-grow-in-2026",
]


@dataclass
class PolymarketSignal:
    question:      str
    probability:   float    # 0.0-1.0
    volume_usd:    float    # total trading volume in USD
    prev_prob:     float    # yesterday's probability (if available)
    delta:         float    # probability change (positive = more likely)
    category:      str      # "economics", "geopolitics", etc.
    url:           str


async def fetch_polymarket_signals() -> list[PolymarketSignal]:
    """
    Fetch prediction market probabilities for economic-relevant markets.
    Returns sorted list by volume (highest liquidity = most reliable).
    """
    signals: list[PolymarketSignal] = []

    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        # Try Gamma API first (better metadata, market categories)
        gamma_results = await _fetch_gamma_markets(client)
        if gamma_results:
            signals.extend(gamma_results)

        # Fallback: direct CLOB markets endpoint
        if not signals:
            clob_results = await _fetch_clob_markets(client)
            signals.extend(clob_results)

    # Sort by volume desc (highest = most reliable signal)
    signals.sort(key=lambda s: s.volume_usd, reverse=True)
    return signals[:15]  # top 15 by liquidity


async def _fetch_gamma_markets(client: httpx.AsyncClient) -> list[PolymarketSignal]:
    """Fetch from Gamma API — broader market data with categories."""
    signals = []
    try:
        params = {
            "closed":   "false",
            "order":    "volume",
            "ascending":"false",
            "limit":    100,
            "tag_slug": "economics",  # economics category
        }
        resp = await client.get(f"{GAMMA_API}/markets", params=params, timeout=10)
        if resp.status_code != 200:
            return []

        markets = resp.json()
        if isinstance(markets, dict):
            markets = markets.get("markets", [])

        for m in markets[:50]:
            sig = _parse_gamma_market(m)
            if sig:
                signals.append(sig)

    except Exception as exc:
        logger.warning(f"Polymarket Gamma API: {exc}")

    return signals


async def _fetch_clob_markets(client: httpx.AsyncClient) -> list[PolymarketSignal]:
    """Fallback: fetch from CLOB markets endpoint."""
    signals = []
    try:
        resp = await client.get(
            f"{CLOB_API}/markets",
            params={"closed": "false", "limit": 100},
            timeout=10,
        )
        if resp.status_code != 200:
            return []

        data = resp.json()
        markets = data if isinstance(data, list) else data.get("data", [])

        for m in markets:
            sig = _parse_clob_market(m)
            if sig and _is_relevant(sig.question):
                signals.append(sig)

    except Exception as exc:
        logger.warning(f"Polymarket CLOB API: {exc}")

    return signals


def _parse_gamma_market(m: dict) -> PolymarketSignal | None:
    try:
        question = m.get("question", "")
        if not question or not _is_relevant(question):
            return None

        # Gamma returns outcomes with probability
        outcomes = m.get("outcomes", [])
        yes_prob = 0.5
        for outcome in outcomes:
            if outcome.get("value", "").lower() in ("yes", "true"):
                p = outcome.get("probability") or outcome.get("price")
                if p is not None:
                    yes_prob = float(p)
                    if yes_prob > 1:
                        yes_prob /= 100  # sometimes percentage
                break

        volume = float(m.get("volumeNum", m.get("volume", 0)) or 0)
        slug   = m.get("slug", "")

        return PolymarketSignal(
            question=question,
            probability=round(yes_prob, 3),
            volume_usd=volume,
            prev_prob=yes_prob,  # delta computed separately if available
            delta=0.0,
            category=m.get("category", "economics"),
            url=f"https://polymarket.com/event/{slug}" if slug else "https://polymarket.com",
        )
    except Exception:
        return None


def _parse_clob_market(m: dict) -> PolymarketSignal | None:
    try:
        question = m.get("question", "")
        if not question:
            return None

        # CLOB tokens: find YES token price
        tokens = m.get("tokens", [])
        yes_prob = 0.5
        for token in tokens:
            if token.get("outcome", "").lower() == "yes":
                price = token.get("price")
                if price is not None:
                    yes_prob = float(price)
                break

        volume = float(m.get("volume", 0) or 0)

        return PolymarketSignal(
            question=question,
            probability=round(yes_prob, 3),
            volume_usd=volume,
            prev_prob=yes_prob,
            delta=0.0,
            category="economics",
            url=f"https://polymarket.com/event/{m.get('market_slug', '')}",
        )
    except Exception:
        return None


def _is_relevant(question: str) -> bool:
    """Filter markets to those relevant to DataBlitz's 4 countries and topics."""
    q = question.lower()
    keywords = [
        "recession", "gdp", "inflation", "interest rate", "fed", "federal reserve",
        "unemployment", "oil", "crude", "brazil", "india", "uk ", "britain",
        "emerging market", "dollar", "currency", "central bank", "bcb", "boe",
        "rate cut", "rate hike", "debt", "default", "economy", "economic",
    ]
    return any(k in q for k in keywords)


def format_polymarket_for_prompt(signals: list[PolymarketSignal]) -> str:
    """Format Polymarket signals as a compact prompt section."""
    if not signals:
        return ""

    # Filter to meaningful volume ($100k+), then sort by volume desc
    liquid = sorted(
        [s for s in signals if s.volume_usd >= 100_000],
        key=lambda s: s.volume_usd, reverse=True,
    )
    if not liquid:
        liquid = sorted(signals, key=lambda s: s.volume_usd, reverse=True)[:8]

    lines = [
        "POLYMARKET PREDICTION ODDS (real-money crowd probabilities, updated daily):",
        "  [These are financial bets by informed traders — higher volume = more reliable]",
    ]

    for sig in liquid[:10]:
        pct = int(sig.probability * 100)
        vol_str = f"${sig.volume_usd/1e6:.1f}M" if sig.volume_usd >= 1e6 else f"${sig.volume_usd/1e3:.0f}K"
        delta_str = ""
        if abs(sig.delta) > 0.01:
            arrow = "▲" if sig.delta > 0 else "▼"
            delta_str = f" [{arrow}{abs(sig.delta*100):.1f}pp today]"

        # Risk framing
        risk = "HIGH" if pct >= 60 else "MEDIUM" if pct >= 35 else "LOW"
        lines.append(
            f"  {pct}% [{risk}] — {sig.question[:70]}"
            f"  (vol={vol_str}){delta_str}"
        )

    return "\n".join(lines)


def get_divergence_signals(
    polymarket_signals: list[PolymarketSignal],
    psi_scores: dict,  # from turchin.py
) -> list[str]:
    """
    Find divergence: crowd says LOW probability but PSI says HIGH stress.
    This is Jiang's core insight — 'hubris + desperation' is when the
    official narrative (crowd odds) diverges from structural reality (PSI).
    """
    divergences = []

    recession_market = next(
        (s for s in polymarket_signals if "recession" in s.question.lower()),
        None
    )
    usa_psi = psi_scores.get("usa")

    if recession_market and usa_psi:
        # Crowd says <30% recession probability but PSI says elevated
        if recession_market.probability < 0.30 and usa_psi.psi > 0.45:
            divergences.append(
                f"⚡ DIVERGENCE: Crowd assigns {int(recession_market.probability*100)}% recession probability "
                f"but Turchin PSI={usa_psi.psi:.2f} (elevated). "
                f"This is the 'hubris before correction' structural signal."
            )
        # Crowd says >60% but PSI is stable
        elif recession_market.probability > 0.60 and usa_psi.psi < 0.30:
            divergences.append(
                f"⚡ DIVERGENCE: Crowd assigns {int(recession_market.probability*100)}% recession probability "
                f"but structural stress (PSI={usa_psi.psi:.2f}) is low. "
                f"Market may be overpricing fear — watch for mean reversion."
            )

    return divergences
