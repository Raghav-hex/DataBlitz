"""
datablitz.ingestion.sources.global.bdi
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Baltic Dry Index (BDI) adapter via yfinance — daily, no API key needed.

The BDI is arguably the single cleanest macro signal available for free:
  - "Totally devoid of speculative content" (Howard Simons, TheStreet)
  - People don't charter ships unless they have actual cargo to move
  - Leads trade volume data by 4-6 weeks
  - Directly links to our 4 countries: Brazil iron ore, India coal,
    UK energy imports, US grain exports

Sub-indices tracked:
  ^BDI    Baltic Dry Index (composite)
  ^BCI    Capesize (iron ore, coal — 150k ton ships)
  ^BPI    Panamax (grain, coal — 60-70k ton)
  ^BSI    Supramax (mid-sized bulk)

Why this is a game-changer:
  When BDI collapses → global trade contracting → GDP prints lag by months
  When BCI collapses specifically → China iron ore demand falling
    → Brazil mining revenue about to drop → BRL pressure ahead
  When BPI rises → grain trade active → India/Brazil agri exports strong

Jiang's framework: BDI is the "oceanic current" beneath the surface noise
of daily stock markets. It's real physical trade, not financial positioning.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta, date
from typing import NamedTuple

import httpx

from ...base import BaseSource
from ...config import Settings
from ...schemas import Category, Country, DataPoint, Frequency, FetchStatus, Indicator

logger = logging.getLogger(__name__)

# yfinance tickers for Baltic indices
BDI_TICKERS = [
    ("^BDI", "Baltic Dry Index (composite)",         "bdi.composite"),
    ("^BCI", "Baltic Capesize Index (iron ore/coal)", "bdi.capesize"),
    ("^BPI", "Baltic Panamax Index (grain/coal)",     "bdi.panamax"),
    ("^BSI", "Baltic Supramax Index",                 "bdi.supramax"),
]

DAYS_BACK = 30


def _fetch_bdi_sync() -> list[Indicator]:
    """Synchronously fetch all BDI tickers via yfinance."""
    try:
        import yfinance as yf
    except ImportError:
        logger.error("yfinance not installed")
        return []

    indicators: list[Indicator] = []
    end_date   = datetime.now(tz=timezone.utc).date()
    start_date = end_date - timedelta(days=DAYS_BACK + 7)

    for ticker_sym, name, id_suffix in BDI_TICKERS:
        try:
            ticker = yf.Ticker(ticker_sym)
            hist   = ticker.history(
                start=start_date.isoformat(),
                end=end_date.isoformat(),
                interval="1d",
                auto_adjust=True,
            )
            if hist.empty:
                logger.warning(f"BDI: no data for {ticker_sym}")
                continue

            observations: list[DataPoint] = []
            for ts, row in hist.iterrows():
                close = row.get("Close")
                if close is None or (hasattr(close, "__float__") and close != close):
                    continue
                obs_date = ts.date() if hasattr(ts, "date") else date.fromisoformat(str(ts)[:10])
                observations.append(DataPoint(date=obs_date, value=round(float(close), 2)))

            if len(observations) < 2:
                continue

            ind = Indicator(
                id=f"global.{id_suffix}",
                name=name,
                source_name="Baltic Exchange / yfinance",
                source_url="https://www.balticexchange.com/en/data-services/market-information0/indices.html",
                country=Country.USA,   # global indicator, assigned to USA node
                category=Category.ECONOMIC,
                frequency=Frequency.DAILY,
                unit="index_points",
                observations=observations,
                fetched_at=datetime.now(tz=timezone.utc),
                status=FetchStatus.LIVE,
            )
            indicators.append(ind)
            logger.info(f"BDI [{ticker_sym}]: {len(observations)} days, latest={observations[-1].value:.0f}")

        except Exception as exc:
            logger.warning(f"BDI [{ticker_sym}]: {exc}")

    return indicators


async def fetch_bdi() -> list[Indicator]:
    """Async wrapper — runs yfinance in thread pool."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch_bdi_sync)


def format_bdi_for_prompt(indicators: list[Indicator]) -> str:
    """Format BDI as a compact prompt section with interpretation."""
    if not indicators:
        return ""

    lines = ["BALTIC DRY INDEX (daily physical trade signal — no speculative content):"]

    bdi_composite = next((i for i in indicators if "composite" in i.id), None)
    bdi_capesize  = next((i for i in indicators if "capesize"  in i.id), None)

    for ind in indicators:
        if not ind.observations:
            continue
        latest = ind.latest
        pct    = ind.pct_change
        # 5-day trend
        trend  = ind.observations[-5:]
        direction = "↑ rising" if pct and pct > 2 else "↓ falling" if pct and pct < -2 else "→ flat"
        spark  = " → ".join(f"{o.value:.0f}" for o in trend)
        lines.append(f"  {ind.name}: {latest.value:.0f} pts [{direction}]")
        lines.append(f"    5d: {spark}")

    # Add interpretation for highest-signal sub-index
    if bdi_capesize and bdi_capesize.pct_change:
        pct = bdi_capesize.pct_change
        if pct < -10:
            lines.append(f"  ⚠ Capesize ({pct:+.1f}%): sharp fall → China iron ore demand contracting → watch Brazil mining revenue, BRL pressure")
        elif pct > 10:
            lines.append(f"  ✓ Capesize ({pct:+.1f}%): strong rise → China iron ore demand → Brazil/India trade receipts improving")

    return "\n".join(lines)
