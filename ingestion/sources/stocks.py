"""
datablitz.ingestion.sources.stocks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Stock market context adapter using yfinance (no key, no auth).

For each country we track 3 instruments:
  - A broad equity index ETF (captures market sentiment)
  - The country's currency vs USD (macro stress indicator)
  - One sector ETF relevant to that country's story (e.g. energy for UK)

Why yfinance over Alpha Vantage?
  yfinance: no key, no rate limit for weekly cadence, unofficial but stable
  Alpha Vantage: 25 req/day free — enough, but adds setup friction
  Decision: yfinance primary, Alpha Vantage key optional for fundamentals later.

Output: one Indicator per symbol, weekly close prices (last 12 weeks).
Injected into the AI prompt as "MARKET CONTEXT" above the macro data.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone, timedelta
from typing import Any

import httpx

from ..base import BaseSource
from ..config import Settings
from ..schemas import Category, Country, DataPoint, Frequency, FetchStatus, Indicator

logger = logging.getLogger(__name__)

# Tickers per country: (symbol, human_name, category)
COUNTRY_TICKERS: dict[Country, list[tuple[str, str, Category]]] = {
    Country.USA: [
        ("SPY",    "US S&P 500 ETF (weekly close)",         Category.ECONOMIC),
        ("^VIX",   "US VIX Volatility Index",               Category.ECONOMIC),
        ("QQQ",    "US NASDAQ-100 ETF (weekly close)",      Category.ECONOMIC),
    ],
    Country.UK: [
        ("EWU",    "UK MSCI ETF (weekly close)",            Category.ECONOMIC),
        ("GBPUSD=X", "GBP/USD Exchange Rate",               Category.ECONOMIC),
        ("BP",     "BP plc - UK Energy Bellwether",         Category.ECONOMIC),
    ],
    Country.INDIA: [
        ("INDA",   "India MSCI ETF (weekly close)",         Category.ECONOMIC),
        ("INRUSD=X","INR/USD Exchange Rate",                Category.ECONOMIC),
        ("INFY",   "Infosys - India Tech Bellwether",       Category.ECONOMIC),
    ],
    Country.BRAZIL: [
        ("EWZ",    "Brazil MSCI ETF (weekly close)",        Category.ECONOMIC),
        ("BRL=X",  "BRL/USD Exchange Rate",                 Category.ECONOMIC),
        ("PBR",    "Petrobras - Brazil Energy Bellwether",  Category.ECONOMIC),
    ],
}

# Weeks of history to keep
WEEKS_BACK = 12


class StocksSource(BaseSource):
    """
    Fetches weekly stock/ETF/FX data via yfinance for market context.
    Not a BaseSource subclass in the traditional sense — doesn't need httpx client.
    Implemented as a standalone async function for simplicity.
    """
    source_name = "yfinance"
    country = Country.USA  # set per instance

    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        super().__init__(client, settings)

    async def fetch_indicators(self) -> list[Indicator]:
        return []  # Not used directly; use fetch_all_countries() instead


async def fetch_stock_context() -> dict[Country, list[Indicator]]:
    """
    Fetch weekly stock data for all 4 countries.
    Returns {Country: [Indicator]} — integrated into pipeline enrichment.
    Runs in thread pool since yfinance is synchronous.
    """
    import asyncio
    loop = asyncio.get_event_loop()

    tasks = {
        country: loop.run_in_executor(None, _fetch_country_stocks, country)
        for country in COUNTRY_TICKERS
    }

    results: dict[Country, list[Indicator]] = {}
    for country, task in tasks.items():
        try:
            indicators = await task
            if indicators:
                results[country] = indicators
                logger.info(f"Stocks [{country.value}]: {len(indicators)} indicators fetched")
        except Exception as exc:
            logger.warning(f"Stock fetch failed [{country.value}]: {exc}")

    return results


def _fetch_country_stocks(country: Country) -> list[Indicator]:
    """Synchronously fetch all tickers for one country via yfinance."""
    try:
        import yfinance as yf
    except ImportError:
        logger.error("yfinance not installed — run: pip install yfinance")
        return []

    tickers = COUNTRY_TICKERS.get(country, [])
    indicators: list[Indicator] = []
    end_date   = datetime.now(tz=timezone.utc).date()
    start_date = end_date - timedelta(weeks=WEEKS_BACK + 2)  # buffer

    for symbol, name, category in tickers:
        try:
            ticker = yf.Ticker(symbol)
            hist   = ticker.history(
                start=start_date.isoformat(),
                end=end_date.isoformat(),
                interval="1wk",
                auto_adjust=True,
            )

            if hist.empty or len(hist) < 2:
                logger.warning(f"yfinance: no data for {symbol}")
                continue

            observations: list[DataPoint] = []
            for ts, row in hist.iterrows():
                close = row.get("Close")
                if close is None or (hasattr(close, "__float__") and close != close):
                    continue  # skip NaN
                obs_date = ts.date() if hasattr(ts, "date") else date.fromisoformat(str(ts)[:10])
                observations.append(DataPoint(date=obs_date, value=round(float(close), 4)))

            if len(observations) < 2:
                continue

            # Keep last WEEKS_BACK
            observations = observations[-WEEKS_BACK:]

            ind = Indicator(
                id=f"{country.value}.stocks.{symbol.replace('^','').replace('=','').replace('/','_')}",
                name=name,
                source_name="yfinance / Yahoo Finance",
                source_url=f"https://finance.yahoo.com/quote/{symbol}",
                country=country,
                category=category,
                frequency=Frequency.WEEKLY,
                unit="usd" if "ETF" in name or "USD" in name or "Rate" in name else "local_currency",
                observations=observations,
                fetched_at=datetime.now(tz=timezone.utc),
                status=FetchStatus.LIVE,
            )
            indicators.append(ind)

        except Exception as exc:
            logger.warning(f"yfinance [{symbol}]: {exc}")

    return indicators


def format_stocks_for_prompt(stock_data: dict[Country, list[Indicator]]) -> str:
    """Format stock indicators as a compact prompt section."""
    if not stock_data:
        return ""

    lines = ["MARKET CONTEXT (weekly equity/FX data — use to gauge investor sentiment):"]
    for country, indicators in stock_data.items():
        if not indicators:
            continue
        lines.append(f"  {country.value.upper()}:")
        for ind in indicators:
            if not ind.observations:
                continue
            latest = ind.latest
            pct    = ind.pct_change
            arrow  = "▲" if pct and pct > 0 else "▼" if pct and pct < 0 else "—"
            pct_s  = f" ({arrow}{abs(pct):.1f}% WoW)" if pct else ""
            lines.append(f"    {ind.name}: {latest.value:.2f}{pct_s}")

    return "\n".join(lines)
