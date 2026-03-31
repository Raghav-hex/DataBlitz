#!/usr/bin/env python3
"""
scripts/fetch_country.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Manual dry-run script for testing a single country's ingestion.

Usage:
    python scripts/fetch_country.py --country usa
    python scripts/fetch_country.py --country brazil --verbose
    python scripts/fetch_country.py --country uk --no-cache

Prints a rich table of fetched indicators without writing any files.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx
from rich.console import Console
from rich.table import Table
from rich import box

from ingestion.cache import CacheLayer
from ingestion.config import Settings
from ingestion.pipeline import fetch_country, SOURCE_REGISTRY
from ingestion.schemas import Country, FetchStatus

console = Console()


async def main(country_str: str, verbose: bool = False) -> None:
    try:
        country = Country(country_str.lower())
    except ValueError:
        console.print(f"[red]Unknown country: {country_str}[/red]")
        console.print(f"Valid options: {[c.value for c in Country]}")
        sys.exit(1)

    settings = Settings()
    cache = CacheLayer(db_path=settings.cache_db_path, ttl_hours=settings.cache_ttl_hours)

    console.print(f"\n[bold]DataBlitz — Fetching {country.value.upper()}[/bold]")
    console.print(f"Sources: {[s.__name__ for s in SOURCE_REGISTRY[country]]}\n")

    limits = httpx.Limits(max_connections=10, max_keepalive_connections=3)
    timeout = httpx.Timeout(settings.http_timeout_seconds)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        digest = await fetch_country(country, client, settings, cache, run_id="dry-run")

    if not digest.indicators:
        console.print("[red]No indicators fetched. Check your API keys in .env[/red]")
        if digest.errors:
            for err in digest.errors:
                console.print(f"  [red]• {err}[/red]")
        return

    # Build results table
    table = Table(
        title=f"{country.value.upper()} — {len(digest.indicators)} indicators",
        box=box.ROUNDED,
        show_lines=True,
    )
    table.add_column("ID", style="cyan", no_wrap=True, max_width=30)
    table.add_column("Name", max_width=32)
    table.add_column("Category", style="magenta")
    table.add_column("Latest value", justify="right", style="green")
    table.add_column("Chg%", justify="right")
    table.add_column("Status", justify="center")

    for ind in sorted(digest.indicators, key=lambda i: (i.category, i.id)):
        pct = ind.pct_change
        pct_str = f"{pct:+.2f}%" if pct is not None else "—"
        pct_style = "green" if pct and pct > 0 else "red" if pct and pct < 0 else ""
        status_icon = "✓" if ind.status == FetchStatus.LIVE else "⚠ STALE"
        status_style = "green" if ind.status == FetchStatus.LIVE else "yellow"

        table.add_row(
            ind.id.split(".")[-1],
            ind.name[:32],
            ind.category.value,
            f"{ind.latest.value:.4f} {ind.unit[:8]}",
            f"[{pct_style}]{pct_str}[/{pct_style}]" if pct_style else pct_str,
            f"[{status_style}]{status_icon}[/{status_style}]",
        )

    console.print(table)

    if digest.errors:
        console.print(f"\n[yellow]Warnings ({len(digest.errors)}):[/yellow]")
        for err in digest.errors:
            console.print(f"  [yellow]• {err}[/yellow]")

    if verbose:
        console.print("\n[bold]Detailed observations:[/bold]")
        for ind in digest.indicators:
            console.print(f"\n  [cyan]{ind.name}[/cyan] — last {len(ind.last_n)} observations:")
            for obs in ind.last_n:
                console.print(f"    {obs.date}  {obs.value:.4f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DataBlitz single-country dry-run")
    parser.add_argument("--country", required=True, help="Country code: usa, uk, india, brazil")
    parser.add_argument("--verbose", action="store_true", help="Show full observation history")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)  # suppress httpx noise
    asyncio.run(main(args.country, args.verbose))
