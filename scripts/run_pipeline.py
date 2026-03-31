#!/usr/bin/env python3
"""
scripts/run_pipeline.py
~~~~~~~~~~~~~~~~~~~~~~~~
Full DataBlitz weekly pipeline runner.

Steps:
  1. Run Python ingestion → data/digest_latest.json
  2. Run Node.js AI engine → data/narrative_latest.json
  3. Format output → data/digest.md + data/digest_email.html

Usage:
    python scripts/run_pipeline.py
    python scripts/run_pipeline.py --skip-ai   # ingestion only
    python scripts/run_pipeline.py --country usa  # single country

Requires .env to be populated with API keys.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from rich.console import Console
from rich.panel import Panel

from ingestion.pipeline import run_pipeline
from ingestion.schemas import Country
from delivery.formatter import load_narrative, narrative_to_markdown, narrative_to_email_html

console = Console()
ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"


def run_ai_engine() -> bool:
    """Invoke Node.js AI engine as subprocess."""
    narrative_input = DATA_DIR / "digest_latest.json"
    if not narrative_input.exists():
        console.print("[red]digest_latest.json not found — run ingestion first[/red]")
        return False

    console.print("\n[bold cyan]Step 2: AI narrative engine[/bold cyan]")
    result = subprocess.run(
        ["node", "ai_engine/index.js"],
        cwd=str(ROOT),
        capture_output=False,
    )
    return result.returncode == 0


def format_outputs() -> None:
    """Convert narrative JSON → Markdown + HTML."""
    narrative_path = DATA_DIR / "narrative_latest.json"
    if not narrative_path.exists():
        console.print("[yellow]No narrative found — skipping format step[/yellow]")
        return

    narrative = load_narrative(str(narrative_path))

    md_path = DATA_DIR / "digest_latest.md"
    md_path.write_text(narrative_to_markdown(narrative))
    console.print(f"[green]✓ Markdown: {md_path}[/green]")

    html_path = DATA_DIR / "digest_latest_email.html"
    html_path.write_text(narrative_to_email_html(narrative))
    console.print(f"[green]✓ Email HTML: {html_path}[/green]")


async def main(country: str | None = None, skip_ai: bool = False) -> None:
    start = datetime.now(tz=timezone.utc)

    console.print(Panel.fit(
        "[bold]DataBlitz Pipeline[/bold]\n"
        f"Started: {start.strftime('%Y-%m-%d %H:%M UTC')}",
        border_style="cyan",
    ))

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # ── Step 1: Ingestion ──────────────────────────────────────────────────
    console.print("\n[bold cyan]Step 1: Data ingestion[/bold cyan]")
    countries = [Country(country)] if country else None

    try:
        digest = await run_pipeline(
            countries=countries,
            output_path=str(DATA_DIR / "digest_latest.json"),
        )
        total = sum(len(d.indicators) for d in digest.digests)
        errors = sum(len(d.errors) for d in digest.digests)
        console.print(f"[green]✓ Ingestion complete: {total} indicators, {errors} errors[/green]")
    except Exception as exc:
        console.print(f"[red]✗ Ingestion failed: {exc}[/red]")
        sys.exit(1)

    if skip_ai:
        console.print("\n[yellow]--skip-ai flag set — stopping after ingestion[/yellow]")
        return

    # ── Step 2: AI engine ──────────────────────────────────────────────────
    if run_ai_engine():
        console.print("[green]✓ AI narrative generated[/green]")
    else:
        console.print("[red]✗ AI engine failed — check PUTER_AUTH_TOKEN[/red]")
        sys.exit(1)

    # ── Step 3: Format ──────────────────────────────────────────────────────
    console.print("\n[bold cyan]Step 3: Formatting outputs[/bold cyan]")
    format_outputs()

    elapsed = (datetime.now(tz=timezone.utc) - start).total_seconds()
    console.print(Panel.fit(
        f"[bold green]Pipeline complete in {elapsed:.1f}s[/bold green]\n"
        f"Outputs in: {DATA_DIR}",
        border_style="green",
    ))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DataBlitz full pipeline runner")
    parser.add_argument("--country", help="Single country: usa, uk, india, brazil")
    parser.add_argument("--skip-ai", action="store_true", help="Skip AI narrative step")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    asyncio.run(main(country=args.country, skip_ai=args.skip_ai))
