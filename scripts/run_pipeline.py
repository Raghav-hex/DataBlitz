#!/usr/bin/env python3
"""
scripts/run_pipeline.py
~~~~~~~~~~~~~~~~~~~~~~~~
Run the full DataBlitz pipeline on demand — locally or via CI.

Usage:
    python scripts/run_pipeline.py               # full run
    python scripts/run_pipeline.py --skip-ai     # ingestion only, skip Claude
    python scripts/run_pipeline.py --country usa # single country
    python scripts/run_pipeline.py --push        # push to Cloudflare KV after run
"""

from __future__ import annotations
import argparse, asyncio, logging, subprocess, sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from rich.console import Console
from rich.panel import Panel
from ingestion.pipeline import run_pipeline
from ingestion.schemas import Country

console = Console()
ROOT   = Path(__file__).parent.parent
DATA   = ROOT / "data"


def step(label: str) -> None:
    console.print(f"\n[bold cyan]▶ {label}[/bold cyan]")


async def main(country: str | None, skip_ai: bool, push: bool) -> None:
    start = datetime.now(tz=timezone.utc)
    console.print(Panel.fit(
        f"[bold]DataBlitz[/bold]  {start.strftime('%Y-%m-%d %H:%M UTC')}",
        border_style="cyan",
    ))
    DATA.mkdir(parents=True, exist_ok=True)

    # ── 1. Ingestion ────────────────────────────────────────────────────────
    step("Ingestion")
    countries = [Country(country)] if country else None
    try:
        digest = await run_pipeline(
            countries=countries,
            output_path=str(DATA / "digest_latest.json"),
        )
        total  = sum(len(d.indicators) for d in digest.digests)
        errors = sum(len(d.errors)     for d in digest.digests)
        console.print(f"  [green]✓[/green] {total} indicators, {errors} errors")
        for d in digest.digests:
            live  = sum(1 for i in d.indicators if i.status.value == "live")
            stale = sum(1 for i in d.indicators if i.status.value == "stale")
            console.print(f"    {d.country.value:8s}  live={live}  stale={stale}  errors={len(d.errors)}")
    except Exception as exc:
        console.print(f"  [red]✗ Ingestion failed: {exc}[/red]")
        sys.exit(1)

    if skip_ai:
        console.print("\n[yellow]--skip-ai: stopping after ingestion[/yellow]")
        return

    # ── 2. AI engine ────────────────────────────────────────────────────────
    step("AI narrative (Puter.js → Claude)")
    result = subprocess.run(
        ["node", "ai_engine/index.js", "--input", str(DATA / "digest_latest.json")],
        cwd=str(ROOT),
    )
    if result.returncode != 0:
        console.print("  [red]✗ AI engine failed — check PUTER_AUTH_TOKEN[/red]")
        sys.exit(1)
    console.print("  [green]✓[/green] narrative_latest.json written")

    # ── 3. Push to KV (optional) ────────────────────────────────────────────
    if push:
        step("Push to Cloudflare KV")
        result = subprocess.run(
            ["python", "delivery/push_to_kv.py"],
            cwd=str(ROOT),
        )
        if result.returncode != 0:
            console.print("  [red]✗ KV push failed — check CF_* env vars[/red]")
            sys.exit(1)
        console.print("  [green]✓[/green] KV updated")

    elapsed = (datetime.now(tz=timezone.utc) - start).total_seconds()
    console.print(Panel.fit(
        f"[bold green]Done in {elapsed:.1f}s[/bold green]  →  data/",
        border_style="green",
    ))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="DataBlitz on-demand pipeline runner")
    p.add_argument("--country", help="usa | uk | india | brazil")
    p.add_argument("--skip-ai", action="store_true")
    p.add_argument("--push", action="store_true", help="Push to Cloudflare KV after run")
    args = p.parse_args()
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main(args.country, args.skip_ai, args.push))
