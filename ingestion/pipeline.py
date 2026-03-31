"""
datablitz.ingestion.pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Main pipeline orchestrator.

Runs all 4 country fetchers concurrently, handles cache fallback,
assembles a GlobalDigest, and serialises to JSON for the AI engine.

Usage:
    python -m ingestion.pipeline            # full run, JSON to stdout
    python -m ingestion.pipeline --country usa  # single country
    python -m ingestion.pipeline --dry-run      # fetch only, no output file
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx

from .cache import CacheLayer
from .config import Settings
from .schemas import Country, CountryDigest, GlobalDigest
from .sources.brazil.bcb import BCBSource
from .sources.brazil.ibge import IBGESource
from .sources.brazil.paho import PAHOBrazilSource
from .sources.india.openaq import OpenAQIndiaSource
from .sources.india.worldbank import WorldBankIndiaSource
from .sources.uk.boe import BoESource
from .sources.uk.ons import ONSSource
from .sources.usa.bls import BLSSource
from .sources.usa.cdc import CDCSource
from .sources.usa.fred import FREDSource
from .sources.usa.noaa import NOAASource

logger = logging.getLogger(__name__)

# ─── Source registry ─────────────────────────────────────────────────────────
# Maps each Country to its list of source adapter classes.
# Order matters for logging — more reliable sources first.

SOURCE_REGISTRY: dict[Country, list] = {
    Country.USA: [FREDSource, BLSSource, NOAASource, CDCSource],
    Country.UK:  [ONSSource, BoESource],
    Country.INDIA:  [WorldBankIndiaSource, OpenAQIndiaSource],
    Country.BRAZIL: [BCBSource, IBGESource, PAHOBrazilSource],
}


async def fetch_country(
    country: Country,
    client: httpx.AsyncClient,
    settings: Settings,
    cache: CacheLayer,
    run_id: str,
) -> CountryDigest:
    """
    Fetch all indicators for one country.
    For each source:
      1. Try live fetch
      2. On failure, fall back to cache for any missing indicator IDs
      3. Log what was live vs stale
    """
    all_indicators = []
    all_errors: list[str] = []

    source_classes = SOURCE_REGISTRY[country]

    for SourceClass in source_classes:
        try:
            source = SourceClass(client, settings)
        except ValueError as exc:
            # Missing API key — skip this source entirely
            msg = f"[{country.value}] {SourceClass.__name__} skipped: {exc}"
            logger.warning(msg)
            all_errors.append(msg)
            continue

        live_indicators, errors = await source.fetch()
        all_errors.extend(errors)

        if live_indicators:
            # Save fresh data to cache
            await cache.save_many(live_indicators)
            all_indicators.extend(live_indicators)
            logger.info(
                f"[{country.value}] {source.source_name}: "
                f"{len(live_indicators)} indicators (live)"
            )
        else:
            # Live fetch failed — try loading from cache
            logger.warning(
                f"[{country.value}] {source.source_name} live fetch failed — "
                f"attempting cache fallback"
            )
            # We don't know which IDs this source would have produced,
            # so we enumerate expected IDs from the source class if it exposes them
            expected_ids = getattr(SourceClass, "INDICATOR_IDS", [])
            if expected_ids:
                for ind_id in expected_ids:
                    cached = await cache.load(ind_id)
                    if cached:
                        all_indicators.append(cached)
                        logger.info(f"[{country.value}] Cache hit: {ind_id} (stale)")
                    else:
                        all_errors.append(f"No cache available for {ind_id}")

    return CountryDigest(
        country=country,
        run_id=run_id,
        indicators=all_indicators,
        errors=all_errors,
    )


async def run_pipeline(
    countries: list[Country] | None = None,
    output_path: str | None = None,
    dry_run: bool = False,
) -> GlobalDigest:
    """
    Full pipeline run. Returns a GlobalDigest.

    Args:
        countries: subset to fetch (default: all 4)
        output_path: write JSON to this file path
        dry_run: skip file write
    """
    settings = Settings()
    cache = CacheLayer(
        db_path=settings.cache_db_path,
        ttl_hours=settings.cache_ttl_hours,
    )

    run_id = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    target_countries = countries or list(Country)

    limits = httpx.Limits(
        max_connections=settings.http_max_connections,
        max_keepalive_connections=5,
    )
    timeout = httpx.Timeout(settings.http_timeout_seconds)

    logger.info(f"Pipeline run {run_id} — countries: {[c.value for c in target_countries]}")

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        # Fetch all countries concurrently
        tasks = [
            fetch_country(country, client, settings, cache, run_id)
            for country in target_countries
        ]
        digests = await asyncio.gather(*tasks)

    global_digest = GlobalDigest(
        run_id=run_id,
        generated_at=datetime.now(tz=timezone.utc),
        digests=list(digests),
    )

    # Summary log
    total_indicators = sum(len(d.indicators) for d in digests)
    total_errors = sum(len(d.errors) for d in digests)
    logger.info(
        f"Pipeline complete: {total_indicators} indicators, "
        f"{total_errors} errors, run_id={run_id}"
    )

    if not dry_run and output_path:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(global_digest.model_dump_json(indent=2))
        logger.info(f"Output written to {output_path}")

    return global_digest


# ─── CLI entry point ──────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="DataBlitz ingestion pipeline")
    parser.add_argument(
        "--country",
        choices=[c.value for c in Country],
        help="Run a single country only",
    )
    parser.add_argument(
        "--output",
        default="./data/digest_latest.json",
        help="Output JSON file path (default: ./data/digest_latest.json)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but do not write output file",
    )
    args = parser.parse_args()

    countries = [Country(args.country)] if args.country else None

    digest = asyncio.run(
        run_pipeline(
            countries=countries,
            output_path=args.output,
            dry_run=args.dry_run,
        )
    )

    # Print summary to stdout
    print(f"\n{'='*50}")
    print(f"Run ID  : {digest.run_id}")
    print(f"Countries: {len(digest.digests)}")
    for d in digest.digests:
        live = sum(1 for i in d.indicators if i.status.value == "live")
        stale = sum(1 for i in d.indicators if i.status.value == "stale")
        print(f"  {d.country.value:8s}  indicators={len(d.indicators)} "
              f"(live={live}, stale={stale})  errors={len(d.errors)}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()
