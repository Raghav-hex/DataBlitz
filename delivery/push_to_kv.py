"""
delivery/push_to_kv.py
~~~~~~~~~~~~~~~~~~~~~~~
Pushes digest + narrative JSON to Cloudflare KV via REST API.
Runs as the final step in GitHub Actions after AI engine completes.

Keys written:
  digest:latest       → full GlobalDigest JSON
  narrative:latest    → full narrative JSON
  digest:YYYY-WW      → archived copy keyed by ISO year-week
  meta:last_run       → {run_id, generated_at, indicator_count}

Forever-free tier: KV allows 1k writes/day — we use 4 per run. Fine.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
import httpx

CF_API   = "https://api.cloudflare.com/client/v4"
ACCOUNT  = os.environ["CF_ACCOUNT_ID"]
NS_ID    = os.environ["CF_KV_NAMESPACE"]
TOKEN    = os.environ["CF_API_TOKEN"]

HEADERS  = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type":  "application/json",
}


def kv_put(key: str, value: str, ttl_seconds: int | None = None) -> None:
    url = f"{CF_API}/accounts/{ACCOUNT}/storage/kv/namespaces/{NS_ID}/values/{key}"
    params = {}
    if ttl_seconds:
        params["expiration_ttl"] = ttl_seconds

    resp = httpx.put(url, headers=HEADERS, content=value.encode(), params=params)
    resp.raise_for_status()
    result = resp.json()
    if not result.get("success"):
        raise RuntimeError(f"KV put failed for '{key}': {result}")
    print(f"  KV ✓  {key}  ({len(value):,} bytes)")


def main() -> None:
    root = Path(__file__).parent.parent

    digest_path    = root / "data" / "digest_latest.json"
    narrative_path = root / "data" / "narrative_latest.json"

    if not digest_path.exists():
        print("ERROR: digest_latest.json not found — ingestion must have failed")
        sys.exit(1)

    digest_json    = digest_path.read_text()
    digest         = json.loads(digest_json)
    run_id         = digest.get("run_id", "unknown")

    # ISO year-week archive key  e.g. "digest:2026-W13"
    now     = datetime.now(tz=timezone.utc)
    week_key = f"digest:{now.strftime('%Y-W%V')}"

    print(f"Pushing to Cloudflare KV — run_id={run_id}")

    kv_put("digest:latest", digest_json)
    kv_put(week_key, digest_json, ttl_seconds=60 * 60 * 24 * 90)  # 90-day archive

    if narrative_path.exists():
        narrative_json = narrative_path.read_text()
        narrative      = json.loads(narrative_json)
        kv_put("narrative:latest", narrative_json)

        week_narr = f"narrative:{now.strftime('%Y-W%V')}"
        kv_put(week_narr, narrative_json, ttl_seconds=60 * 60 * 24 * 90)
    else:
        print("  WARN: narrative_latest.json missing — skipping narrative KV push")

    # Lightweight meta record for the frontend header
    indicator_count = sum(
        len(d.get("indicators", [])) for d in digest.get("digests", [])
    )
    meta = {
        "run_id":          run_id,
        "generated_at":    now.isoformat(),
        "indicator_count": indicator_count,
        "week":            now.strftime("%Y-W%V"),
    }
    kv_put("meta:last_run", json.dumps(meta))
    print(f"\nDone — {indicator_count} indicators pushed to KV")

    # Write to Obsidian vault (non-fatal if not configured)
    if narrative_path.exists():
        try:
            from delivery.obsidian_writer import write_to_obsidian
            write_to_obsidian(str(narrative_path))
        except Exception as exc:
            print(f"  Obsidian write skipped: {exc}")


if __name__ == "__main__":
    main()
