"""
datablitz.ingestion.memory
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Structured memory layer for historical context injection (RAG).

Why not vector embeddings + Cloudflare Vectorize?
  Vectorize requires the paid Workers plan.
  More importantly: economic data is numerical. "Last time Brazil's SELIC
  was this high" is more precise and useful than cosine similarity on text.

This module maintains a SQLite store of:
  - Key indicator snapshots per week per country
  - A compressed narrative summary (200 chars) per week
  - Direction flags (rising/falling/flat) per indicator

At prompt time, find_similar_weeks() returns the N most historically
relevant weeks by comparing current indicator levels to stored history.
The injected context gives Claude/Grok/Gemini genuine institutional memory:
  "6 weeks ago, when Brazil's SELIC was 13.25% and rising,
   the narrative focused on corporate credit stress..."
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS weekly_memory (
    week_id      TEXT NOT NULL,   -- "2026-W13"
    country      TEXT NOT NULL,   -- "usa" | "uk" | "india" | "brazil"
    run_id       TEXT NOT NULL,   -- ISO datetime run identifier
    snapshots    TEXT NOT NULL,   -- JSON: {indicator_id: {value, direction, unit}}
    summary      TEXT NOT NULL,   -- 200-char compressed narrative for this country
    headline     TEXT DEFAULT '', -- top-level headline from the run
    created_at   TEXT NOT NULL,
    PRIMARY KEY (week_id, country)
);
CREATE INDEX IF NOT EXISTS idx_memory_country ON weekly_memory(country, week_id);
"""

DIRECTION_THRESHOLD = 0.5  # % change needed to count as "rising" or "falling"


class MemoryLayer:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self._ready = False

    async def _ensure_ready(self) -> None:
        if not self._ready:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            async with aiosqlite.connect(self.db_path) as db:
                await db.executescript(_DDL)
                await db.commit()
            self._ready = True

    # ── Write ──────────────────────────────────────────────────────────────

    async def store_week(
        self,
        week_id: str,
        country: str,
        run_id: str,
        indicators: list[dict],
        country_brief: str = "",
        headline: str = "",
    ) -> None:
        """
        Persist a week's indicator snapshot for one country.
        indicators: list of Indicator model dicts (from model_dump())
        """
        await self._ensure_ready()

        snapshots: dict[str, dict] = {}
        for ind in indicators:
            obs = ind.get("observations", [])
            if len(obs) < 1:
                continue
            latest = obs[-1]["value"]
            direction = "flat"
            if len(obs) >= 2:
                prev = obs[-2]["value"]
                if prev != 0:
                    pct = ((latest - prev) / abs(prev)) * 100
                    if pct > DIRECTION_THRESHOLD:
                        direction = "rising"
                    elif pct < -DIRECTION_THRESHOLD:
                        direction = "falling"
            snapshots[ind["id"]] = {
                "value":     round(latest, 4),
                "direction": direction,
                "unit":      ind.get("unit", ""),
                "name":      ind.get("name", ""),
                "category":  ind.get("category", ""),
            }

        # Compress brief to 200 chars for context injection
        summary = (country_brief or "").strip()[:200]

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT OR REPLACE INTO weekly_memory
                   (week_id, country, run_id, snapshots, summary, headline, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    week_id, country, run_id,
                    json.dumps(snapshots),
                    summary, headline,
                    datetime.now(tz=timezone.utc).isoformat(),
                ),
            )
            await db.commit()
        logger.debug(f"memory stored: {week_id}/{country} ({len(snapshots)} indicators)")

    # ── Read / retrieval ───────────────────────────────────────────────────

    async def find_similar_weeks(
        self,
        country: str,
        current_snapshots: dict[str, dict],
        n: int = 3,
        exclude_week: str | None = None,
    ) -> list[dict]:
        """
        Find N most historically relevant weeks for a country by comparing
        current indicator levels + directions to stored history.

        Similarity score = number of matching conditions:
          - Same direction (rising/falling/flat) for each indicator
          - Value within 20% of current value for each indicator

        Returns list of {week_id, score, summary, headline, snapshots}
        """
        await self._ensure_ready()

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM weekly_memory WHERE country = ? ORDER BY week_id DESC",
                (country,),
            ) as cur:
                rows = await cur.fetchall()

        if not rows:
            return []

        scored: list[tuple[float, dict]] = []
        for row in rows:
            wid = row["week_id"]
            if exclude_week and wid == exclude_week:
                continue
            try:
                stored = json.loads(row["snapshots"])
            except json.JSONDecodeError:
                continue

            score = _similarity_score(current_snapshots, stored)
            if score > 0:
                scored.append((score, {
                    "week_id":  wid,
                    "score":    score,
                    "summary":  row["summary"],
                    "headline": row["headline"],
                    "snapshots": stored,
                }))

        # Sort by score desc, return top N
        scored.sort(key=lambda x: x[0], reverse=True)
        return [item for _, item in scored[:n]]

    async def get_last_week(
        self, country: str, before_week: str | None = None
    ) -> dict | None:
        """Return the most recent stored week for a country (optionally before a given week)."""
        await self._ensure_ready()

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            if before_week:
                async with db.execute(
                    "SELECT * FROM weekly_memory WHERE country = ? AND week_id < ? ORDER BY week_id DESC LIMIT 1",
                    (country, before_week),
                ) as cur:
                    row = await cur.fetchone()
            else:
                async with db.execute(
                    "SELECT * FROM weekly_memory WHERE country = ? ORDER BY week_id DESC LIMIT 1",
                    (country,),
                ) as cur:
                    row = await cur.fetchone()

        if not row:
            return None
        return {
            "week_id":   row["week_id"],
            "summary":   row["summary"],
            "headline":  row["headline"],
            "snapshots": json.loads(row["snapshots"]),
        }

    async def weeks_stored(self, country: str) -> int:
        """How many weeks of history do we have for a country."""
        await self._ensure_ready()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM weekly_memory WHERE country = ?", (country,)
            ) as cur:
                row = await cur.fetchone()
        return row[0] if row else 0


# ── Similarity scoring ────────────────────────────────────────────────────────

def _similarity_score(current: dict, stored: dict) -> float:
    """
    Score how similar two snapshots are.
    Returns 0.0–1.0 (proportion of indicators with matching conditions).
    """
    if not current or not stored:
        return 0.0

    shared_keys = set(current.keys()) & set(stored.keys())
    if not shared_keys:
        return 0.0

    matches = 0
    for key in shared_keys:
        cur_snap  = current[key]
        stor_snap = stored[key]
        cur_val   = cur_snap.get("value", 0)
        stor_val  = stor_snap.get("value", 0)

        # Direction match
        if cur_snap.get("direction") == stor_snap.get("direction"):
            matches += 0.5

        # Value within 20% (avoids penalising normal drift over time)
        if stor_val != 0:
            pct_diff = abs((cur_val - stor_val) / abs(stor_val)) * 100
            if pct_diff <= 20:
                matches += 0.5

    return round(matches / len(shared_keys), 3)


# ── Formatting for prompt injection ──────────────────────────────────────────

def format_historical_context(
    similar_weeks_by_country: dict[str, list[dict]]
) -> str:
    """Format retrieved historical weeks as a compact prompt block."""
    if not similar_weeks_by_country:
        return ""

    blocks = []
    for country, weeks in similar_weeks_by_country.items():
        if not weeks:
            continue
        country_lines = [f"  {country.upper()} — similar historical weeks:"]
        for w in weeks:
            score_pct = int(w["score"] * 100)
            headline  = w["headline"][:60] + "…" if len(w["headline"]) > 60 else w["headline"]
            summary   = w["summary"][:120] + "…" if len(w["summary"]) > 120 else w["summary"]
            country_lines.append(
                f"    [{w['week_id']}] ({score_pct}% match) \"{headline}\"\n"
                f"      {summary}"
            )
        blocks.append("\n".join(country_lines))

    if not blocks:
        return ""

    return (
        "HISTORICAL CONTEXT (similar past weeks — use to find patterns and precedents):\n"
        + "\n\n".join(blocks)
    )


def format_week_over_week(
    deltas_by_country: dict[str, list[dict]]
) -> str:
    """Format week-over-week changes as a compact prompt block."""
    if not deltas_by_country:
        return ""

    lines = ["WEEK-OVER-WEEK CHANGES (vs last week):"]
    for country, deltas in deltas_by_country.items():
        if not deltas:
            continue
        lines.append(f"  {country.upper()}:")
        for d in deltas:
            arrow = "▲" if d["direction"] == "up" else "▼" if d["direction"] == "down" else "—"
            lines.append(f"    {arrow} {d['name']}: {d['prev']:.3f} → {d['current']:.3f} ({d['pct']:+.1f}%)")
    return "\n".join(lines)
