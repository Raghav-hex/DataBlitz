"""
datablitz.ingestion.cache
~~~~~~~~~~~~~~~~~~~~~~~~~~
Async SQLite stale-cache layer.

Purpose: If a live API fetch fails, we fall back to the last known-good
snapshot for that indicator rather than publishing a broken digest.

Design decisions:
  - One SQLite DB file (configurable path)
  - Table: cache(indicator_id TEXT PK, payload TEXT, saved_at TEXT)
  - payload is the full Indicator model serialised as JSON
  - TTL is checked on read — stale entries older than CACHE_TTL_HOURS are
    treated as expired and will NOT be served (hard fail instead)
  - aiosqlite used for non-blocking I/O inside async pipeline
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiosqlite

from .schemas import FetchStatus, Indicator

logger = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS cache (
    indicator_id TEXT PRIMARY KEY,
    payload      TEXT NOT NULL,
    saved_at     TEXT NOT NULL
);
"""


class CacheLayer:
    def __init__(self, db_path: str | Path, ttl_hours: int = 48) -> None:
        self.db_path = Path(db_path)
        self.ttl = timedelta(hours=ttl_hours)
        self._ready = False

    async def _ensure_ready(self) -> None:
        if not self._ready:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(_CREATE_TABLE)
                await db.commit()
            self._ready = True

    async def save(self, indicator: Indicator) -> None:
        """Persist an indicator snapshot (called after a successful live fetch)."""
        await self._ensure_ready()
        payload = indicator.model_dump_json()
        saved_at = datetime.now(tz=timezone.utc).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO cache (indicator_id, payload, saved_at) VALUES (?, ?, ?)",
                (indicator.id, payload, saved_at),
            )
            await db.commit()
        logger.debug("cache_saved", extra={"id": indicator.id})

    async def load(self, indicator_id: str) -> Indicator | None:
        """
        Return a cached Indicator tagged as STALE, or None if:
          - no entry exists
          - entry is older than TTL
        """
        await self._ensure_ready()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT payload, saved_at FROM cache WHERE indicator_id = ?",
                (indicator_id,),
            ) as cursor:
                row = await cursor.fetchone()

        if row is None:
            return None

        payload, saved_at_str = row
        saved_at = datetime.fromisoformat(saved_at_str)
        age = datetime.now(tz=timezone.utc) - saved_at

        if age > self.ttl:
            logger.warning(
                "cache_expired",
                extra={"id": indicator_id, "age_hours": round(age.total_seconds() / 3600, 1)},
            )
            return None

        indicator = Indicator.model_validate_json(payload)
        stale = indicator.model_copy(update={"status": FetchStatus.STALE})
        logger.info(
            "cache_hit",
            extra={"id": indicator_id, "age_hours": round(age.total_seconds() / 3600, 1)},
        )
        return stale

    async def save_many(self, indicators: list[Indicator]) -> None:
        for ind in indicators:
            await self.save(ind)

    async def load_many(self, ids: list[str]) -> dict[str, Indicator | None]:
        return {id_: await self.load(id_) for id_ in ids}
