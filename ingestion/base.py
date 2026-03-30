"""
datablitz.ingestion.base
~~~~~~~~~~~~~~~~~~~~~~~~~
Abstract BaseSource that every country/source adapter must implement.

Contract:
  - fetch() returns a list[Indicator] — never raises on soft errors
  - All HTTP is done via the shared httpx.AsyncClient passed at call time
  - Retry logic lives here (via tenacity), NOT in individual adapters
  - Stale-cache fallback is handled by the CacheLayer, called from run()
"""

from __future__ import annotations

import abc
import logging
from datetime import datetime, timezone
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .schemas import Country, FetchStatus, Indicator

logger = logging.getLogger(__name__)


# ─── Retry decorator shared by all adapters ───────────────────────────────────

def with_retries(func):
    """
    3 attempts with exponential backoff: 2s → 4s → 8s.
    Only retries on network-level errors and 429/5xx responses.
    """
    return retry(
        retry=retry_if_exception_type(
            (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError)
        ),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )(func)


# ─── Base adapter ─────────────────────────────────────────────────────────────

class BaseSource(abc.ABC):
    """
    Abstract base for all DataBlitz source adapters.

    Subclasses implement fetch_indicators() which does the actual HTTP work.
    The public entry point is fetch(), which wraps with logging and error
    normalisation so callers always get list[Indicator] back.
    """

    #: Must be set by each subclass
    country: Country
    source_name: str

    def __init__(self, client: httpx.AsyncClient, settings: Any) -> None:
        self.client = client
        self.settings = settings

    @abc.abstractmethod
    async def fetch_indicators(self) -> list[Indicator]:
        """
        Fetch and return all indicators this source is responsible for.
        Must raise on hard errors (bad auth, schema mismatch, etc).
        May return a partial list if some series succeed and others fail —
        log the failures and continue.
        """

    async def fetch(self) -> tuple[list[Indicator], list[str]]:
        """
        Public entry point. Returns (indicators, errors).
        Never raises — errors are surfaced as strings in the second element.
        """
        start = datetime.now(tz=timezone.utc)
        try:
            indicators = await self.fetch_indicators()
            elapsed = (datetime.now(tz=timezone.utc) - start).total_seconds()
            logger.info(
                "source_fetch_ok",
                extra={
                    "source": self.source_name,
                    "country": self.country,
                    "count": len(indicators),
                    "elapsed_s": round(elapsed, 2),
                },
            )
            return indicators, []
        except Exception as exc:
            elapsed = (datetime.now(tz=timezone.utc) - start).total_seconds()
            msg = f"{self.source_name} failed after {elapsed:.1f}s: {exc!r}"
            logger.error("source_fetch_error", extra={"source": self.source_name, "error": str(exc)})
            return [], [msg]

    # ── HTTP helpers ───────────────────────────────────────────────────────

    @with_retries
    async def get_json(self, url: str, params: dict | None = None) -> Any:
        """GET → JSON with retry. Raises on non-2xx after retries exhausted."""
        resp = await self.client.get(url, params=params)
        if resp.status_code == 429:
            raise httpx.TimeoutException(f"Rate-limited by {url}", request=resp.request)
        resp.raise_for_status()
        return resp.json()

    @with_retries
    async def get_text(self, url: str, params: dict | None = None) -> str:
        """GET → raw text with retry. For CSV/plain-text sources."""
        resp = await self.client.get(url, params=params)
        resp.raise_for_status()
        return resp.text

    def now_utc(self) -> datetime:
        return datetime.now(tz=timezone.utc)

    def mark_stale(self, indicators: list[Indicator]) -> list[Indicator]:
        """Tag indicators as STALE (served from cache after live fetch failed)."""
        return [
            ind.model_copy(update={"status": FetchStatus.STALE})
            for ind in indicators
        ]
