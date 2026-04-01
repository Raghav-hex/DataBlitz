"""
datablitz.ingestion.alerts
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Threshold alert detector.

Checks indicator values against configurable thresholds and injects
alert flags into the AI prompt. Alerts signal conditions like:
  - "Brazil SELIC crossed 14% (historically associated with credit stress)"
  - "India PM2.5 > 150 μg/m³ (severe pollution threshold)"
  - "UK unemployment above 5% for first time since 2021"

These are injected as a clear section in the prompt so the AI
explicitly knows which numbers are threshold-crossing events worth flagging.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable

logger = logging.getLogger(__name__)


@dataclass
class Alert:
    indicator_id:  str
    message:       str
    level:         str   # "critical" | "warning" | "info"
    current_value: float
    threshold:     float


# ── Threshold configuration ───────────────────────────────────────────────────
# Each rule: (indicator_id_pattern, condition_fn, threshold, message, level)
# condition_fn receives (current_value, threshold) and returns True if alert fires.

_ABOVE = lambda v, t: v > t
_BELOW = lambda v, t: v < t

ALERT_RULES: list[tuple[str, Callable, float, str, str]] = [
    # USA
    ("usa.fred.UNRATE",   _ABOVE, 5.0,  "US unemployment exceeded 5% — approaching recessionary territory", "warning"),
    ("usa.fred.UNRATE",   _ABOVE, 6.0,  "US unemployment above 6% — historical recession signal", "critical"),
    ("usa.fred.FEDFUNDS", _ABOVE, 5.5,  "Fed funds above 5.5% — highest since pre-GFC era", "info"),
    ("usa.fred.FEDFUNDS", _BELOW, 1.0,  "Fed funds below 1% — emergency-level rate", "warning"),
    ("usa.fred.T10Y2Y",   _BELOW, 0.0,  "Yield curve inverted — historically precedes recession by 6-18 months", "critical"),

    # UK
    ("uk.ons.UNEMPLOYMENT", _ABOVE, 5.5, "UK unemployment above 5.5% — 5-year high territory", "warning"),
    ("uk.ons.CPIH",          _ABOVE, 5.0, "UK CPIH above 5% — double the BoE target", "critical"),
    ("uk.ons.CPIH",          _BELOW, 1.0, "UK CPIH below 1% — deflation risk territory", "warning"),
    ("uk.boe.BASE_RATE",     _ABOVE, 5.0, "BoE base rate above 5% — 15-year high", "info"),

    # India
    ("india.wb.GDP_GROWTH",  _BELOW, 5.0,  "India GDP growth below 5% — significant slowdown from trend", "critical"),
    ("india.wb.GDP_GROWTH",  _ABOVE, 8.0,  "India GDP growth above 8% — exceptional expansion", "info"),
    ("india.openaq.PM25",    _ABOVE, 150.0, "India PM2.5 > 150 μg/m³ — 30x WHO annual limit (severe)", "critical"),
    ("india.openaq.PM25",    _ABOVE, 100.0, "India PM2.5 > 100 μg/m³ — WHO very unhealthy threshold", "warning"),

    # Brazil
    ("brazil.bcb.SELIC",     _ABOVE, 13.0, "Brazil SELIC above 13% — highest since 2006 credit cycle", "warning"),
    ("brazil.bcb.SELIC",     _ABOVE, 14.0, "Brazil SELIC above 14% — extreme tightening territory", "critical"),
    ("brazil.bcb.IPCA",      _ABOVE, 5.0,  "Brazil IPCA above 5% — 60% above BCB's 3.25% target", "warning"),
    ("brazil.bcb.IPCA",      _ABOVE, 8.0,  "Brazil IPCA above 8% — approaching crisis-level inflation", "critical"),
    ("brazil.bcb.USDBRL",    _ABOVE, 6.0,  "USD/BRL above 6.0 — weakest real since COVID-era crisis", "critical"),
]


def check_alerts(global_digest_dict: dict) -> list[Alert]:
    """
    Run all threshold checks against the current GlobalDigest.
    Returns list of Alert objects that fired.
    """
    alerts: list[Alert] = []
    digests = global_digest_dict.get("digests", [])

    for country_digest in digests:
        for ind in country_digest.get("indicators", []):
            ind_id = ind.get("id", "")
            obs    = ind.get("observations", [])
            if not obs:
                continue
            latest_value = obs[-1]["value"]

            for rule_id, condition, threshold, message, level in ALERT_RULES:
                # Support suffix matching (e.g. "usa.fred.UNRATE" matches exactly)
                if ind_id != rule_id:
                    continue
                if condition(latest_value, threshold):
                    alerts.append(Alert(
                        indicator_id=ind_id,
                        message=message,
                        level=level,
                        current_value=latest_value,
                        threshold=threshold,
                    ))
                    logger.info(
                        f"Alert fired [{level}]: {ind_id} = {latest_value:.3f} "
                        f"(threshold {threshold})"
                    )

    return alerts


def format_alerts_for_prompt(alerts: list[Alert]) -> str:
    """Format alerts as a clearly marked prompt section."""
    if not alerts:
        return ""

    # Sort by severity: critical → warning → info
    order = {"critical": 0, "warning": 1, "info": 2}
    alerts = sorted(alerts, key=lambda a: order.get(a.level, 3))

    lines = ["⚠ THRESHOLD ALERTS (these are significant — address explicitly in your analysis):"]
    for a in alerts:
        icon  = "🔴" if a.level == "critical" else "🟡" if a.level == "warning" else "🔵"
        lines.append(
            f"  {icon} [{a.level.upper()}] {a.indicator_id}: "
            f"{a.current_value:.3f} (threshold: {a.threshold})\n"
            f"     {a.message}"
        )

    return "\n".join(lines)
