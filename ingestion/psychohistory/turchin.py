"""
datablitz.ingestion.psychohistory.turchin
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Peter Turchin's Structural-Demographic Theory (SDT) implementation.

Political Stress Index: PSI = MMP × EMP × SFD

Where:
  MMP = Mass Mobilization Potential  (immiseration + youth unemployment)
  EMP = Elite Mobilization Potential (inequality proxy + elite overproduction)
  SFD = State Fiscal Distress        (debt trajectory + fiscal capacity)

We derive these from indicators we already collect:
  - FRED: UNRATE, GDP, FEDFUNDS, T10Y2Y (USA)
  - BCB:  SELIC, IPCA, UNEMPLOYMENT (Brazil)
  - ONS:  CPIH, UNEMPLOYMENT (UK)
  - World Bank: GDP_GROWTH, UNEMPLOYMENT, HEALTH_EXP (India)
  - Stocks: equity indices as elite wealth proxy

PSI output: 0.0–1.0 normalized score per country
  > 0.7 = HIGH INSTABILITY RISK (historically precedes discord)
  0.4–0.7 = ELEVATED
  < 0.4 = STABLE

Reference: Turchin, P. "Ages of Discord" (2016); CrisisDB methodology
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class PSIComponents:
    """Decomposed Political Stress Index for one country."""
    country:    str
    mmp:        float   # Mass Mobilization Potential (0-1)
    emp:        float   # Elite Mobilization Potential (0-1)
    sfd:        float   # State Fiscal Distress (0-1)
    psi:        float   # Final score MMP × EMP × SFD (0-1, cube root normalized)
    signals:    list[str] = field(default_factory=list)  # human-readable drivers
    level:      str = "stable"   # stable | elevated | high | critical


# ── Indicator ID mappings per country ────────────────────────────────────────
# Maps our Indicator IDs to Turchin SDT components

INDICATOR_MAP = {
    "usa": {
        "unemployment":   "usa.fred.UNRATE",
        "gdp":            "usa.fred.GDP",
        "interest_rate":  "usa.fred.FEDFUNDS",
        "yield_spread":   "usa.fred.T10Y2Y",
        "equity":         "usa.stocks.SPY",
        "cpi":            "usa.fred.CPIAUCSL",
    },
    "uk": {
        "unemployment":   "uk.ons.UNEMPLOYMENT",
        "interest_rate":  "uk.boe.BASE_RATE",
        "cpi":            "uk.ons.CPIH",
        "equity":         "uk.stocks.EWU",
        "fx":             "uk.stocks.GBPUSDX",
    },
    "india": {
        "gdp_growth":     "india.wb.GDP_GROWTH",
        "unemployment":   "india.wb.UNEMPLOYMENT",
        "inflation":      "india.wb.INFLATION",
        "health_exp":     "india.wb.HEALTH_EXP",
        "equity":         "india.stocks.INDA",
    },
    "brazil": {
        "unemployment":   "brazil.bcb.UNEMPLOYMENT",
        "interest_rate":  "brazil.bcb.SELIC",
        "inflation":      "brazil.bcb.IPCA",
        "fx":             "brazil.bcb.USDBRL",
        "equity":         "brazil.stocks.EWZ",
    },
}

# Historical "normal" ranges for normalization (median of stable periods)
BASELINES = {
    "usa":    {"unemployment": 4.5, "interest_rate": 2.5, "cpi": 2.5,  "yield_spread": 1.5},
    "uk":     {"unemployment": 4.8, "interest_rate": 1.5, "cpi": 2.0},
    "india":  {"gdp_growth":   6.5, "unemployment":  4.0, "inflation": 5.0},
    "brazil": {"unemployment": 9.0, "interest_rate": 8.0, "inflation": 4.5, "fx": 4.5},
}


def compute_psi(country: str, indicators: list[dict]) -> PSIComponents | None:
    """
    Compute Turchin PSI for a country using its current indicator snapshot.

    indicators: list of Indicator model dicts (from model_dump())
    Returns PSIComponents or None if insufficient data.
    """
    # Build lookup by indicator ID
    ind_by_id: dict[str, dict] = {}
    for ind in indicators:
        obs = ind.get("observations", [])
        if obs:
            ind_by_id[ind["id"]] = {
                "latest":    obs[-1]["value"],
                "prev":      obs[-2]["value"] if len(obs) >= 2 else obs[-1]["value"],
                "trend":     _trend(obs),
                "name":      ind.get("name", ""),
            }

    id_map   = INDICATOR_MAP.get(country, {})
    baseline = BASELINES.get(country, {})
    signals  = []

    if not ind_by_id:
        return None

    # ── MMP: Mass Mobilization Potential ─────────────────────────────────────
    # High unemployment + rising prices = immiseration = high MMP
    mmp_components = []

    unemp_id = id_map.get("unemployment")
    if unemp_id and unemp_id in ind_by_id:
        unemp = ind_by_id[unemp_id]["latest"]
        base_unemp = baseline.get("unemployment", 5.0)
        unemp_score = min(1.0, max(0.0, (unemp - base_unemp * 0.8) / (base_unemp * 1.5)))
        mmp_components.append(unemp_score)
        if unemp_score > 0.5:
            signals.append(f"Unemployment at {unemp:.1f}% exceeds stable baseline ({base_unemp}%) — immiseration signal")

    for cpi_key in ("cpi", "inflation"):
        cpi_id = id_map.get(cpi_key)
        if cpi_id and cpi_id in ind_by_id:
            cpi = ind_by_id[cpi_id]["latest"]
            base_cpi = baseline.get(cpi_key, 3.0)
            cpi_score = min(1.0, max(0.0, (cpi - base_cpi) / (base_cpi * 3)))
            mmp_components.append(cpi_score)
            if cpi_score > 0.4:
                signals.append(f"Inflation {cpi:.1f}% squeezing real wages — wealth pump accelerating")
            break

    mmp = sum(mmp_components) / len(mmp_components) if mmp_components else 0.3

    # ── EMP: Elite Mobilization Potential ────────────────────────────────────
    # Yield curve inversion + high equity = elite/financial stress divergence
    # Proxy: interest rate deviation from neutral + equity volatility
    emp_components = []

    rate_id = id_map.get("interest_rate")
    if rate_id and rate_id in ind_by_id:
        rate = ind_by_id[rate_id]["latest"]
        base_rate = baseline.get("interest_rate", 3.0)
        # Very high rates = financial elite stress (debt servicing)
        rate_score = min(1.0, max(0.0, (rate - base_rate) / (base_rate * 3)))
        emp_components.append(rate_score)
        if rate_score > 0.5:
            signals.append(f"Interest rate {rate:.2f}% far above neutral ({base_rate}%) — elite financial stress / debt squeeze")

    yield_id = id_map.get("yield_spread")
    if yield_id and yield_id in ind_by_id:
        spread = ind_by_id[yield_id]["latest"]
        if spread < 0:
            emp_components.append(0.9)
            signals.append(f"Yield curve inverted ({spread:.2f}%) — Turchin 'elite trap': markets pricing recession, but fiscal commitments persist")
        elif spread < 0.5:
            emp_components.append(0.5)

    # Equity trend as elite confidence proxy (declining equity = elite distress)
    equity_id = id_map.get("equity")
    if equity_id and equity_id in ind_by_id:
        eq_trend = ind_by_id[equity_id]["trend"]
        if eq_trend < -5:
            emp_components.append(0.7)
            signals.append(f"Equity markets declining ({eq_trend:.1f}% trend) — elite capital under pressure")
        elif eq_trend > 15:
            # Extremely high equity = speculative excess = instability signal (Turchin)
            emp_components.append(0.4)

    emp = sum(emp_components) / len(emp_components) if emp_components else 0.25

    # ── SFD: State Fiscal Distress ─────────────────────────────────────────────
    # GDP growth deviation + FX pressure + health spending gaps
    sfd_components = []

    gdp_id = id_map.get("gdp") or id_map.get("gdp_growth")
    if gdp_id and gdp_id in ind_by_id:
        gdp_trend = ind_by_id[gdp_id]["trend"]
        base_growth = baseline.get("gdp_growth", 2.5)
        if gdp_trend < 0:
            sfd_components.append(0.8)
            signals.append(f"GDP contracting ({gdp_trend:.1f}%) — state revenue base shrinking")
        elif gdp_trend < base_growth * 0.5:
            sfd_components.append(0.5)

    fx_id = id_map.get("fx")
    if fx_id and fx_id in ind_by_id:
        fx = ind_by_id[fx_id]["latest"]
        base_fx = baseline.get("fx", 1.0)
        if base_fx > 0:
            fx_depreciation = (fx - base_fx) / base_fx
            if fx_depreciation > 0.3:
                sfd_components.append(0.7)
                signals.append(f"Currency depreciated {fx_depreciation*100:.0f}% from baseline — import costs rising, sovereign pressure")

    health_id = id_map.get("health_exp")
    if health_id and health_id in ind_by_id:
        health_pct = ind_by_id[health_id]["latest"]
        if health_pct < 3.5:
            sfd_components.append(0.6)
            signals.append(f"Health expenditure only {health_pct:.1f}% of GDP (BRICS low) — human capital underinvestment = long-run fiscal drag")

    sfd = sum(sfd_components) / len(sfd_components) if sfd_components else 0.2

    # ── Final PSI ──────────────────────────────────────────────────────────────
    # Turchin multiplies: PSI = MMP × EMP × SFD
    # We cube-root normalize to 0-1 for readability while preserving the multiplicative logic
    raw_psi = mmp * emp * sfd
    psi = raw_psi ** (1/3)  # cube root brings back to 0-1 scale

    # Classify level
    if psi > 0.65:
        level = "critical"
    elif psi > 0.5:
        level = "high"
    elif psi > 0.35:
        level = "elevated"
    else:
        level = "stable"

    return PSIComponents(
        country=country, mmp=round(mmp, 3), emp=round(emp, 3),
        sfd=round(sfd, 3), psi=round(psi, 3), signals=signals, level=level,
    )


def _trend(observations: list[dict]) -> float:
    """Simple % change over last 6 observations (or available)."""
    obs = observations[-6:]
    if len(obs) < 2:
        return 0.0
    first, last = obs[0]["value"], obs[-1]["value"]
    if first == 0:
        return 0.0
    return round(((last - first) / abs(first)) * 100, 2)


def format_psi_for_prompt(psi_scores: dict[str, PSIComponents]) -> str:
    """Format PSI scores as a compact prompt section."""
    if not psi_scores:
        return ""

    icon_map = {"critical": "🔴", "high": "🟠", "elevated": "🟡", "stable": "🟢"}
    lines = ["TURCHIN POLITICAL STRESS INDEX (PSI = MMP × EMP × SFD):"]
    lines.append("  [Measures structural instability risk — NOT short-term sentiment]")

    for country, psi in sorted(psi_scores.items(), key=lambda x: x[1].psi, reverse=True):
        icon = icon_map.get(psi.level, "⚪")
        lines.append(
            f"  {icon} {country.upper()}: PSI={psi.psi:.2f} [{psi.level.upper()}]"
            f"  (MMP={psi.mmp:.2f} | EMP={psi.emp:.2f} | SFD={psi.sfd:.2f})"
        )
        for sig in psi.signals[:2]:  # top 2 signals per country
            lines.append(f"    → {sig}")

    return "\n".join(lines)
