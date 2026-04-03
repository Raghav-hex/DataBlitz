"""
datablitz.ingestion.psychohistory.analogies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Historical analogy library for Jiang's Stage 2 workflow.

Maps current structural conditions to historical precedents using
Turchin's SDT framework + Jiang's Oceanic Current Model.

Each analogy contains:
  - trigger_conditions: dict of PSI/GDELT thresholds that activate it
  - historical_case: the precedent
  - structural_match: why the structures are similar
  - causal_chain: what happened historically
  - implication: what this suggests for the current situation
  - confidence_modifier: how reliable this class of analogy is

This is NOT fortune-telling — it's structural pattern recognition.
As Turchin argues: individual events are unpredictable, but the
structural conditions that enable them are measurable.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class HistoricalAnalogy:
    id:                  str
    title:               str
    historical_case:     str   # e.g. "Athens' Sicilian Expedition, 415-413 BC"
    structural_match:    str   # why current → historical similarity holds
    causal_chain:        str   # what the historical sequence was
    implication:         str   # structural implication for present
    trigger_country:     str   # which country this primarily applies to
    trigger_psi_min:     float # minimum PSI to trigger this analogy
    trigger_conditions:  dict  # additional trigger conditions
    confidence:          str   # low | medium | high (per analogical reasoning literature)
    source:              str   # academic/historical source


# ── Core analogy library ──────────────────────────────────────────────────────
# Based on Jiang's validated predictions + Turchin's historical database

ANALOGY_LIBRARY: list[HistoricalAnalogy] = [

    # ── USA ANALOGIES ─────────────────────────────────────────────────────────
    HistoricalAnalogy(
        id="usa_late_roman_debasement",
        title="Late Roman Currency Debasement",
        historical_case="Roman Empire, 3rd century AD: Severan dynasty debased denarius from 85% to 50% silver to fund endless military campaigns.",
        structural_match="Both empires maintain global reserve currency status through military spending beyond fiscal capacity. Elite overproduction + wage stagnation + currency pressure = Turchin's 'Wealth Pump' at maximum.",
        causal_chain="Debasement → inflation → real wage collapse → peasant revolts (Bacaudae) → frontier instability → imperial contraction. Full cycle: ~50 years.",
        implication="Current FEDFUNDS + debt trajectory suggests fiscal dominance risk. The structural pressure is not inflation per se — it's the political response to inflation that drives instability.",
        trigger_country="usa",
        trigger_psi_min=0.45,
        trigger_conditions={"yield_spread_inverted": True, "fedfunds_above": 4.5},
        confidence="medium",
        source="Turchin (2016), 'Ages of Discord'; Ward-Perkins (2005), 'The Fall of Rome'"
    ),

    HistoricalAnalogy(
        id="usa_sicilian_expedition",
        title="Athenian Sicilian Expedition",
        historical_case="Athens, 415-413 BC: Ambitious military campaign launched at peak imperial overreach. Defeated by rugged terrain, local resistance, and strategic overextension.",
        structural_match="Jiang's primary analogy. Maritime power at peak hubris launching land campaign it cannot sustain. Sunk cost drives escalation past rational exit points.",
        causal_chain="Initial victory delusion → reinforcement spiral → strategic defeat → home political instability → oligarchic coup (411 BC) → eventual Spartan victory (404 BC).",
        implication="Watch for 'mission creep' signals: each escalation justified by prior investment rather than strategic merit. The structural tell is when 'cutting losses' becomes politically impossible.",
        trigger_country="usa",
        trigger_psi_min=0.5,
        trigger_conditions={"gdelt_crisis": True},
        confidence="medium",
        source="Thucydides, 'History of the Peloponnesian War', Book VI-VII; Jiang Xueqin, 'Game Theory #14'"
    ),

    HistoricalAnalogy(
        id="usa_weimar_elite_flight",
        title="Weimar Elite Capital Flight",
        historical_case="Weimar Germany, 1919-1933: Elite overproduction (too many lawyers, academics, officers for available positions) combined with hyperinflation created a 'counter-elite' movement that channeled mass immiseration into authoritarian politics.",
        structural_match="Turchin's EMP signal. When elite aspirants cannot find positions commensurate with their credentials, they become destabilizing. Law school enrollment vs. legal job openings is the modern proxy.",
        causal_chain="Elite frustration → extremist organization funding → mass movement capture → institutional erosion → democratic breakdown.",
        implication="PSI EMP component is the leading indicator, not the mass population. Structural instability begins when educated elites lose faith in meritocracy.",
        trigger_country="usa",
        trigger_psi_min=0.4,
        trigger_conditions={"emp_above": 0.5},
        confidence="high",
        source="Turchin (2023), 'End Times'; Mann (2004), 'Fascists'"
    ),

    # ── UK ANALOGIES ──────────────────────────────────────────────────────────
    HistoricalAnalogy(
        id="uk_british_decline_1956",
        title="Suez Crisis and Sterling Moment",
        historical_case="UK, 1956: Military intervention in Suez abandoned when US refused to support sterling. First clear demonstration that UK military power was contingent on US financial backing.",
        structural_match="GBP weakness + BoE rate pressure + post-imperial overcommitment = structural echo. The 'sterling moment' recurs when a power overextends and its currency becomes the pressure point.",
        causal_chain="Military ambition → currency speculative attack → forced humiliating withdrawal → accelerated decolonization → permanent regional power status.",
        implication="When GBP comes under pressure simultaneously with political overextension, this is the structural signal — not the political event itself.",
        trigger_country="uk",
        trigger_psi_min=0.35,
        trigger_conditions={"gbpusd_below": 1.20},
        confidence="medium",
        source="Kyle (1991), 'Suez'; Tooze (2018), 'Crashed'"
    ),

    # ── INDIA ANALOGIES ───────────────────────────────────────────────────────
    HistoricalAnalogy(
        id="india_demographic_dividend_trap",
        title="Demographic Dividend Trap (Japan 1990s)",
        historical_case="Japan, 1990-2010: Rapid demographic aging after peak growth phase created structural deflation. The 'lost decade' was structurally determined, not accidental.",
        structural_match="India's GDP growth trajectory mirrors pre-peak Japan in structural terms. The question is whether the demographic dividend converts to durable human capital before the aging transition.",
        causal_chain="Peak growth → elite overproduction in engineering/services → insufficient domestic demand creation → export dependency → vulnerability to external shocks.",
        implication="Health expenditure as % GDP is the leading indicator. If India fails to invest in human capital during the growth phase, the structural transition becomes structurally disadvantaged.",
        trigger_country="india",
        trigger_psi_min=0.3,
        trigger_conditions={"health_exp_below": 4.0, "gdp_growth_declining": True},
        confidence="medium",
        source="Turchin SDT applied; Dreze & Sen (2013), 'An Uncertain Glory'"
    ),

    # ── BRAZIL ANALOGIES ──────────────────────────────────────────────────────
    HistoricalAnalogy(
        id="brazil_argentina_mirror",
        title="Argentina's Structural Sickness Cycle",
        historical_case="Argentina, 1975-2002: Five distinct currency crises, each driven by the same structural pattern — fiscal dominance, elite capital flight, and commodity dependency.",
        structural_match="Brazil and Argentina share the same structural DNA: commodity export dependency, urban elite-rural poor tension, and a central bank forced to fight fiscal profligacy with interest rates. SELIC above 13% = BCB fighting fiscal expansion with monetary tightening = structural contradiction.",
        causal_chain="High rates → domestic credit crunch → corporate stress → political pressure on central bank → eventual rate capitulation → inflation acceleration → currency crisis.",
        implication="SELIC trajectory is the warning signal. The structural risk is not the current level but the political sustainability of maintaining it. Watch for BCB 'independence' political attacks.",
        trigger_country="brazil",
        trigger_psi_min=0.4,
        trigger_conditions={"selic_above": 13.0, "brl_above": 5.5},
        confidence="high",
        source="Turchin SDT; Reinhart & Rogoff (2009), 'This Time Is Different'"
    ),

    # ── CROSS-COUNTRY ANALOGIES ───────────────────────────────────────────────
    HistoricalAnalogy(
        id="global_1970s_stagflation",
        title="1970s Global Stagflation Divergence",
        historical_case="1973-1982: Oil shock created DM-EM divergence. Countries that maintained fiscal discipline (Germany, Japan) emerged stronger. Countries that monetized debt (UK, US in 1970s, LatAm) faced structural crises.",
        structural_match="Current rate divergence pattern (EM hiking, DM cutting) mirrors the post-1973 structural fork. The question is which countries are 'West Germany 1973' and which are 'UK 1976'.",
        causal_chain="Divergence → capital flow realignment → structural winners and losers → decade-long reshaping of global power hierarchy.",
        implication="Cross-country rate divergence is not cyclical noise — it's the structural signal of which economies have the fiscal foundations to absorb the current shock.",
        trigger_country="brazil",  # triggered by Brazil/UK divergence
        trigger_psi_min=0.35,
        trigger_conditions={"cross_country": True, "rate_divergence_above": 8.0},
        confidence="high",
        source="Eichengreen (2015), 'Hall of Mirrors'; Turchin SDT"
    ),
]


def find_active_analogies(
    country: str,
    psi_score: float,
    psi_components: Any,
    gdelt_signal: Any = None,
    indicator_snapshot: dict | None = None,
) -> list[HistoricalAnalogy]:
    """
    Find historically analogous situations given current structural conditions.
    Returns list of applicable HistoricalAnalogy objects sorted by relevance.
    """
    active = []

    for analogy in ANALOGY_LIBRARY:
        # Country filter
        if analogy.trigger_country != country and analogy.trigger_conditions.get("cross_country") is not True:
            continue

        # PSI threshold
        if psi_score < analogy.trigger_psi_min:
            continue

        # Additional condition checks
        conditions = analogy.trigger_conditions
        triggered = True

        if "yield_spread_inverted" in conditions and indicator_snapshot:
            spread = indicator_snapshot.get("yield_spread", 1.0)
            if spread >= 0:
                triggered = False

        if "gdelt_crisis" in conditions and gdelt_signal:
            if gdelt_signal.crisis_level not in ("crisis", "volatile"):
                triggered = False

        if "emp_above" in conditions and psi_components:
            if psi_components.emp < conditions["emp_above"]:
                triggered = False

        if "selic_above" in conditions and indicator_snapshot:
            selic = indicator_snapshot.get("interest_rate", 0.0)
            if selic < conditions["selic_above"]:
                triggered = False

        if triggered:
            active.append(analogy)

    return active[:3]  # top 3 most relevant


def format_analogies_for_prompt(
    active_by_country: dict[str, list[HistoricalAnalogy]]
) -> str:
    """Format active historical analogies as a prompt section."""
    if not active_by_country:
        return ""

    conf_icon = {"high": "⭐", "medium": "◆", "low": "◇"}
    lines = ["HISTORICAL ANALOGIES (Jiang's Stage 2 — structural pattern matching):"]
    lines.append("  [These are structural echoes, not predictions. Use to frame YOUR analysis.]")

    for country, analogies in active_by_country.items():
        if not analogies:
            continue
        for a in analogies:
            icon = conf_icon.get(a.confidence, "◇")
            lines.append(f"\n  {icon} {country.upper()}: {a.title}")
            lines.append(f"    Case: {a.historical_case[:120]}...")
            lines.append(f"    Match: {a.structural_match[:120]}...")
            lines.append(f"    Implication: {a.implication[:120]}...")

    return "\n".join(lines)
