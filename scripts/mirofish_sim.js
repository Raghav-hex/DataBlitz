#!/usr/bin/env node
/**
 * scripts/mirofish_sim.js
 * ───────────────────────
 * Standalone MiroFish-style multi-agent social simulation.
 *
 * Feed it any DataBlitz narrative JSON and a prediction question.
 * Spawns N agent "personas" who each react from their structural position,
 * then synthesizes the emergent collective pattern.
 *
 * This is a lightweight implementation of MiroFish's core loop:
 *   Document → Agent personas → Parallel reactions → Synthesis
 *
 * Uses the same Puter/Claude/Grok/Gemini fallback chain as the main engine.
 *
 * Usage:
 *   node scripts/mirofish_sim.js \
 *     --input data/narrative_latest.json \
 *     --question "How will investors react to Brazil SELIC at 14.25%?" \
 *     --rounds 2 \
 *     --output data/mirofish_report.json
 *
 *   node scripts/mirofish_sim.js --demo  # runs with built-in demo data
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { tryWithFallback } from '../ai_engine/fallback_client.js';

const __dirname = dirname(fileURLToPath(import.meta.url));

// ── Agent personas — deliberately diverse structural positions ─────────────
const AGENT_PERSONAS = [
  {
    id: 'em_fund_manager',
    role: 'Emerging Markets Fund Manager (London-based, $2B AUM)',
    focus: 'EM carry trade, BRL/USD positioning, sovereign spread dynamics',
    reaction_lens: 'capital allocation and risk-adjusted returns',
  },
  {
    id: 'brazil_pension_cio',
    role: 'Brazilian Domestic Pension Fund CIO (São Paulo, R$50B fund)',
    focus: 'BRL assets, domestic inflation hedging, SELIC-indexed bonds',
    reaction_lens: 'fiduciary duty to Brazilian retirees, long-horizon returns',
  },
  {
    id: 'us_macro_analyst',
    role: 'US Macro Hedge Fund Analyst (New York, specializes in rate divergence)',
    focus: 'Fed/EM rate spread, USD liquidity, cross-country contagion',
    reaction_lens: 'systematic macro trades and relative value',
  },
  {
    id: 'brazil_corporate_cfo',
    role: 'CFO of a Brazilian industrial company (USD-denominated debt, exports to Europe)',
    focus: 'FX hedging cost, refinancing risk, export competitiveness',
    reaction_lens: 'operational survival and balance sheet risk',
  },
  {
    id: 'imf_mission_chief',
    role: 'IMF Mission Chief for Latin America',
    focus: 'Systemic risk, debt sustainability, contagion to other EM economies',
    reaction_lens: 'financial stability and policy recommendations',
  },
  {
    id: 'india_tech_ceo',
    role: 'Indian Tech CEO (listed company, US revenue, monitoring global macro)',
    focus: 'USD/INR, US tech demand, global risk appetite',
    reaction_lens: 'business planning and investor communications',
  },
];

// ── Core simulation ────────────────────────────────────────────────────────

async function runSimulation(seedText, question, rounds = 1) {
  console.log('\n╔══════════════════════════════════════════════════════════╗');
  console.log('║         DataBlitz MiroFish-Style Simulation              ║');
  console.log('╚══════════════════════════════════════════════════════════╝\n');
  console.log(`Question: "${question}"`);
  console.log(`Agents:   ${AGENT_PERSONAS.length}`);
  console.log(`Rounds:   ${rounds}\n`);

  const allRoundOutputs = [];

  for (let round = 1; round <= rounds; round++) {
    console.log(`─── Round ${round}/${rounds} ───────────────────────────────────────────`);

    // Phase 1: parallel agent reactions
    const reactionTasks = AGENT_PERSONAS.map(async (agent) => {
      const contextBlock = round === 1
        ? `SEED MATERIAL:\n${seedText}`
        : `SEED MATERIAL:\n${seedText}\n\nPREVIOUS ROUND SYNTHESIS:\n${allRoundOutputs[round - 2]?.synthesis ?? ''}`;

      const prompt = `You are a ${agent.role}.
Your analytical focus: ${agent.focus}
You evaluate situations through the lens of: ${agent.reaction_lens}

${contextBlock}

PREDICTION QUESTION: ${question}

In 80 words, give your REACTION from your structural position:
- What does this data mean for YOUR situation specifically?
- What action are you considering in the next 30 days?
- What are you most uncertain about?

Be specific — use exact numbers from the seed material. Stay in character.`;

      try {
        const result = await tryWithFallback(prompt, { maxTokens: 200, temperature: 0.75 });
        return { agent, text: result.text, provider: result.provider };
      } catch (err) {
        return { agent, text: `[${agent.role} failed to respond: ${err.message}]`, provider: 'failed' };
      }
    });

    process.stdout.write('Running agents');
    const reactions = await Promise.all(reactionTasks.map(async (t, i) => {
      const r = await t;
      process.stdout.write('.');
      return r;
    }));
    console.log(' done\n');

    // Print reactions
    for (const { agent, text } of reactions) {
      console.log(`[${agent.role}]`);
      console.log(text.trim());
      console.log();
    }

    // Phase 2: synthesis of emergent pattern
    const reactionsBlock = reactions
      .map(r => `[${r.agent.role}]\n${r.text}`)
      .join('\n\n');

    const synthPrompt = `You are a MiroFish ReportAgent analyzing a multi-agent social simulation.

PREDICTION QUESTION: "${question}"

AGENT REACTIONS (${reactions.length} diverse actors):
${reactionsBlock}

Synthesize the EMERGENT COLLECTIVE PATTERN in 150 words:

1. CONSENSUS: What direction are most actors moving?
2. FRACTURE POINTS: Where do actors diverge most sharply? (This is where volatility hides)
3. FEEDBACK LOOP: How do these reactions affect each other? (EM fund manager's move affects CFO's hedge cost, etc.)
4. 30-DAY PROBABILITY: Given these collective reactions, what is the highest-probability market/economic development?
5. CONTRARIAN SIGNAL: Which one actor is swimming against the current? Why does that matter?`;

    const synthResult = await tryWithFallback(synthPrompt, { maxTokens: 400, temperature: 0.65 });

    console.log(`═══ Round ${round} Emergent Pattern ══════════════════════════════════`);
    console.log(synthResult.text);
    console.log();

    allRoundOutputs.push({
      round,
      reactions: reactions.map(r => ({ role: r.agent.role, text: r.text, provider: r.provider })),
      synthesis: synthResult.text,
    });
  }

  return allRoundOutputs;
}


// ── Seed extraction from DataBlitz narrative ──────────────────────────────

function extractSeedFromNarrative(narrativePath) {
  try {
    const narrative = JSON.parse(readFileSync(narrativePath, 'utf-8'));
    const main = narrative.main_narrative ?? '';
    const psycho = narrative.psychohistory ?? '';
    const psi = narrative.meta?.psi_computed ? '[PSI computed this run]' : '';

    // Extract headline + big three summary as seed
    const lines = main.split('\n');
    const headline = lines.find(l => l.trim() && !l.startsWith('#')) ?? '';
    const seedLines = lines.slice(0, 60).join('\n');

    return `DataBlitz Digest ${narrative.run_id ?? ''}:\n${seedLines}\n${psycho ? '\nSTRUCTURAL CONTEXT:\n' + psycho.slice(0, 400) : ''}`;
  } catch (err) {
    return null;
  }
}


// ── Demo seed (used when --demo flag or no narrative found) ───────────────

const DEMO_SEED = `DataBlitz Weekly Digest 2026-W13:

HEADLINE: Brazil's SELIC hits 14.25% as US-UK rate cuts diverge sharply

KEY DATA:
- Brazil SELIC: 14.25% (highest since 2006, +350bps since Sep 2025)
- Brazil IPCA inflation: 5.48% vs 3.25% target
- USD/BRL: 5.90 (weakest since COVID May 2020)
- US Federal Funds Rate: 4.33% (3rd consecutive hold)
- US Unemployment: 4.2% (18-month high)
- UK BoE base rate: 4.25% (4th cut in 6 months)
- UK CPIH: 2.9% (first sub-3% since 2024)
- India GDP growth: 6.4% (down from 8.2%)
- India PM2.5: 89.7 μg/m³ (18x WHO limit)

PSI STRUCTURAL STRESS:
- Brazil: ELEVATED (SFD=0.70, currency -31% from baseline)
- USA: ELEVATED (yield curve flat, unemployment rising)
- UK: STABLE (inflation declining, cuts underway)
- India: STABLE (growth leading G20, air quality risk)

ACTIVE HISTORICAL ANALOG:
- Brazil-Argentina structural cycle (HIGH confidence): 
  Same pattern — fiscal dominance + high rates + political pressure on BCB.
  Historical outcome: eventual rate capitulation + inflation acceleration.`;


// ── Main entry point ─────────────────────────────────────────────────────

const args = process.argv.slice(2);
const isDemo       = args.includes('--demo');
const inputIdx     = args.indexOf('--input');
const questionIdx  = args.indexOf('--question');
const roundsIdx    = args.indexOf('--rounds');
const outputIdx    = args.indexOf('--output');

const inputPath    = inputIdx    >= 0 ? args[inputIdx + 1]    : resolve(__dirname, '../data/narrative_latest.json');
const question     = questionIdx >= 0 ? args[questionIdx + 1] : 'How will key market actors respond to Brazil SELIC at 14.25% and what are the 30-day capital flow implications?';
const rounds       = roundsIdx   >= 0 ? parseInt(args[roundsIdx + 1], 10) : 2;
const outputPath   = outputIdx   >= 0 ? args[outputIdx + 1]   : resolve(__dirname, '../data/mirofish_report.json');

let seed = DEMO_SEED;
if (!isDemo && existsSync(inputPath)) {
  seed = extractSeedFromNarrative(inputPath) ?? DEMO_SEED;
  console.log(`Seed loaded from: ${inputPath}`);
} else {
  console.log('Using built-in demo seed (run with --input data/narrative_latest.json for live data)');
}

runSimulation(seed, question, rounds)
  .then(outputs => {
    const report = {
      question,
      seed_chars:  seed.length,
      agents:      AGENT_PERSONAS.length,
      rounds:      outputs.length,
      generated:   new Date().toISOString(),
      rounds_data: outputs,
      final_synthesis: outputs[outputs.length - 1]?.synthesis ?? '',
    };

    writeFileSync(outputPath, JSON.stringify(report, null, 2));
    console.log(`\n✓ Report saved to ${outputPath}`);
    console.log('\n╔══════════════════════════════════════════════════════════╗');
    console.log('║                FINAL SYNTHESIS                           ║');
    console.log('╚══════════════════════════════════════════════════════════╝\n');
    console.log(report.final_synthesis);
  })
  .catch(err => {
    console.error('Simulation failed:', err);
    process.exit(1);
  });
