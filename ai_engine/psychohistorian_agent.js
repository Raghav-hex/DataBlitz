/**
 * datablitz/ai_engine/psychohistorian_agent.js
 * ──────────────────────────────────────────────
 * The Synthetic Psychohistorian agent.
 *
 * Implements Jiang Xueqin's 3-stage predictive workflow:
 *   Stage 1: Structural Sickness Detection
 *            (PSI scores, GDELT tone, Oceanic Current positioning)
 *   Stage 2: Historical Pattern Matching
 *            (Active historical analogies from analogy library)
 *   Stage 3: Game Theory + Sunk Cost Analysis
 *            (Actor incentive modeling, decision tree analysis)
 *
 * Combined with Turchin's quantitative SDT framework.
 * Output: structured prediction section injected into the final narrative.
 *
 * Key insight from the PDFs: this agent does NOT predict events.
 * It identifies STRUCTURAL CONDITIONS that make certain outcomes
 * more or less probable. The distinction matters for accuracy.
 *
 * Architecture role: runs AFTER the 4 country analysts, BEFORE the
 * synthesizer. Its output gets appended to the synthesis prompt.
 */

import { tryWithFallback } from './fallback_client.js';

export async function runPsychohistorianAgent(digest, enrichment, ragCtx, psychoCtx) {
  console.log('[psychohistorian] Starting structural analysis...');

  const prompt = buildPsychohistorianPrompt(digest, enrichment, ragCtx, psychoCtx);

  try {
    const result = await tryWithFallback(prompt, {
      maxTokens: 2048,
      temperature: 0.6,  // Lower temperature — more analytical, less creative
    });
    console.log(`[psychohistorian] Done (${result.text.length} chars via ${result.provider})`);
    return { text: result.text, provider: result.provider };
  } catch (err) {
    console.error(`[psychohistorian] Failed: ${err.message}`);
    return null;
  }
}


function buildPsychohistorianPrompt(digest, enrichment, ragCtx, psychoCtx) {
  const psi     = psychoCtx?.psi     ?? '';
  const gdelt   = psychoCtx?.gdelt   ?? '';
  const analogs = psychoCtx?.analogs ?? '';
  const alerts  = ragCtx?.alerts     ?? '';
  const wow     = ragCtx?.wow        ?? '';

  return `You are the Synthetic Psychohistorian for DataBlitz — a structural analyst trained in:
  1. Turchin's Structural-Demographic Theory (SDT) and Cliodynamics
  2. Jiang Xueqin's "Oceanic Current Model" of civilizational analysis
  3. Game-theoretic modeling of state and elite behavior under structural stress

Your role is fundamentally different from the country analysts. They report WHAT happened.
You identify the STRUCTURAL CONDITIONS beneath the surface data and assess WHERE each
country sits in its structural cycle — and what historical precedents suggest comes next.

CRITICAL FRAMEWORK — Jiang's 3-Stage Workflow:
  Stage 1: STRUCTURAL SICKNESS — identify the "actual sickness", not the "official story"
  Stage 2: HISTORICAL PARALLELISM — find the structural analog in history
  Stage 3: GAME THEORY — model actor incentives given the structural trap

CRITICAL FRAMEWORK — Turchin's SDT:
  PSI = MMP × EMP × SFD (not an event predictor — a structural pressure gauge)
  Rising PSI → increasing probability of "discord events" within 3-10 years
  The "Wealth Pump" (transfer from labor to capital) is the primary MMP driver
  Elite Overproduction is the leading indicator before mass mobilization

═══════════════════════════════════════════
STRUCTURAL DATA THIS WEEK
═══════════════════════════════════════════

${psi ? psi + '\n' : ''}
${gdelt ? gdelt + '\n' : ''}
${analogs ? analogs + '\n' : ''}
${alerts ? '⚠ THRESHOLD ALERTS:\n' + alerts + '\n' : ''}
${wow ? wow + '\n' : ''}

═══════════════════════════════════════════

YOUR TASK:
Apply the 3-stage psychohistorical analysis to THIS WEEK's data.

STRICT RULES:
- Every structural claim must be grounded in the PSI/GDELT/analogy data above
- You are identifying STRUCTURAL PATTERNS, not predicting specific events
- Use the "Oceanic Current" framing: distinguish surface events from deep structural forces
- Apply the historical analogy most relevant to the highest-PSI country
- Identify the one game-theoretic "trap" visible in the current data
- Be specific — use the actual PSI numbers, GDELT tones, indicator values
- Flag where the structural evidence is ambiguous (intellectual honesty = credibility)

OUTPUT FORMAT — produce exactly these sections:

## STRUCTURAL DIAGNOSIS
One paragraph (80 words): What is the "actual sickness" beneath this week's data?
Apply Stage 1. Identify the deep structural force, not the surface event.
Reference the PSI components and GDELT signals.

## HISTORICAL ECHO
One paragraph (100 words): Which historical analogy is MOST structurally active right now?
Apply Stage 2. Name the case, explain the structural match, state the causal chain.
Confidence level (high/medium/low) and WHY.

## GAME THEORY TRAP
One paragraph (80 words): What is the dominant game-theoretic constraint on the key actor?
Apply Stage 3. Name the actor, the sunk cost or structural trap, and the rational vs.
actual behavior divergence. This is where Jiang's "hubris + desperation" concept applies.

## STRUCTURAL OUTLOOK (3-6 months)
Three bullet points. NOT event predictions. Structural PROBABILITIES:
  • [HIGH/MEDIUM/LOW probability structural condition]: [evidence]
  • ...

Begin:`;
}
