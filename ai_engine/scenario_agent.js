/**
 * datablitz/ai_engine/scenario_agent.js
 * ──────────────────────────────────────
 * Scenario Planning agent — implements Jiang Xueqin's core deliverable.
 *
 * Jiang's actual value-add isn't just analysis — it's presenting 3 concrete
 * forward scenarios with structural probabilities. This is what makes
 * Predictive History actionable rather than just academic.
 *
 * Each scenario follows Jiang's game-theory structure:
 *   - Actor: who is the key decision-maker
 *   - Incentive: what structural pressure are they under (sunk cost, trap)
 *   - Decision point: what do they actually decide
 *   - Causal chain: 3-4 step consequence sequence
 *   - Time horizon: when this becomes visible in data
 *   - Probability: structural (not event) probability, with honest caveats
 *
 * The three scenarios follow Jiang's "Base Case / Deterioration / Black Swan"
 * structure, informed by the PSI levels and active historical analogies.
 *
 * This agent receives:
 *   - PSI scores (structural stress levels)
 *   - Active historical analogies (precedent outcomes)
 *   - GDELT tone (media reality vs official narrative)
 *   - Psychohistorian analysis (structural diagnosis)
 *   - Current indicator data (the factual base)
 */

import { tryWithFallback } from './fallback_client.js';

export async function runScenarioPlannerAgent(digest, enrichment, ragCtx, psychoCtx, psychoResult) {
  console.log('[scenario] Building scenario framework...');

  const prompt = buildScenarioPrompt(digest, enrichment, ragCtx, psychoCtx, psychoResult);

  try {
    const result = await tryWithFallback(prompt, {
      maxTokens: 2048,
      temperature: 0.65,
    });
    console.log(`[scenario] Done (${result.text.length} chars via ${result.provider})`);
    return { text: result.text, provider: result.provider };
  } catch (err) {
    console.error(`[scenario] Failed: ${err.message}`);
    return null;
  }
}


function buildScenarioPrompt(digest, enrichment, ragCtx, psychoCtx, psychoResult) {
  const psi        = psychoCtx?.psi     ?? '';
  const analogs    = psychoCtx?.analogs ?? '';
  const gdelt      = psychoCtx?.gdelt   ?? '';
  const structural = psychoResult?.text ?? '';
  const alerts     = ragCtx?.alerts     ?? '';
  const stocks     = enrichment?.stocks ?? '';
  const wow        = ragCtx?.wow        ?? '';

  // Find the highest-PSI country for scenario focus
  const countries = digest.digests?.map(d => d.country) ?? ['usa', 'uk', 'india', 'brazil'];

  return `You are the Scenario Planning analyst for DataBlitz.

Your job is Jiang Xueqin's core deliverable: produce 3 concrete forward scenarios
grounded in the structural data. This is NOT speculation — it's game-theory modeling
of the decision trees available to key actors under current structural constraints.

FRAMEWORK: Jiang's "Base Case / Deterioration / Dislocation" structure
Each scenario must identify: Actor → Structural Trap → Decision → Consequence Chain

━━━ STRUCTURAL INPUTS ━━━
${psi    ? psi    + '\n' : ''}
${gdelt  ? gdelt  + '\n' : ''}
${analogs ? analogs + '\n' : ''}
${alerts  ? '⚠ ALERTS:\n' + alerts + '\n' : ''}
${wow     ? wow + '\n' : ''}
${stocks  ? stocks + '\n' : ''}
${structural ? '--- PSYCHOHISTORIAN DIAGNOSIS ---\n' + structural.slice(0, 600) + '\n' : ''}
━━━━━━━━━━━━━━━━━━━━━━━━━

RULES:
- Base every scenario on the structural inputs above — no fabrication
- Probabilities are STRUCTURAL (not event), meaning: given current conditions,
  what % of historical analogues resolved this way?
- The "Dislocation" scenario is not doom — it's the tail risk made explicit
- Time horizons must be concrete (weeks / months / quarters), not vague
- Identify the ONE piece of data that would confirm or deny each scenario
- Countries: focus on the one with the highest PSI score, then cross-effects

Produce exactly this structure:

## SCENARIO PLANNING

**The Central Question:** [One sentence: the structural decision point this week forces]

---

### Scenario A — Base Case [XX% structural probability]
**Actor:** [who makes the key decision]
**Structural Trap:** [what constraint they're operating under]
**Most Likely Decision:** [what they do]
**Consequence Chain:** 
→ [Step 1, timeframe]
→ [Step 2, timeframe]
→ [Step 3, timeframe]
**Data Confirmation Signal:** [the ONE indicator that confirms this path]
**Historical Precedent:** [what happened in the analogous historical case]

---

### Scenario B — Deterioration [XX% structural probability]
[Same structure as A]

---

### Scenario C — Dislocation [XX% structural probability]
[Same structure — this is the tail risk scenario]
*Note: low probability ≠ impossible. Turchin shows structural stress makes
 dislocation events non-random — they're more probable than they appear.*

---

**Analyst Note:** [1-2 sentences on what would change the probability distribution —
 what data in the NEXT run would shift you from Scenario A toward B or C?]

Begin:`;
}
