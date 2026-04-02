/**
 * datablitz/ai_engine/agents.js
 * ──────────────────────────────
 * Multi-agent orchestration layer.
 *
 * Architecture:
 *   ┌─────────────────────────────────────────┐
 *   │  ANALYST AGENTS (parallel, specialized) │
 *   │  USAAgent  UKAgent  IndiaAgent  BrazilAgent │
 *   └─────────────────┬───────────────────────┘
 *                     │  4 country analyses
 *                     ▼
 *   ┌─────────────────────────────────────────┐
 *   │  SYNTHESIZER AGENT                      │
 *   │  Cross-country pattern finder           │
 *   │  Produces final structured narrative    │
 *   └─────────────────────────────────────────┘
 *
 * Each analyst agent receives:
 *   - That country's indicators + WoW deltas
 *   - That country's news headlines
 *   - That country's Google Trends
 *   - That country's stock market context
 *   - Historical similar weeks for that country
 *   - Any threshold alerts for that country
 *
 * The synthesizer receives:
 *   - All 4 country analyses (compressed)
 *   - Cross-country alerts
 *   - The full output format spec
 *
 * Why this is better than one big prompt:
 *   - Each agent has focused, country-specific context
 *   - Parallel execution: 4 agents run simultaneously
 *   - Synthesizer can find cross-country patterns the analyst agents surface
 *   - If one country's agent fails, the others still succeed
 *   - Total prompt size per agent is smaller → better quality per call
 */

import { tryWithFallback } from './fallback_client.js';
import { formatCountrySectionRaw } from './prompts.js';

const COUNTRIES = ['usa', 'uk', 'india', 'brazil'];

// ─── Country analyst agent prompts ────────────────────────────────────────────

function buildAnalystPrompt(country, digest, enrichment, ragCtx) {
  const countryDigest = digest.digests.find(d => d.country === country);
  if (!countryDigest) return null;

  const dataSection   = formatCountrySectionRaw(countryDigest);
  const news          = (enrichment.news?.[country] ?? []).map(h => `  - ${h}`).join('\n');
  const alerts        = extractCountryAlerts(ragCtx.alerts ?? '', country);
  const stocks        = extractCountryStocks(enrichment.stocks ?? '', country);

  return `You are the ${country.toUpperCase()} analyst for DataBlitz, a global data intelligence service.

Your job: produce a sharp, data-grounded analysis of ${country.toUpperCase()}'s economic situation this week.

${alerts ? `⚠ ALERTS FOR ${country.toUpperCase()}:\n${alerts}\n` : ''}
RAW INDICATOR DATA:
${dataSection}

${stocks ? `MARKET CONTEXT:\n${stocks}\n` : ''}
${news   ? `NEWS THIS WEEK:\n${news}\n` : ''}

Produce a structured analysis in exactly this format:

HEADLINE: [one punchy sentence, max 12 words, the single biggest story]

KEY_MOVE: [the most significant indicator change this week, with exact numbers]

STORY: [2 paragraphs, 100-150 words total. Ground every claim in the data above.
Use news to explain WHY numbers moved. Be specific — use actual figures.]

SIGNAL: [1 sentence: what should a cross-country analyst watch from this country next week?]

RISKS: [2-3 bullet points of downside risks visible in the data]

${ragCtx.historical ? `HISTORICAL NOTE: Similar weeks found in our database:\n${extractCountryHistory(ragCtx.historical, country)}` : ''}`;
}


function buildSynthesizerPrompt(countryAnalyses, digest, enrichment, ragCtx) {
  const analysesBlock = Object.entries(countryAnalyses)
    .map(([c, a]) => `=== ${c.toUpperCase()} ANALYST REPORT ===\n${a}`)
    .join('\n\n');

  const wow     = ragCtx.wow     ?? '';
  const trends  = enrichment.trends ?? '';
  const alerts  = ragCtx.alerts  ?? '';

  return `You are the Chief Analyst for DataBlitz — a weekly global data intelligence digest.
Four country analysts have submitted their reports. Your job is to synthesize them into
the final weekly narrative, finding cross-country patterns that no single analyst would spot.

${alerts ? `THRESHOLD ALERTS (critical — address these explicitly):\n${alerts}\n` : ''}
${wow    ? `${wow}\n`    : ''}
${trends ? `${trends}\n` : ''}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
COUNTRY ANALYST REPORTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
${analysesBlock}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

STRICT RULES:
- Every claim must come from the analyst reports above — no invention
- You MUST find at least one cross-country comparison or divergence
- Flag any alerts as significant — they crossed real thresholds
- Write for an intelligent non-specialist (policy analyst, investor, curious reader)
- Use exact numbers from the reports

Produce exactly these sections with exactly these headings:

## HEADLINE
One punchy sentence (max 12 words) — the biggest story that cuts across countries this week.

## THE BIG THREE
Three numbered stories. Format each as:
**[N]. [Bold title, max 8 words]**
[2-3 paragraphs, 150-200 words. Specific numbers. Ground in analyst reports.]
So what? [1 sentence on real-world implications]

## CROSS-COUNTRY SIGNAL
200 words. One trend cutting across 2+ countries. What does the comparison reveal?

## DATA WATCH
5-8 bullet points of notable movements that didn't make the Big Three.

## NUMBERS OF THE WEEK
Exactly 4 entries: **[NUMBER]** — [one-sentence context]

## METHODOLOGY NOTE
One short paragraph noting data gaps, stale sources, or caveats.

Begin:`;
}


// ─── Public: runMultiAgentPipeline ────────────────────────────────────────────

export async function runMultiAgentPipeline(digest, enrichment, ragCtx) {
  console.log('[agents] Starting multi-agent pipeline — 4 country analysts in parallel');

  // Phase 1: all 4 country analysts run concurrently
  const analystTasks = COUNTRIES.map(async (country) => {
    const prompt = buildAnalystPrompt(country, digest, enrichment, ragCtx);
    if (!prompt) {
      console.warn(`[agents] No data for ${country} — skipping analyst`);
      return { country, result: null };
    }

    try {
      console.log(`[agents] ${country.toUpperCase()} analyst starting...`);
      const result = await tryWithFallback(prompt, { maxTokens: 1024, temperature: 0.65 });
      console.log(`[agents] ${country.toUpperCase()} analyst done (${result.text.length} chars, ${result.provider})`);
      return { country, result };
    } catch (err) {
      console.error(`[agents] ${country.toUpperCase()} analyst failed: ${err.message}`);
      return { country, result: null };
    }
  });

  const analystOutputs = await Promise.all(analystTasks);

  // Collect successful analyses
  const countryAnalyses = {};
  const providerMap = {};
  for (const { country, result } of analystOutputs) {
    if (result) {
      countryAnalyses[country] = result.text;
      providerMap[country] = result.provider;
    }
  }

  const successCount = Object.keys(countryAnalyses).length;
  console.log(`[agents] Phase 1 complete: ${successCount}/4 country analyses`);

  if (successCount === 0) {
    throw new Error('[agents] All country analysts failed — cannot synthesize');
  }

  // Phase 2: synthesizer agent
  console.log('[agents] Phase 2: synthesizer agent starting...');
  const synthPrompt  = buildSynthesizerPrompt(countryAnalyses, digest, enrichment, ragCtx);
  const synthResult  = await tryWithFallback(synthPrompt, { maxTokens: 4096, temperature: 0.72 });
  console.log(`[agents] Synthesizer done (${synthResult.text.length} chars, ${synthResult.provider})`);

  return {
    mainNarrative:   synthResult.text,
    countryAnalyses,                      // individual agent outputs saved for debugging
    providers: {
      synthesizer: synthResult.provider,
      analysts:    providerMap,
    },
  };
}


// ─── Country brief agent (per-country snapshot, short) ───────────────────────

export async function runCountryBriefAgents(digest, enrichment) {
  console.log('[agents] Running country brief agents...');

  const briefTasks = digest.digests.map(async (cd) => {
    const news = enrichment.news?.[cd.country] ?? [];
    const newsBlock = news.length
      ? `\nRECENT NEWS:\n${news.slice(0, 3).map(h => `  - ${h}`).join('\n')}`
      : '';

    const dataSection = formatCountrySectionRaw(cd);
    const prompt = `Write a 120-word brief for ${cd.country.toUpperCase()} based on this week's data.
Use the actual numbers. If news is provided, reference what drove the main change.
End with one "watch for" note.

DATA:\n${dataSection}${newsBlock}

Write:`;

    try {
      const result = await tryWithFallback(prompt, { maxTokens: 400, temperature: 0.6 });
      return { country: cd.country, brief: result.text, provider: result.provider };
    } catch (err) {
      console.warn(`[agents] Brief failed for ${cd.country}: ${err.message}`);
      return { country: cd.country, brief: '', provider: 'failed' };
    }
  });

  const results = await Promise.all(briefTasks);
  const briefs     = {};
  const providers  = {};
  for (const r of results) {
    if (r.brief) {
      briefs[r.country]    = r.brief;
      providers[r.country] = r.provider;
    }
  }
  return { briefs, providers };
}


// ─── Helpers ──────────────────────────────────────────────────────────────────

function extractCountryAlerts(alertsStr, country) {
  if (!alertsStr) return '';
  const lines = alertsStr.split('\n');
  return lines.filter(l => l.includes(country + '.')).join('\n').trim();
}

function extractCountryStocks(stocksStr, country) {
  if (!stocksStr) return '';
  const lines = stocksStr.split('\n');
  const idx = lines.findIndex(l => l.trim().startsWith(country.toUpperCase() + ':'));
  if (idx === -1) return '';
  const block = [];
  for (let i = idx + 1; i < lines.length; i++) {
    if (lines[i].match(/^\s{2}[A-Z]+:/)) break;
    block.push(lines[i]);
  }
  return block.join('\n').trim();
}

function extractCountryHistory(historicalStr, country) {
  if (!historicalStr) return '';
  const lines = historicalStr.split('\n');
  const idx = lines.findIndex(l => l.includes(country.toUpperCase()));
  if (idx === -1) return '';
  const block = [];
  for (let i = idx; i < lines.length && i < idx + 5; i++) {
    block.push(lines[i]);
  }
  return block.join('\n').trim();
}
