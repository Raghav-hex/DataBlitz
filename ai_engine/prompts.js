/**
 * datablitz/ai_engine/prompts.js
 * ───────────────────────────────
 * Prompt engineering for DataBlitz weekly digest.
 *
 * Design principles:
 *  1. Give Claude the raw numbers — let it find the story
 *  2. Force cross-country comparison — that's our differentiation
 *  3. "Surprise me" directive — no generic summaries
 *  4. Structured output sections so delivery layer can parse cleanly
 */

/**
 * Format a single indicator's recent data as a concise text summary.
 */
function formatIndicator(ind) {
  const latest = ind.observations.at(-1);
  const prev = ind.observations.at(-2);

  let changeStr = '';
  if (prev && latest) {
    const delta = latest.value - prev.value;
    const pct = prev.value !== 0 ? ((delta / Math.abs(prev.value)) * 100).toFixed(2) : null;
    const arrow = delta > 0 ? '▲' : delta < 0 ? '▼' : '—';
    changeStr = pct !== null
      ? ` (${arrow} ${Math.abs(delta).toFixed(3)}, ${pct}% vs prior period)`
      : ` (${arrow} ${Math.abs(delta).toFixed(3)} vs prior period)`;
  }

  // Last 6 observations as a trend spark
  const spark = ind.observations.slice(-6).map(o => o.value.toFixed(2)).join(' → ');

  return `  • ${ind.name}: ${latest?.value?.toFixed(3)} ${ind.unit}${changeStr}
    Trend (last ${Math.min(6, ind.observations.length)} periods): ${spark}
    [Source: ${ind.source_name} | Status: ${ind.status}]`;
}

/**
 * Format one country's full digest section.
 */
function formatCountrySection(digest) {
  const country = digest.country.toUpperCase();
  const byCategory = {};
  for (const ind of digest.indicators) {
    if (!byCategory[ind.category]) byCategory[ind.category] = [];
    byCategory[ind.category].push(ind);
  }

  const lines = [`\n### ${country}\n`];
  for (const [cat, indicators] of Object.entries(byCategory)) {
    lines.push(`**${cat.toUpperCase()}**`);
    for (const ind of indicators) {
      lines.push(formatIndicator(ind));
    }
    lines.push('');
  }

  if (digest.errors?.length) {
    lines.push(`⚠ Data gaps: ${digest.errors.join('; ')}`);
  }

  return lines.join('\n');
}

/**
 * Build the full prompt for the weekly digest generation.
 *
 * @param {object} globalDigest  - The parsed GlobalDigest JSON object
 * @param {object} enrichment    - Optional: { news, trends } from RSS + Google Trends
 * @returns {string} - Complete prompt ready for Claude/Grok/Gemini
 */
export function buildDigestPrompt(globalDigest, enrichment = {}) {
  const runDate = new Date(globalDigest.generated_at).toDateString();
  const dataSections = globalDigest.digests.map(formatCountrySection).join('\n');

  // Build enrichment context block if available
  let enrichmentBlock = '';
  if (enrichment.news && Object.keys(enrichment.news).length > 0) {
    const newsLines = Object.entries(enrichment.news)
      .map(([country, headlines]) =>
        `  ${country.toUpperCase()}:\n` +
        headlines.map(h => `    - ${h}`).join('\n')
      ).join('\n');
    enrichmentBlock += `\nTHIS WEEK'S NEWS HEADLINES (use to explain WHY numbers moved):\n${newsLines}\n`;
  }
  if (enrichment.trends && enrichment.trends.trim()) {
    enrichmentBlock += `\n${enrichment.trends}\n`;
  }

  return `You are the lead analyst and writer for DataBlitz — a weekly global data digest that finds surprising, non-obvious stories in open government data across 4 countries: USA, UK, India, and Brazil.

Today's run date: ${runDate}
Run ID: ${globalDigest.run_id}

═══════════════════════════════════════════
LIVE DATA FROM GOVERNMENT APIS THIS WEEK
═══════════════════════════════════════════
${dataSections}${enrichmentBlock}
═══════════════════════════════════════════

YOUR TASK:
Write the weekly DataBlitz digest. You must find the 3 most surprising, counterintuitive, or cross-country resonant stories in this data. Generic summaries like "inflation rose slightly" are worthless — we want the stories that make a reader stop and say "wait, really?".

STRICT RULES:
- Every claim must be grounded in the numbers above — no invention
- You MUST find at least one cross-country comparison or divergence
- Flag any data marked "stale" as potentially outdated
- Be specific: use the actual numbers, not vague adjectives
- Write for an intelligent non-specialist reader (policy wonk, investor, curious citizen)

OUTPUT FORMAT — produce exactly these sections, using these exact headings:

## HEADLINE
One punchy sentence (max 12 words) capturing the single biggest story this week.

## THE BIG THREE
Three numbered story sections, each containing:
- **Story title** (bold, max 8 words)
- 2–3 paragraphs of analysis (150–250 words each)
- One "So what?" paragraph explaining real-world implications

## CROSS-COUNTRY SIGNAL
One 200-word section spotlighting a trend that cuts across 2+ countries this week. What does the comparison reveal that looking at any single country would miss?

## DATA WATCH
Bullet-point list of 5–8 additional notable movements in the data this week that didn't make the Big Three — but a sharp analyst would want to track.

## NUMBERS OF THE WEEK
Exactly 4 striking single statistics from this week's data, formatted as:
**[NUMBER]** — [one-sentence context]

## METHODOLOGY NOTE
One short paragraph noting any data gaps, stale sources, or caveats readers should know about.

Begin:`;
}

/**
 * Build a shorter "data brief" prompt for individual country summaries.
 *
 * @param {object} countryDigest    - Single CountryDigest object
 * @param {string[]} recentHeadlines - Optional RSS headlines for this country
 * @returns {string}
 */
export function buildCountryBriefPrompt(countryDigest, recentHeadlines = []) {
  const section = formatCountrySection(countryDigest);
  const country = countryDigest.country.toUpperCase();

  const newsBlock = recentHeadlines.length > 0
    ? `\nRECENT NEWS:\n${recentHeadlines.slice(0, 4).map(h => `  - ${h}`).join('\n')}\n`
    : '';

  return `You are a concise data journalist. Write a 150-word brief on ${country}'s key economic and social data movements this week.

DATA:
${section}${newsBlock}
Requirements:
- Focus on the single most significant change
- Use the actual numbers
- Reference news context if provided — explain *why* the data moved
- End with one "watch for" forward-looking note
- Tone: crisp, analytical, no fluff

Write the brief:`;
}
