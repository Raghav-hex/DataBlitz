/**
 * datablitz/ai_engine/index.js
 * ─────────────────────────────
 * Main AI narrative engine.
 *
 * Reads:  ../data/digest_latest.json  (Python ingestion output)
 *         ../data/enrichment.json     (RSS + Trends, if present)
 * Writes: ../data/narrative_latest.json
 *
 * AI chain: Claude (Puter) → Grok (Puter) → Gemini Flash-Lite
 *
 * Usage:
 *   node ai_engine/index.js
 *   node ai_engine/index.js --dry-run   (show prompt, no API call)
 */

import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { tryWithFallback } from './fallback_client.js';
import { buildDigestPrompt, buildCountryBriefPrompt } from './prompts.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, '..');

const args      = process.argv.slice(2);
const dryRun    = args.includes('--dry-run');
const inputIdx  = args.indexOf('--input');
const inputPath = inputIdx >= 0 ? args[inputIdx + 1] : resolve(ROOT, 'data', 'digest_latest.json');
const outputPath   = resolve(ROOT, 'data', 'narrative_latest.json');
const enrichPath   = resolve(ROOT, 'data', 'enrichment.json');


async function main() {
  console.log(`[ai_engine] Starting  input=${inputPath}  dry=${dryRun}`);

  // 1. Load digest
  let digest;
  try {
    digest = JSON.parse(readFileSync(inputPath, 'utf-8'));
    console.log(`[ai_engine] Digest loaded: run_id=${digest.run_id}, countries=${digest.digests?.length}`);
  } catch (err) {
    console.error(`[ai_engine] Cannot read digest: ${err.message}`);
    process.exit(1);
  }

  if (!digest.digests?.length) {
    console.error('[ai_engine] Digest has no country data — aborting');
    process.exit(1);
  }

  // 2. Load enrichment (RSS + Trends) if present
  let enrichment = {};
  if (existsSync(enrichPath)) {
    try {
      enrichment = JSON.parse(readFileSync(enrichPath, 'utf-8'));
      const newsCount  = Object.values(enrichment.news  ?? {}).flat().length;
      const trendCount = Object.values(enrichment.trends_raw ?? {}).length;
      console.log(`[ai_engine] Enrichment loaded: ${newsCount} headlines, ${trendCount} trend countries`);
    } catch (err) {
      console.warn(`[ai_engine] Could not load enrichment.json: ${err.message}`);
    }
  } else {
    console.log('[ai_engine] No enrichment.json found — running without news/trends context');
  }

  // 2b. Load RAG context (historical + WoW + alerts)
  const ragPath = resolve(ROOT, 'data', 'rag_context.json');
  let ragCtx = {};
  if (existsSync(ragPath)) {
    try {
      ragCtx = JSON.parse(readFileSync(ragPath, 'utf-8'));
      console.log(`[ai_engine] RAG loaded: ${ragCtx.alert_count ?? 0} alerts, historical=${!!ragCtx.historical}`);
    } catch (err) {
      console.warn(`[ai_engine] Could not load rag_context.json: ${err.message}`);
    }
  }

  // 3. Build prompt
  const mainPrompt = buildDigestPrompt(digest, {
    news:        enrichment.news   ?? {},
    trends:      enrichment.trends ?? '',
    historical:  ragCtx.historical ?? '',
    wow:         ragCtx.wow        ?? '',
    alerts:      ragCtx.alerts     ?? '',
  });
  console.log(`[ai_engine] Prompt: ${mainPrompt.length} chars`);

  if (dryRun) {
    console.log('\n── PROMPT PREVIEW (first 1200 chars) ──\n');
    console.log(mainPrompt.slice(0, 1200) + '\n...[truncated]');
    return;
  }

  // 4. Generate main narrative (with fallback chain)
  console.log('[ai_engine] Generating main narrative...');
  let mainResult;
  try {
    mainResult = await tryWithFallback(mainPrompt, { maxTokens: 4096, temperature: 0.75 });
    console.log(`[ai_engine] Main narrative: ${mainResult.text.length} chars via ${mainResult.provider}`);
  } catch (err) {
    console.error(`[ai_engine] All AI providers failed: ${err.message}`);
    process.exit(1);
  }

  // 5. Generate per-country briefs in parallel (shorter, fallback also applies)
  console.log('[ai_engine] Generating country briefs...');
  const briefResults = await Promise.allSettled(
    digest.digests.map(async (cd) => {
      const prompt = buildCountryBriefPrompt(cd, enrichment.news?.[cd.country] ?? []);
      const result = await tryWithFallback(prompt, { maxTokens: 512, temperature: 0.6 });
      console.log(`[ai_engine]   ${cd.country}: ${result.text.length} chars via ${result.provider}`);
      return { country: cd.country, brief: result.text, provider: result.provider };
    })
  );

  const countryBriefs  = {};
  const briefProviders = {};
  for (const r of briefResults) {
    if (r.status === 'fulfilled') {
      countryBriefs[r.value.country]  = r.value.brief;
      briefProviders[r.value.country] = r.value.provider;
    } else {
      console.warn(`[ai_engine] Brief failed: ${r.reason}`);
    }
  }

  // 6. Write output
  const narrative = {
    run_id:          digest.run_id,
    generated_at:    new Date().toISOString(),
    main_narrative:  mainResult.text,
    country_briefs:  countryBriefs,
    meta: {
      indicators_total: digest.digests.reduce((s, d) => s + (d.indicators?.length ?? 0), 0),
      countries:        digest.digests.map(d => d.country),
      main_provider:    mainResult.provider,
      brief_providers:  briefProviders,
      enrichment_used:  !!enrichment.news,
      alerts:           parseAlerts(ragCtx.alerts ?? ''),
      prompt_chars:     mainPrompt.length,
    },
  };

  mkdirSync(dirname(outputPath), { recursive: true });
  writeFileSync(outputPath, JSON.stringify(narrative, null, 2));
}

/** Parse the formatted alert string back into structured objects for the frontend. */
function parseAlerts(alertStr) {
  if (!alertStr) return [];
  const alerts = [];
  const lines = alertStr.split('\n');
  for (const line of lines) {
    const m = line.match(/\[(\w+)\]\s+([^:]+):\s+([\d.]+)\s+\(threshold:/);
    if (m) {
      alerts.push({
        level:   m[1].toLowerCase(),
        id:      m[2].trim(),
        value:   parseFloat(m[3]),
        message: '',
      });
    }
    // Pick up the message line
    if (alerts.length > 0 && line.trim().length > 0 && !line.includes('[') && line.startsWith('     ')) {
      alerts[alerts.length - 1].message = line.trim();
    }
  }
  return alerts;

  mkdirSync(dirname(outputPath), { recursive: true });
  writeFileSync(outputPath, JSON.stringify(narrative, null, 2));

  console.log(`\n[ai_engine] ✓ Done — narrative written to ${outputPath}`);
  console.log(`[ai_engine] Provider: ${mainResult.provider}`);
  const headlineMatch = mainResult.text.match(/## HEADLINE\n(.+)/);
  if (headlineMatch) {
    console.log(`\nHEADLINE: ${headlineMatch[1]}`);
  }
}

main().catch(err => {
  console.error('[ai_engine] Fatal:', err);
  process.exit(1);
});
