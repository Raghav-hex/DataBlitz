/**
 * datablitz/ai_engine/index.js
 * ─────────────────────────────
 * Main AI narrative engine entry point.
 *
 * Reads:  ../data/digest_latest.json  (from Python ingestion pipeline)
 * Writes: ../data/narrative_latest.json
 *
 * Usage:
 *   node ai_engine/index.js
 *   node ai_engine/index.js --input ./data/digest_2024-01-01.json
 *   node ai_engine/index.js --dry-run  (prompt only, no API call)
 */

import { readFileSync, writeFileSync, mkdirSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { puterChat } from './puter_client.js';
import { buildDigestPrompt, buildCountryBriefPrompt } from './prompts.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, '..');

// ─── CLI args ─────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const dryRun = args.includes('--dry-run');
const inputIdx = args.indexOf('--input');
const inputPath = inputIdx >= 0
  ? args[inputIdx + 1]
  : resolve(ROOT, 'data', 'digest_latest.json');
const outputPath = resolve(ROOT, 'data', 'narrative_latest.json');


// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`[ai_engine] Starting narrative generation`);
  console.log(`[ai_engine] Input:  ${inputPath}`);
  console.log(`[ai_engine] Output: ${outputPath}`);
  console.log(`[ai_engine] Dry run: ${dryRun}`);

  // 1. Load the digest
  let digest;
  try {
    const raw = readFileSync(inputPath, 'utf-8');
    digest = JSON.parse(raw);
    console.log(`[ai_engine] Loaded digest: run_id=${digest.run_id}, countries=${digest.digests?.length}`);
  } catch (err) {
    console.error(`[ai_engine] Failed to read digest: ${err.message}`);
    process.exit(1);
  }

  if (!digest.digests || digest.digests.length === 0) {
    console.error('[ai_engine] Digest contains no country data — aborting');
    process.exit(1);
  }

  // 2. Build the main digest prompt
  const mainPrompt = buildDigestPrompt(digest);
  console.log(`[ai_engine] Main prompt: ${mainPrompt.length} chars`);

  if (dryRun) {
    console.log('\n[ai_engine] DRY RUN — prompt preview:\n');
    console.log(mainPrompt.slice(0, 1000) + '\n...[truncated]');
    return;
  }

  // 3. Generate main weekly narrative
  console.log('[ai_engine] Calling Puter/Claude for main digest...');
  let mainNarrative;
  try {
    mainNarrative = await puterChat(mainPrompt, {
      model: 'claude-sonnet-4-5',
      maxTokens: 4096,
      temperature: 0.75,
    });
    console.log(`[ai_engine] Main narrative: ${mainNarrative.length} chars`);
  } catch (err) {
    console.error(`[ai_engine] Main narrative failed: ${err.message}`);
    process.exit(1);
  }

  // 4. Generate per-country briefs (parallelised)
  console.log('[ai_engine] Generating per-country briefs (parallel)...');
  const briefResults = await Promise.allSettled(
    digest.digests.map(async (countryDigest) => {
      const prompt = buildCountryBriefPrompt(countryDigest);
      const brief = await puterChat(prompt, {
        model: 'claude-sonnet-4-5',
        maxTokens: 512,
        temperature: 0.6,
      });
      console.log(`[ai_engine]   ${countryDigest.country}: brief done (${brief.length} chars)`);
      return { country: countryDigest.country, brief };
    })
  );

  const countryBriefs = {};
  for (const result of briefResults) {
    if (result.status === 'fulfilled') {
      countryBriefs[result.value.country] = result.value.brief;
    } else {
      console.warn(`[ai_engine] Brief failed: ${result.reason}`);
    }
  }

  // 5. Assemble output
  const narrative = {
    run_id: digest.run_id,
    generated_at: new Date().toISOString(),
    digest_run_id: digest.run_id,
    main_narrative: mainNarrative,
    country_briefs: countryBriefs,
    meta: {
      indicators_total: digest.digests.reduce((s, d) => s + (d.indicators?.length ?? 0), 0),
      countries: digest.digests.map(d => d.country),
      model: 'claude-sonnet-4-5',
      prompt_chars: mainPrompt.length,
    },
  };

  // 6. Write output
  mkdirSync(dirname(outputPath), { recursive: true });
  writeFileSync(outputPath, JSON.stringify(narrative, null, 2));
  console.log(`\n[ai_engine] Done! Narrative written to ${outputPath}`);
  console.log(`[ai_engine] Main narrative: ${mainNarrative.length} chars`);
  console.log(`[ai_engine] Country briefs: ${Object.keys(countryBriefs).join(', ')}`);
  console.log(`\n${'─'.repeat(60)}`);
  console.log('HEADLINE PREVIEW:');
  const headlineMatch = mainNarrative.match(/## HEADLINE\n(.+)/);
  if (headlineMatch) console.log(headlineMatch[1]);
  console.log('─'.repeat(60));
}

main().catch(err => {
  console.error('[ai_engine] Unhandled error:', err);
  process.exit(1);
});
