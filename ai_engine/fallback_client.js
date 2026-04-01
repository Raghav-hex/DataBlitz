/**
 * datablitz/ai_engine/fallback_client.js
 * ──────────────────────────────────────
 * AI client with automatic fallback chain:
 *
 *   1. Puter → Claude (claude-sonnet-4-5) — primary
 *   2. Puter → Grok  (x-ai/grok-4-1-fast) — same PUTER_AUTH_TOKEN, no extra setup!
 *   3. Gemini 2.5 Flash-Lite (REST, free 1000/day) — GEMINI_API_KEY needed
 *
 * Why this order:
 *   - Puter handles both Claude and Grok — no extra keys for fallback #1
 *   - Grok 4.1 Fast is genuinely good at data narrative (real-time X data helps)
 *   - Gemini Flash-Lite is the safety net with the most generous free quota (1k/day)
 *
 * Each level retries 2x before falling to the next level.
 */

import { createRequire } from 'module';
const require = createRequire(import.meta.url);

const PUTER_API   = 'https://api.puter.com/drivers/call';
const GEMINI_BASE = 'https://generativelanguage.googleapis.com/v1beta/models';
const GEMINI_MODEL = 'gemini-2.5-flash-lite'; // 1000 free req/day

const sleep = ms => new Promise(r => setTimeout(r, ms));


// ─── Level 1 & 2: Puter (Claude or Grok) ─────────────────────────────────────

async function callViaPuter(prompt, model, opts = {}) {
  const token = process.env.PUTER_AUTH_TOKEN;
  if (!token) throw new Error('PUTER_AUTH_TOKEN not set');

  const body = {
    interface: 'puter-chat-completion',
    driver:    model.startsWith('x-ai/') ? 'xai' : 'claude',
    method:    'complete',
    args: {
      model,
      max_tokens:  opts.maxTokens  ?? 4096,
      temperature: opts.temperature ?? 0.7,
      messages:    [{ role: 'user', content: prompt }],
    },
  };

  const response = await fetch(PUTER_API, {
    method:  'POST',
    headers: {
      'Content-Type':  'application/json',
      'Authorization': `Bearer ${token}`,
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(`Puter/${model} HTTP ${response.status}: ${text.slice(0, 150)}`);
  }

  const data = await response.json();
  const content = data?.result?.message?.content
    ?? data?.result?.content
    ?? data?.message?.content;

  if (!content) throw new Error(`Puter/${model}: empty response`);
  return typeof content === 'string' ? content : content[0]?.text ?? JSON.stringify(content);
}


// ─── Level 3: Gemini 2.5 Flash-Lite (direct REST) ────────────────────────────

async function callGemini(prompt, opts = {}) {
  const key = process.env.GEMINI_API_KEY;
  if (!key) throw new Error('GEMINI_API_KEY not set — skipping Gemini fallback');

  const url = `${GEMINI_BASE}/${GEMINI_MODEL}:generateContent?key=${key}`;

  const body = {
    contents: [{ parts: [{ text: prompt }] }],
    generationConfig: {
      maxOutputTokens: opts.maxTokens  ?? 4096,
      temperature:     opts.temperature ?? 0.7,
    },
  };

  const response = await fetch(url, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify(body),
  });

  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(`Gemini HTTP ${response.status}: ${text.slice(0, 150)}`);
  }

  const data = await response.json();
  const text  = data?.candidates?.[0]?.content?.parts?.[0]?.text;
  if (!text) throw new Error('Gemini: empty response');
  return text;
}


// ─── Public: tryWithFallback ──────────────────────────────────────────────────

/**
 * Try each AI provider in order, with retries per level.
 * Returns { text, provider } so callers know which model answered.
 */
export async function tryWithFallback(prompt, opts = {}) {
  const levels = [
    {
      name:  'Claude (Puter)',
      model: 'claude-sonnet-4-5',
      call:  () => callViaPuter(prompt, 'claude-sonnet-4-5', opts),
    },
    {
      name:  'Grok (Puter)',
      model: 'x-ai/grok-4-1-fast',
      call:  () => callViaPuter(prompt, 'x-ai/grok-4-1-fast', opts),
    },
    {
      name:  'Gemini Flash-Lite',
      model: GEMINI_MODEL,
      call:  () => callGemini(prompt, opts),
    },
  ];

  for (const level of levels) {
    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        console.log(`[ai] Trying ${level.name} (attempt ${attempt})...`);
        const text = await level.call();
        console.log(`[ai] ✓ ${level.name} responded (${text.length} chars)`);
        return { text, provider: level.name, model: level.model };
      } catch (err) {
        console.warn(`[ai] ${level.name} attempt ${attempt} failed: ${err.message}`);
        if (attempt < 2) await sleep(1500 * attempt);
      }
    }
    console.warn(`[ai] ${level.name} exhausted — trying next provider`);
  }

  throw new Error('[ai] All providers failed. Check PUTER_AUTH_TOKEN and GEMINI_API_KEY.');
}
