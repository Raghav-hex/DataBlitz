/**
 * datablitz/ai_engine/puter_client.js
 * ─────────────────────────────────────
 * Server-side Puter.js client using the official init.cjs pattern.
 *
 * Auth: Set PUTER_AUTH_TOKEN env var.
 * Get token: puter.com/dashboard → click "Copy" next to your auth token.
 *
 * Fallback: direct fetch to api.puter.com if SDK init fails.
 */

import { createRequire } from 'module';
const require = createRequire(import.meta.url);

const PUTER_API  = 'https://api.puter.com/drivers/call';
const MAX_RETRIES = 3;
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ─── SDK-based call (preferred) ───────────────────────────────────────────────

async function puterChatSDK(prompt, opts = {}) {
  const token = process.env.PUTER_AUTH_TOKEN;
  if (!token) throw new Error('PUTER_AUTH_TOKEN not set');

  const { init } = require('@heyputer/puter.js/src/init.cjs');
  const puter = init(token);

  const model   = opts.model      ?? 'claude-sonnet-4-5';
  const maxTok  = opts.maxTokens  ?? 4096;
  const temp    = opts.temperature ?? 0.7;

  const result = await puter.ai.chat(prompt, {
    model,
    max_tokens: maxTok,
    temperature: temp,
  });

  // result.toString() returns the text content per Puter SDK docs
  const text = result?.message?.content ?? result?.toString?.() ?? String(result);
  if (!text) throw new Error('Puter SDK returned empty content');
  return text;
}

// ─── Direct HTTP fallback ─────────────────────────────────────────────────────

async function puterChatHTTP(prompt, opts = {}) {
  const token = process.env.PUTER_AUTH_TOKEN;
  if (!token) throw new Error('PUTER_AUTH_TOKEN not set');

  const body = {
    interface: 'puter-chat-completion',
    driver:    'claude',
    method:    'complete',
    args: {
      model:      opts.model      ?? 'claude-sonnet-4-5',
      max_tokens: opts.maxTokens  ?? 4096,
      temperature: opts.temperature ?? 0.7,
      messages:   [{ role: 'user', content: prompt }],
    },
  };

  const response = await fetch(PUTER_API, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`,
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(`Puter HTTP ${response.status}: ${text.slice(0, 200)}`);
  }

  const data = await response.json();
  const content =
    data?.result?.message?.content ??
    data?.result?.content ??
    data?.message?.content;

  if (!content) throw new Error(`Unexpected Puter response shape: ${JSON.stringify(data).slice(0, 200)}`);
  return typeof content === 'string' ? content : content[0]?.text ?? JSON.stringify(content);
}

// ─── Public export with retry + fallback ─────────────────────────────────────

export async function puterChat(prompt, opts = {}) {
  let lastError;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      // Try SDK first, fall back to direct HTTP on SDK init errors
      try {
        return await puterChatSDK(prompt, opts);
      } catch (sdkErr) {
        if (sdkErr.message?.includes('init.cjs')) {
          console.warn('[puter_client] SDK init failed, using HTTP fallback');
          return await puterChatHTTP(prompt, opts);
        }
        throw sdkErr;
      }
    } catch (err) {
      lastError = err;
      console.error(`[puter_client] Attempt ${attempt}/${MAX_RETRIES}: ${err.message}`);
      if (attempt < MAX_RETRIES) await sleep(2000 * attempt);
    }
  }

  throw new Error(`Puter API failed after ${MAX_RETRIES} attempts: ${lastError?.message}`);
}
