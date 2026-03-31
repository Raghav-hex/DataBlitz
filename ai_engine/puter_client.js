/**
 * datablitz/ai_engine/puter_client.js
 * ─────────────────────────────────────
 * Thin server-side wrapper around the Puter.com chat completion API.
 *
 * Puter's SDK is browser-first, so we call the API directly via fetch
 * (native in Node.js 18+). Same endpoint, same auth — no browser polyfills.
 *
 * API: POST https://api.puter.com/drivers/call
 * Auth: Bearer token (PUTER_AUTH_TOKEN env var)
 * Model: claude-sonnet-4-5 (best balance of quality / cost for weekly digest)
 */

const PUTER_API = 'https://api.puter.com/drivers/call';
const DEFAULT_MODEL = 'claude-sonnet-4-5';
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 2000;

/**
 * Sleep helper for retry backoff.
 */
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Call Puter's Claude API with retry logic.
 *
 * @param {string} prompt  - The full prompt string
 * @param {object} opts    - Optional overrides: model, maxTokens, temperature
 * @returns {Promise<string>} - The text content of Claude's response
 */
export async function puterChat(prompt, opts = {}) {
  const token = process.env.PUTER_AUTH_TOKEN;
  if (!token) {
    throw new Error('PUTER_AUTH_TOKEN is not set in environment');
  }

  const model = opts.model ?? DEFAULT_MODEL;
  const maxTokens = opts.maxTokens ?? 4096;
  const temperature = opts.temperature ?? 0.7;

  const body = {
    interface: 'puter-chat-completion',
    driver: 'claude',
    method: 'complete',
    args: {
      model,
      max_tokens: maxTokens,
      temperature,
      messages: [
        { role: 'user', content: prompt }
      ],
    },
  };

  let lastError;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
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
        throw new Error(`Puter API HTTP ${response.status}: ${text.slice(0, 200)}`);
      }

      const data = await response.json();

      // Puter wraps the response — extract text content
      const content = data?.result?.message?.content
        ?? data?.result?.content
        ?? data?.message?.content;

      if (!content) {
        throw new Error(`Puter API returned unexpected shape: ${JSON.stringify(data).slice(0, 300)}`);
      }

      return typeof content === 'string'
        ? content
        : content[0]?.text ?? JSON.stringify(content);

    } catch (err) {
      lastError = err;
      console.error(`[puter_client] Attempt ${attempt}/${MAX_RETRIES} failed: ${err.message}`);
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAY_MS * attempt);
      }
    }
  }

  throw new Error(`Puter API failed after ${MAX_RETRIES} attempts: ${lastError?.message}`);
}
