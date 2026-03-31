/**
 * DataBlitz Cloudflare Worker
 * ───────────────────────────
 * Reads from KV, serves JSON to the frontend.
 * Deploy with: wrangler deploy
 *
 * Routes:
 *   GET /api/narrative        → latest narrative JSON
 *   GET /api/digest           → latest raw digest JSON
 *   GET /api/meta             → last run metadata
 *   GET /api/archive/:week    → narrative for ISO week (e.g. 2026-W13)
 *
 * Binding: KV namespace bound as DATABLITZ_KV in wrangler.toml
 */

const CORS = {
  "Access-Control-Allow-Origin":  "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Content-Type":                 "application/json",
};

export default {
  async fetch(request, env) {
    const url  = new URL(request.url);
    const path = url.pathname;

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS });
    }

    // Route table
    if (path === "/api/narrative") {
      return kvResponse(env, "narrative:latest");
    }
    if (path === "/api/digest") {
      return kvResponse(env, "digest:latest");
    }
    if (path === "/api/meta") {
      return kvResponse(env, "meta:last_run");
    }

    const archiveMatch = path.match(/^\/api\/archive\/(\d{4}-W\d{2})$/);
    if (archiveMatch) {
      return kvResponse(env, `narrative:${archiveMatch[1]}`);
    }

    return new Response(
      JSON.stringify({ error: "Not found", path }),
      { status: 404, headers: CORS }
    );
  },
};

async function kvResponse(env, key) {
  const value = await env.DATABLITZ_KV.get(key);
  if (!value) {
    return new Response(
      JSON.stringify({ error: `No data for key: ${key}` }),
      { status: 404, headers: CORS }
    );
  }
  return new Response(value, {
    status: 200,
    headers: {
      ...CORS,
      "Cache-Control": "public, max-age=3600",  // 1hr edge cache
    },
  });
}
