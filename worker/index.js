/**
 * Read-only public API in front of the D1 `gold_candles` table.
 *
 * This Worker runs INSIDE Cloudflare's network and talks to D1 over the native
 * binding (env.DB), so — unlike engine/pipeline.py, which reaches D1 from the
 * outside via the HTTP query API — it needs NO account id, database id, or
 * D1_API_TOKEN. There are no secrets in this component at all. A frontend can
 * hit these endpoints directly without ever seeing D1 credentials.
 *
 * Routes:
 *   GET /api/candles?symbol=XAUUSD&timeframe=5min&limit=200
 *   GET /api/health
 *
 * Safety model (defence in depth):
 *   1. symbol/timeframe are validated against the SAME character-class whitelist
 *      engine/pipeline.py uses (_SYMBOL_RE / _TIMEFRAME_RE) — anything outside it
 *      is rejected with 400 before we touch D1.
 *   2. Every value that reaches SQL is a bound parameter (`?`), never string
 *      interpolation, so even a whitelist mistake could not become injection.
 *   All statements are SELECTs; the binding has whatever grants the token/account
 *   allow, but nothing here issues a write.
 */

// ── Whitelists — identical character classes to engine/pipeline.py ────────────
// (_SYMBOL_RE = ^[A-Z0-9]{1,20}$, _TIMEFRAME_RE = ^[0-9]{1,4}[a-z]{1,10}$)
const SYMBOL_RE = /^[A-Z0-9]{1,20}$/;
const TIMEFRAME_RE = /^[0-9]{1,4}[a-z]{1,10}$/;

const DEFAULT_LIMIT = 200;
const MAX_LIMIT = 1000;

// Light caching so dashboard polling doesn't hammer D1. The edge cache below
// (caches.default) means repeated polls — even from many clients — are served
// without re-querying D1 until the entry expires.
const CANDLES_CACHE_SECONDS = 5;
const HEALTH_CACHE_SECONDS = 10;

// CORS: locked to the deployed dashboard origin (Cloudflare Pages). A single
// STATIC origin (not request-origin reflection) is deliberate — it stays
// compatible with the edge cache below (caches.default): every cached response
// carries this same ACAO header, so there's no cross-origin cache-poisoning risk
// that reflection would introduce. Update this if the dashboard origin changes.
const ALLOWED_ORIGIN = "https://market-data-dashboard-62u.pages.dev";
const CORS_HEADERS = {
  "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Max-Age": "86400",
};

/** Build a JSON Response with CORS + cache headers. */
function json(body, { status = 200, maxAge = 0 } = {}) {
  const headers = {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": maxAge > 0 ? `public, max-age=${maxAge}` : "no-store",
    ...CORS_HEADERS,
  };
  return new Response(JSON.stringify(body), { status, headers });
}

/** Parse/clamp the limit query param: default 200, min 1, max 1000. */
function clampLimit(raw) {
  const n = parseInt(raw, 10);
  if (!Number.isFinite(n) || n <= 0) return DEFAULT_LIMIT;
  return Math.min(n, MAX_LIMIT);
}

export default {
  async fetch(request, env, ctx) {
    // Preflight.
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }
    if (request.method !== "GET") {
      return json(
        { error: "method_not_allowed", message: "Only GET is supported." },
        { status: 405 }
      );
    }

    const url = new URL(request.url);

    // Edge cache: serve repeat polls straight from Cloudflare's cache, sparing
    // D1. Key is the full request (URL + query), so each symbol/timeframe/limit
    // combination caches independently.
    const cache = caches.default;
    const hit = await cache.match(request);
    if (hit) return hit;

    let response;
    try {
      if (url.pathname === "/api/candles") {
        response = await handleCandles(url, env);
      } else if (url.pathname === "/api/health") {
        response = await handleHealth(env);
      } else {
        response = json(
          { error: "not_found", message: `Unknown route ${url.pathname}` },
          { status: 404 }
        );
      }
    } catch (err) {
      // Never leak internals to the public; log for the Worker tail instead.
      console.error("handler error:", err && err.stack ? err.stack : err);
      response = json(
        { error: "internal_error", message: "Query failed." },
        { status: 500 }
      );
    }

    // Only store successful, explicitly-cacheable responses at the edge.
    if (
      response.status === 200 &&
      (response.headers.get("Cache-Control") || "").includes("max-age")
    ) {
      ctx.waitUntil(cache.put(request, response.clone()));
    }
    return response;
  },
};

/**
 * GET /api/candles?symbol=XAUUSD&timeframe=5min&limit=200
 * Returns the most recent `limit` candles for symbol+timeframe, oldest->newest.
 */
async function handleCandles(url, env) {
  const symbol = (url.searchParams.get("symbol") || "").toUpperCase();
  const timeframe = url.searchParams.get("timeframe") || "";

  if (!SYMBOL_RE.test(symbol)) {
    return json(
      { error: "invalid_symbol", message: `symbol must match ${SYMBOL_RE}` },
      { status: 400 }
    );
  }
  if (!TIMEFRAME_RE.test(timeframe)) {
    return json(
      { error: "invalid_timeframe", message: `timeframe must match ${TIMEFRAME_RE}` },
      { status: 400 }
    );
  }
  const limit = clampLimit(url.searchParams.get("limit"));

  // Take the newest `limit` rows (DESC + LIMIT), then re-sort ascending so the
  // chart receives them oldest->newest. symbol/timeframe are whitelist-checked
  // above; all three values are bound params, never interpolated.
  const sql = `
    SELECT bar_time, bar_open, bar_high, bar_low, bar_close, bar_volume, atr
    FROM (
      SELECT bar_time, bar_open, bar_high, bar_low, bar_close, bar_volume, atr
      FROM gold_candles
      WHERE symbol = ? AND timeframe = ?
      ORDER BY bar_time DESC
      LIMIT ?
    )
    ORDER BY bar_time ASC
  `;
  const { results } = await env.DB.prepare(sql).bind(symbol, timeframe, limit).all();

  return json(
    { symbol, timeframe, count: results.length, candles: results },
    { maxAge: CANDLES_CACHE_SECONDS }
  );
}

/**
 * GET /api/health
 * Freshness snapshot for the dashboard's pipeline-health panel: newest
 * ingested_at across ALL rows, total row count, and minutes since that write.
 */
async function handleHealth(env) {
  const row = await env.DB.prepare(
    `SELECT MAX(ingested_at) AS last_ingested_at, COUNT(*) AS row_count FROM gold_candles`
  ).first();

  // ingested_at is epoch-ms (see gold_candles DDL); NULL when the table is empty.
  const lastIngestedAt = row && row.last_ingested_at != null ? row.last_ingested_at : null;
  const rowCount = row && row.row_count != null ? row.row_count : 0;
  const minutesSince =
    lastIngestedAt == null
      ? null
      : Math.round(((Date.now() - lastIngestedAt) / 60000) * 10) / 10;

  return json(
    {
      status: "ok",
      row_count: rowCount,
      last_ingested_at: lastIngestedAt,
      last_ingested_at_iso:
        lastIngestedAt == null ? null : new Date(lastIngestedAt).toISOString(),
      minutes_since_last_update: minutesSince,
    },
    { maxAge: HEALTH_CACHE_SECONDS }
  );
}
