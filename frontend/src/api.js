// Thin client for the read-only market-data Worker API.
//
// The Worker (worker/index.js) exposes two GET endpoints with wide-open CORS:
//   GET /api/candles?symbol=XAUUSD&timeframe=5min&limit=200
//   GET /api/health
// No auth, no credentials — the frontend can call these directly.

// Base URL: override with VITE_API_BASE in a .env.local, otherwise fall back to
// the deployed Worker. Trailing slash trimmed so path concatenation is clean.
const API_BASE = (
  import.meta.env.VITE_API_BASE ||
  'https://market-data-api.shop-backend-kitweb.workers.dev'
).replace(/\/+$/, '');

// Instrument shown by this single-page dashboard.
export const SYMBOL = 'XAUUSD';
export const TIMEFRAME = '5min';
export const CANDLE_LIMIT = 200;

/** GET `path` (relative to API_BASE) and parse JSON, throwing on non-2xx. */
async function getJson(path, { signal } = {}) {
  let res;
  try {
    res = await fetch(`${API_BASE}${path}`, {
      headers: { Accept: 'application/json' },
      signal,
    });
  } catch (err) {
    // Network failure / CORS / DNS — normalise into a readable message.
    throw new Error(`Cannot reach the API (${err.message || 'network error'}).`);
  }
  if (!res.ok) {
    throw new Error(`API returned ${res.status} ${res.statusText}.`);
  }
  return res.json();
}

/** Freshness snapshot: { status, row_count, last_ingested_at, ..., minutes_since_last_update }. */
export function fetchHealth(opts) {
  return getJson('/api/health', opts);
}

/** OHLC+ATR candles oldest→newest: { symbol, timeframe, count, candles: [...] }. */
export function fetchCandles(
  { symbol = SYMBOL, timeframe = TIMEFRAME, limit = CANDLE_LIMIT } = {},
  opts,
) {
  const query = new URLSearchParams({ symbol, timeframe, limit: String(limit) });
  return getJson(`/api/candles?${query.toString()}`, opts);
}

export { API_BASE };
