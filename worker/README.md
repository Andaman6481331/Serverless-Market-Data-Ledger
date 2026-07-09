# market-data-api (Cloudflare Worker)

Read-only public API in front of the D1 `gold_candles` table. Runs inside
Cloudflare's network and binds to D1 directly (`env.DB`), so it needs **no
secrets** — no account id, no `D1_API_TOKEN`. A frontend can call it directly.

## Endpoints

### `GET /api/candles?symbol=XAUUSD&timeframe=5min&limit=200`
Most recent `limit` candles (default 200, max 1000) for `symbol`+`timeframe`,
returned oldest→newest.

```json
{
  "symbol": "XAUUSD",
  "timeframe": "5min",
  "count": 200,
  "candles": [
    { "bar_time": 1420074000000, "bar_open": 1184.1, "bar_high": 1184.6,
      "bar_low": 1183.9, "bar_close": 1184.4, "bar_volume": 812.0, "atr": 0.53 }
  ]
}
```
`bar_time` is epoch-ms UTC (bucket start). `atr` may be `null` (ATR warm-up).
Invalid `symbol`/`timeframe` → `400`. `symbol` is upper-cased before validation;
both are checked against the same whitelist `engine/pipeline.py` uses.

### `GET /api/health`
Pipeline-freshness snapshot for the dashboard health panel.

```json
{
  "status": "ok",
  "row_count": 51234,
  "last_ingested_at": 1420078200000,
  "last_ingested_at_iso": "2015-01-01T01:30:00.000Z",
  "minutes_since_last_update": 3.2
}
```

CORS is `*` for now (lock `Access-Control-Allow-Origin` in `index.js` to the real
frontend origin once deployed). Responses carry a short `Cache-Control` max-age and
are cached at the edge so repeated polling doesn't hit D1 every time.

## Deploy

The D1 database already exists (UUID `99d829b6-09fc-4ddf-a4b3-abfa365b8ac9`), so
there's nothing to create — just authenticate and deploy.

```bash
cd worker

# 1. Authenticate wrangler against the Cloudflare account that owns the DB.
npx wrangler login
#   (CI/headless alternative: export CLOUDFLARE_API_TOKEN=... with Workers +
#    D1 edit scopes. If your login has multiple accounts, also set
#    CLOUDFLARE_ACCOUNT_ID=f6b144524516642f76670b6590e1e668)

# 2. (optional) Confirm the binding resolves and the table is reachable.
npx wrangler d1 execute xauusd_metrics --remote \
  --command "SELECT COUNT(*) AS n FROM gold_candles;"

# 3. Deploy.
npx wrangler deploy
```

Deploy prints the public URL, e.g.
`https://market-data-api.<your-subdomain>.workers.dev`. Smoke-test:

```bash
curl "https://market-data-api.<your-subdomain>.workers.dev/api/health"
curl "https://market-data-api.<your-subdomain>.workers.dev/api/candles?symbol=XAUUSD&timeframe=5min&limit=5"
```

Local dev against the **remote** D1 (no local copy of the data):

```bash
npx wrangler dev --remote
```

Live logs from the deployed Worker:

```bash
npx wrangler tail
```
