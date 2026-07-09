# Market Data Ledger — Dashboard

A single-page **Vue 3 (Composition API, `<script setup>`) + Vite** dashboard for
the Serverless Market Data Ledger. It renders live XAUUSD data straight from the
read-only Cloudflare Worker API — no backend of its own, no auth, no routing.

## What it shows

- **Pipeline health card** — last ingested time ("3 minutes ago"), total row
  count, and a colored status indicator:
  - 🟢 **Healthy** — updated < 20 min ago
  - 🟡 **Delayed** — 20–60 min (the ~15 min pipeline missed a cycle)
  - 🔴 **Stale** — > 60 min (pipeline likely stuck)
- **Candle chart** — XAUUSD 5-minute OHLC candles via
  [`lightweight-charts`](https://tradingview.github.io/lightweight-charts/), with
  **ATR** (Average True Range) drawn on a compact secondary pane below.
- **Auto-refresh** — both panels poll the Worker every 60 s. A local clock keeps
  the "x minutes ago" label and status color live in between polls.
- **Graceful states** — first-load skeletons/spinner, error overlays when the
  Worker is unreachable, an empty-data message, and a "showing last known values"
  warning if a refresh fails after data was already loaded (never a blank screen).

## Data source

The deployed Worker (see [`../worker/`](../worker/)):

```
GET /api/candles?symbol=XAUUSD&timeframe=5min&limit=200
GET /api/health
```

CORS is wide open on the Worker, so the browser calls it directly.

## Run locally

Requires Node 18+.

```bash
cd frontend
npm install
npm run dev
```

Open the printed URL (default http://localhost:5173). It talks to the **deployed**
Worker out of the box — no config needed.

To point at a different Worker (e.g. a local `wrangler dev` on port 8787), create
`frontend/.env.local`:

```bash
# frontend/.env.local
VITE_API_BASE=http://localhost:8787
```

(See [`.env.example`](.env.example). The value falls back to the deployed Worker
when unset.)

### Other scripts

```bash
npm run build     # production build -> dist/
npm run preview   # serve the built dist/ locally to sanity-check the build
```

## Deploy to Cloudflare Pages

Everything else in this project lives on Cloudflare, so Pages is the natural home.
The build output is a plain static `dist/` folder — no server runtime.

**Build settings**

| Setting          | Value           |
| ---------------- | --------------- |
| Build command    | `npm run build` |
| Build output dir | `dist`          |
| Root directory   | `frontend`      |

### Option A — Git integration (recommended)

1. Cloudflare dashboard → **Workers & Pages** → **Create** → **Pages** →
   **Connect to Git**, and pick this repo.
2. Framework preset **Vue** (or none), then set the build command / output dir /
   root directory from the table above.
3. **Save and Deploy.** Every push to the branch redeploys automatically; PRs get
   preview URLs.

### Option B — Direct upload with Wrangler

From `frontend/`:

```bash
npm run build
npx wrangler pages deploy dist --project-name market-data-dashboard
```

First run creates the Pages project (accept the prompt / add
`--branch main` for the production branch). Subsequent runs redeploy.

### After deploying — lock down CORS (recommended)

The Worker currently sends `Access-Control-Allow-Origin: *`. Once you know the
Pages URL (e.g. `https://market-data-dashboard.pages.dev`), tighten it in
[`../worker/index.js`](../worker/index.js) — change `CORS_HEADERS`'
`Access-Control-Allow-Origin` to that exact origin and redeploy the Worker. (A
custom `VITE_API_BASE` isn't needed for production since the default already
points at the live Worker; set one as a Pages environment variable only if the
API moves.)

## Project layout

```
frontend/
├── index.html
├── package.json
├── vite.config.js
├── .env.example
└── src/
    ├── main.js               # app bootstrap
    ├── App.vue               # layout + 60s polling + shared clock/state
    ├── api.js                # Worker API client (base URL, fetchHealth, fetchCandles)
    ├── styles.css            # global theme (dark default, light fallback)
    ├── utils/format.js       # relative time / number / price formatting
    └── components/
        ├── HealthCard.vue    # pipeline-health panel
        └── CandleChart.vue   # lightweight-charts candle + ATR pane
```

## Notes / choices

- **`lightweight-charts` v5** — series are created with the v5
  `chart.addSeries(CandlestickSeries, …)` API; ATR sits on pane index 1.
- **No UI framework** — vanilla scoped CSS with CSS variables keeps the bundle
  small (~83 kB gzipped, mostly the charting lib) and the design consistent.
- **Polling, not WebSockets** — the pipeline runs every ~15 min, so a 60 s poll is
  more than fresh enough and far simpler to operate.
