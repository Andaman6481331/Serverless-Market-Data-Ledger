<script setup>
import { ref, onMounted, onBeforeUnmount } from 'vue';
import HealthCard from './components/HealthCard.vue';
import CandleChart from './components/CandleChart.vue';
import {
  fetchHealth,
  fetchCandles,
  SYMBOL,
  TIMEFRAME,
  CANDLE_LIMIT,
} from './api.js';
import { formatRelativeTime } from './utils/format.js';

const REFRESH_MS = 60_000; // poll the Worker every 60s
const CLOCK_MS = 5_000; // tick the local clock every 5s (keeps "x min ago" live)

// Per-panel state. Data is kept on failure so a transient blip shows the last
// good values (with a stale warning) instead of blanking the panel.
const health = ref(null);
const healthError = ref(null);
const healthLoading = ref(true);

const candles = ref([]);
const candlesError = ref(null);
const candlesLoading = ref(true);

const now = ref(Date.now());
const lastRefresh = ref(null); // epoch-ms of last *successful* refresh (either panel)

let refreshTimer = null;
let clockTimer = null;

async function refreshHealth() {
  try {
    const data = await fetchHealth();
    health.value = data;
    healthError.value = null;
    lastRefresh.value = Date.now();
  } catch (err) {
    healthError.value = err.message || 'Failed to load health.';
  } finally {
    healthLoading.value = false;
  }
}

async function refreshCandles() {
  try {
    const data = await fetchCandles({
      symbol: SYMBOL,
      timeframe: TIMEFRAME,
      limit: CANDLE_LIMIT,
    });
    candles.value = Array.isArray(data.candles) ? data.candles : [];
    candlesError.value = null;
    lastRefresh.value = Date.now();
  } catch (err) {
    candlesError.value = err.message || 'Failed to load candles.';
  } finally {
    candlesLoading.value = false;
  }
}

// Refresh both panels together (independent failures) — the brief's single 60s tick.
function refreshAll() {
  now.value = Date.now();
  return Promise.allSettled([refreshHealth(), refreshCandles()]);
}

onMounted(() => {
  refreshAll();
  refreshTimer = setInterval(refreshAll, REFRESH_MS);
  clockTimer = setInterval(() => {
    now.value = Date.now();
  }, CLOCK_MS);
});

onBeforeUnmount(() => {
  clearInterval(refreshTimer);
  clearInterval(clockTimer);
});
</script>

<template>
  <div class="page">
    <header class="app-header">
      <div class="brand">
        <span class="logo" aria-hidden="true">◧</span>
        <div>
          <h1>Market Data Ledger</h1>
          <p class="subtitle">Serverless XAUUSD pipeline · live dashboard</p>
        </div>
      </div>
      <div class="refresh-meta" aria-live="polite">
        <span class="live-dot" aria-hidden="true"></span>
        <span v-if="lastRefresh">
          Updated {{ formatRelativeTime(lastRefresh, now) }}
        </span>
        <span v-else>Connecting…</span>
        <span class="refresh-hint">· auto-refresh 60s</span>
      </div>
    </header>

    <main class="panels">
      <HealthCard
        :health="health"
        :loading="healthLoading"
        :error="healthError"
        :now="now"
      />
      <CandleChart
        :candles="candles"
        :loading="candlesLoading"
        :error="candlesError"
        :symbol="SYMBOL"
        :timeframe="TIMEFRAME"
      />
    </main>

    <footer class="app-footer">
      Data served from the read-only Cloudflare Worker · Bronze → Silver → Gold
      pipeline
    </footer>
  </div>
</template>

<style scoped>
.page {
  max-width: var(--maxw);
  margin: 0 auto;
  padding: 1.5rem 1.25rem 3rem;
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
}

/* Header -------------------------------------------------------------------- */
.app-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
  flex-wrap: wrap;
  padding: 0.5rem 0.25rem;
}

.brand {
  display: flex;
  align-items: center;
  gap: 0.85rem;
}
.logo {
  font-size: 1.9rem;
  color: var(--accent);
  line-height: 1;
}
.brand h1 {
  margin: 0;
  font-size: 1.4rem;
  font-weight: 750;
  letter-spacing: -0.01em;
}
.subtitle {
  margin: 0.1rem 0 0;
  font-size: 0.85rem;
  color: var(--text-muted);
}

.refresh-meta {
  display: inline-flex;
  align-items: center;
  gap: 0.4rem;
  font-size: 0.82rem;
  color: var(--text-muted);
  white-space: nowrap;
}
.live-dot {
  width: 0.5rem;
  height: 0.5rem;
  border-radius: 50%;
  background: var(--ok);
  box-shadow: 0 0 0 0 var(--ok);
  animation: live 2.4s ease-out infinite;
}
.refresh-hint {
  opacity: 0.7;
}
@keyframes live {
  0% { box-shadow: 0 0 0 0 color-mix(in srgb, var(--ok) 60%, transparent); }
  70% { box-shadow: 0 0 0 0.45rem transparent; }
  100% { box-shadow: 0 0 0 0 transparent; }
}

/* Panels -------------------------------------------------------------------- */
.panels {
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
}

/* Footer -------------------------------------------------------------------- */
.app-footer {
  text-align: center;
  font-size: 0.78rem;
  color: var(--text-muted);
  padding-top: 0.5rem;
}

@media (max-width: 560px) {
  .refresh-meta { width: 100%; }
}
</style>
