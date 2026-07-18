<script setup>
import { ref, watch, onMounted, onBeforeUnmount, computed } from 'vue';
import {
  createChart,
  CandlestickSeries,
  LineSeries,
  CrosshairMode,
} from 'lightweight-charts';
import { formatPrice } from '../utils/format.js';

const props = defineProps({
  candles: { type: Array, default: () => [] },
  loading: { type: Boolean, default: false },
  error: { type: String, default: null },
  symbol: { type: String, default: 'XAUUSD' },
  timeframe: { type: String, default: '5min' },
  // Trading-session status from App (marketStatus()); drives the "Markets
  // closed" badge so a flat weekend window reads as intentional.
  market: { type: Object, default: () => ({ open: true }) },
});

const container = ref(null);

// lightweight-charts objects are not reactive — keep them in plain closure vars
// so Vue never tries to proxy them.
let chart = null;
let candleSeries = null;
let atrSeries = null;
let hasFitOnce = false;

// Dark, market-terminal palette. Kept in JS because lightweight-charts is
// canvas-drawn and can't read our CSS variables.
const COLORS = {
  text: '#8b94a7',
  grid: 'rgba(255, 255, 255, 0.05)',
  border: 'rgba(255, 255, 255, 0.10)',
  up: '#26a69a',
  down: '#ef5350',
  atr: '#f0b90b',
};

/** epoch-ms -> lightweight-charts UNIX time (seconds). */
const toTime = (barTimeMs) => Math.floor(barTimeMs / 1000);

function buildCandleData() {
  return props.candles
    .filter((c) => c && c.bar_time != null)
    .map((c) => ({
      time: toTime(c.bar_time),
      open: c.bar_open,
      high: c.bar_high,
      low: c.bar_low,
      close: c.bar_close,
    }));
}

function buildAtrData() {
  return props.candles
    .filter((c) => c && c.bar_time != null && c.atr != null)
    .map((c) => ({ time: toTime(c.bar_time), value: c.atr }));
}

function createChartInstance() {
  chart = createChart(container.value, {
    autoSize: true, // tracks container size via ResizeObserver internally
    layout: {
      background: { color: 'transparent' },
      textColor: COLORS.text,
      fontFamily:
        "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
      attributionLogo: false,
      panes: { separatorColor: COLORS.border, separatorHoverColor: COLORS.border },
    },
    grid: {
      vertLines: { color: COLORS.grid },
      horzLines: { color: COLORS.grid },
    },
    rightPriceScale: { borderColor: COLORS.border },
    timeScale: {
      borderColor: COLORS.border,
      timeVisible: true,
      secondsVisible: false,
    },
    crosshair: { mode: CrosshairMode.Normal },
  });

  candleSeries = chart.addSeries(CandlestickSeries, {
    upColor: COLORS.up,
    downColor: COLORS.down,
    borderUpColor: COLORS.up,
    borderDownColor: COLORS.down,
    wickUpColor: COLORS.up,
    wickDownColor: COLORS.down,
    priceFormat: { type: 'price', precision: 2, minMove: 0.01 },
  });

  // ATR on its own pane (index 1) below the price — an oscillator-style read
  // that would otherwise crush the candle scale if overlaid.
  atrSeries = chart.addSeries(
    LineSeries,
    {
      color: COLORS.atr,
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: true,
      title: 'ATR',
      priceFormat: { type: 'price', precision: 2, minMove: 0.01 },
    },
    1,
  );

  // Keep the ATR pane compact so the price chart gets most of the height.
  try {
    const panes = chart.panes();
    if (panes.length > 1) panes[1].setHeight(110);
  } catch {
    /* pane sizing is best-effort; default layout is fine if it throws */
  }

  syncData();
}

function syncData() {
  if (!chart || !candleSeries || !atrSeries) return;
  const candleData = buildCandleData();
  candleSeries.setData(candleData);
  atrSeries.setData(buildAtrData());

  // Fit the full window once on first data; afterwards just keep the latest bar
  // in view without yanking the user's zoom on every 60s refresh.
  if (candleData.length) {
    if (!hasFitOnce) {
      chart.timeScale().fitContent();
      hasFitOnce = true;
    } else {
      chart.timeScale().scrollToRealTime();
    }
  }
}

// Latest close + change, shown in the header for a quick read.
const lastClose = computed(() => {
  const list = props.candles;
  return list.length ? list[list.length - 1].bar_close : null;
});
const change = computed(() => {
  const list = props.candles;
  if (list.length < 2) return null;
  const prev = list[list.length - 2].bar_close;
  const curr = list[list.length - 1].bar_close;
  if (prev == null || curr == null) return null;
  const abs = curr - prev;
  return { abs, pct: prev !== 0 ? (abs / prev) * 100 : 0, up: abs >= 0 };
});
const lastCloseLabel = computed(() =>
  lastClose.value == null ? '—' : formatPrice(lastClose.value),
);

const showLoading = computed(() => props.loading && props.candles.length === 0);
const showError = computed(() => props.error && props.candles.length === 0);
const showEmpty = computed(
  () => !props.loading && !props.error && props.candles.length === 0,
);

onMounted(createChartInstance);
watch(() => props.candles, syncData, { deep: false });
onBeforeUnmount(() => {
  if (chart) {
    chart.remove();
    chart = null;
    candleSeries = null;
    atrSeries = null;
  }
});
</script>

<template>
  <section class="card chart-card" aria-label="XAUUSD candle chart">
    <header class="chart-head">
      <div class="chart-title">
        <h2>{{ symbol }}</h2>
        <span class="tf-badge">{{ timeframe }}</span>
        <span
          v-if="!market.open"
          class="closed-badge"
          :title="market.detail"
        >Markets closed</span>
      </div>
      <div v-if="lastClose != null" class="last-price">
        <span class="price">{{ lastCloseLabel }}</span>
        <span
          v-if="change"
          class="change"
          :class="change.up ? 'up' : 'down'"
        >
          {{ change.up ? '▲' : '▼' }}
          {{ formatPrice(Math.abs(change.abs)) }}
          ({{ change.pct >= 0 ? '+' : '' }}{{ change.pct.toFixed(2) }}%)
        </span>
      </div>
    </header>

    <div class="chart-wrap">
      <!-- The canvas host. Overlays sit on top for non-data states. -->
      <div ref="container" class="chart-canvas"></div>

      <div v-if="showLoading" class="chart-overlay">
        <div class="spinner" aria-hidden="true"></div>
        <p>Loading candles…</p>
      </div>
      <div v-else-if="showError" class="chart-overlay error">
        <p class="overlay-title">Chart unavailable</p>
        <p class="overlay-detail">{{ error }}</p>
      </div>
      <div v-else-if="showEmpty" class="chart-overlay">
        <p class="overlay-title">No candle data</p>
        <p class="overlay-detail">
          The ledger returned no {{ timeframe }} candles for {{ symbol }}.
        </p>
      </div>
    </div>

    <p class="chart-footnote">
      Yellow line = ATR (Average True Range) volatility, lower pane.
    </p>
  </section>
</template>

<style scoped>
.chart-card {
  display: flex;
  flex-direction: column;
  gap: 0.9rem;
}

.chart-head {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 1rem;
  flex-wrap: wrap;
}

.chart-title {
  display: flex;
  align-items: baseline;
  gap: 0.6rem;
}
.chart-title h2 {
  margin: 0;
  font-size: 1.15rem;
  font-weight: 700;
  letter-spacing: 0.02em;
}
.tf-badge {
  font-size: 0.72rem;
  font-weight: 600;
  color: var(--text-muted);
  background: var(--surface-2);
  border: 1px solid var(--border);
  padding: 0.12rem 0.5rem;
  border-radius: 999px;
  text-transform: uppercase;
  letter-spacing: 0.04em;
}
.closed-badge {
  font-size: 0.72rem;
  font-weight: 600;
  color: var(--accent);
  background: color-mix(in srgb, var(--accent) 14%, transparent);
  border: 1px solid color-mix(in srgb, var(--accent) 30%, transparent);
  padding: 0.12rem 0.5rem;
  border-radius: 999px;
}

.last-price {
  display: flex;
  align-items: baseline;
  gap: 0.6rem;
  font-variant-numeric: tabular-nums;
}
.last-price .price {
  font-size: 1.15rem;
  font-weight: 700;
}
.change {
  font-size: 0.85rem;
  font-weight: 600;
}
.change.up { color: var(--ok); }
.change.down { color: var(--bad); }

/* Chart canvas + overlays --------------------------------------------------- */
.chart-wrap {
  position: relative;
  width: 100%;
  height: 460px;
}
.chart-canvas {
  position: absolute;
  inset: 0;
}

.chart-overlay {
  position: absolute;
  inset: 0;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 0.6rem;
  text-align: center;
  padding: 1rem;
  background: color-mix(in srgb, var(--surface) 70%, transparent);
  backdrop-filter: blur(1px);
  color: var(--text-muted);
}
.overlay-title {
  margin: 0;
  font-weight: 600;
  color: var(--text);
}
.chart-overlay.error .overlay-title { color: var(--bad); }
.overlay-detail {
  margin: 0;
  font-size: 0.88rem;
  max-width: 32ch;
}

.spinner {
  width: 2.1rem;
  height: 2.1rem;
  border-radius: 50%;
  border: 3px solid var(--surface-2);
  border-top-color: var(--accent);
  animation: spin 0.8s linear infinite;
}
@keyframes spin {
  to { transform: rotate(360deg); }
}

.chart-footnote {
  margin: 0;
  font-size: 0.76rem;
  color: var(--text-muted);
}

@media (max-width: 560px) {
  .chart-wrap { height: 360px; }
}
</style>
