<script setup>
import { computed } from 'vue';
import {
  formatRelativeTime,
  formatNumber,
  formatTimestamp,
  minutesSince,
} from '../utils/format.js';

const props = defineProps({
  health: { type: Object, default: null },
  loading: { type: Boolean, default: false },
  error: { type: String, default: null },
  // Reactive "current time" (epoch-ms) driven by App.vue so the relative label
  // and status color stay live between the 60s API polls.
  now: { type: Number, default: () => Date.now() },
});

// Minutes since the last ingest, computed client-side against the live clock so
// the status can go stale on screen even if the API hasn't been re-polled yet.
const minutes = computed(() =>
  props.health ? minutesSince(props.health.last_ingested_at, props.now) : null,
);

// Health thresholds reflect the real pipeline cadence (~15 min):
//   green  < 20 min  (a fresh run landed)
//   amber  20–60 min (one cycle missed — watch it)
//   red    > 60 min  (stale — pipeline likely stuck)
const status = computed(() => {
  const m = minutes.value;
  if (m == null) return { level: 'unknown', label: 'Unknown' };
  if (m < 20) return { level: 'healthy', label: 'Healthy' };
  if (m <= 60) return { level: 'delayed', label: 'Delayed' };
  return { level: 'stale', label: 'Stale' };
});

const relativeTime = computed(() =>
  props.health ? formatRelativeTime(props.health.last_ingested_at, props.now) : '—',
);
const absoluteTime = computed(() =>
  props.health ? formatTimestamp(props.health.last_ingested_at) : '',
);
const rowCount = computed(() =>
  props.health ? formatNumber(props.health.row_count) : '—',
);
const minutesLabel = computed(() => {
  const m = minutes.value;
  if (m == null) return '—';
  return `${m < 10 ? m.toFixed(1) : Math.round(m)} min`;
});

// Show the skeleton only on the very first load (no data yet). Once we have a
// snapshot we keep rendering it even while a refresh is in flight or failing.
const showSkeleton = computed(() => props.loading && !props.health);
const showError = computed(() => props.error && !props.health);
const showStaleWarning = computed(() => props.error && props.health);
</script>

<template>
  <section class="card health-card" aria-label="Pipeline health">
    <header class="card-head">
      <h2>Pipeline Health</h2>
      <span
        v-if="health"
        class="status-pill"
        :class="`status-${status.level}`"
        role="status"
      >
        <span class="dot" aria-hidden="true"></span>
        {{ status.label }}
      </span>
    </header>

    <!-- First-load skeleton -->
    <div v-if="showSkeleton" class="health-loading">
      <div class="skeleton-line"></div>
      <div class="skeleton-line short"></div>
    </div>

    <!-- Hard error with nothing to show -->
    <div v-else-if="showError" class="health-error">
      <p class="error-title">Health unavailable</p>
      <p class="error-detail">{{ error }}</p>
    </div>

    <!-- Data -->
    <template v-else-if="health">
      <div class="stats">
        <div class="stat">
          <span class="stat-label">Last ingested</span>
          <span class="stat-value" :title="absoluteTime">{{ relativeTime }}</span>
        </div>
        <div class="stat">
          <span class="stat-label">Rows in ledger</span>
          <span class="stat-value">{{ rowCount }}</span>
        </div>
        <div class="stat">
          <span class="stat-label">Since last update</span>
          <span class="stat-value">{{ minutesLabel }}</span>
        </div>
      </div>

      <p v-if="showStaleWarning" class="stale-warning" role="alert">
        ⚠ Showing last known values — refresh failed ({{ error }})
      </p>
    </template>
  </section>
</template>

<style scoped>
.health-card {
  display: flex;
  flex-direction: column;
  gap: 1rem;
}

.card-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
}

.card-head h2 {
  margin: 0;
  font-size: 0.95rem;
  font-weight: 600;
  letter-spacing: 0.02em;
  color: var(--text-muted);
  text-transform: uppercase;
}

/* Status pill --------------------------------------------------------------- */
.status-pill {
  display: inline-flex;
  align-items: center;
  gap: 0.45rem;
  padding: 0.3rem 0.7rem;
  border-radius: 999px;
  font-size: 0.82rem;
  font-weight: 600;
  border: 1px solid transparent;
  white-space: nowrap;
}

.status-pill .dot {
  width: 0.55rem;
  height: 0.55rem;
  border-radius: 50%;
  background: currentColor;
  box-shadow: 0 0 0 0 currentColor;
  animation: pulse 2.4s ease-out infinite;
}

.status-healthy {
  color: var(--ok);
  background: color-mix(in srgb, var(--ok) 14%, transparent);
  border-color: color-mix(in srgb, var(--ok) 35%, transparent);
}
.status-delayed {
  color: var(--warn);
  background: color-mix(in srgb, var(--warn) 14%, transparent);
  border-color: color-mix(in srgb, var(--warn) 35%, transparent);
}
.status-stale {
  color: var(--bad);
  background: color-mix(in srgb, var(--bad) 14%, transparent);
  border-color: color-mix(in srgb, var(--bad) 35%, transparent);
}
.status-unknown {
  color: var(--text-muted);
  background: color-mix(in srgb, var(--text-muted) 12%, transparent);
  border-color: color-mix(in srgb, var(--text-muted) 30%, transparent);
}

@keyframes pulse {
  0% { box-shadow: 0 0 0 0 color-mix(in srgb, currentColor 60%, transparent); }
  70% { box-shadow: 0 0 0 0.5rem transparent; }
  100% { box-shadow: 0 0 0 0 transparent; }
}

/* Stats --------------------------------------------------------------------- */
.stats {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 1rem;
}

.stat {
  display: flex;
  flex-direction: column;
  gap: 0.3rem;
  min-width: 0;
}

.stat-label {
  font-size: 0.75rem;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.03em;
}

.stat-value {
  font-size: 1.5rem;
  font-weight: 650;
  color: var(--text);
  font-variant-numeric: tabular-nums;
  line-height: 1.15;
  overflow-wrap: break-word;
}

/* Loading / error ----------------------------------------------------------- */
.health-loading {
  display: flex;
  flex-direction: column;
  gap: 0.7rem;
  padding: 0.5rem 0;
}
.skeleton-line {
  height: 1.5rem;
  border-radius: 6px;
  background: linear-gradient(
    90deg,
    var(--surface-2) 25%,
    color-mix(in srgb, var(--surface-2) 55%, var(--text-muted)) 50%,
    var(--surface-2) 75%
  );
  background-size: 200% 100%;
  animation: shimmer 1.4s ease-in-out infinite;
}
.skeleton-line.short { width: 55%; }
@keyframes shimmer {
  0% { background-position: 200% 0; }
  100% { background-position: -200% 0; }
}

.health-error {
  padding: 0.25rem 0;
}
.error-title {
  margin: 0 0 0.25rem;
  font-weight: 600;
  color: var(--bad);
}
.error-detail {
  margin: 0;
  color: var(--text-muted);
  font-size: 0.9rem;
}

.stale-warning {
  margin: 0;
  font-size: 0.82rem;
  color: var(--warn);
}

@media (max-width: 560px) {
  .stats {
    grid-template-columns: 1fr;
    gap: 0.85rem;
  }
  .stat-value { font-size: 1.3rem; }
}
</style>
