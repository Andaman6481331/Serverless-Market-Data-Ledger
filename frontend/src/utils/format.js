// Small, dependency-free formatting helpers shared by the panels.

/**
 * Human-readable "time ago" for an epoch-ms timestamp, e.g. "3 minutes ago".
 * `now` is passed in (rather than read from Date.now here) so callers can drive
 * it from a reactive clock and keep the label live between API polls.
 */
export function formatRelativeTime(epochMs, now = Date.now()) {
  if (epochMs == null) return '—';
  const diffMs = now - epochMs;
  if (diffMs < 0) return 'just now'; // clock skew: server slightly ahead
  const seconds = Math.floor(diffMs / 1000);
  if (seconds < 45) return 'just now';

  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes} minute${minutes === 1 ? '' : 's'} ago`;

  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours} hour${hours === 1 ? '' : 's'} ago`;

  const days = Math.floor(hours / 24);
  return `${days} day${days === 1 ? '' : 's'} ago`;
}

/** Whole number of minutes between `epochMs` and `now` (used for status color). */
export function minutesSince(epochMs, now = Date.now()) {
  if (epochMs == null) return null;
  return Math.max(0, (now - epochMs) / 60000);
}

/** Thousands-separated integer, e.g. 12345 -> "12,345". */
export function formatNumber(n) {
  if (n == null || Number.isNaN(n)) return '—';
  return Number(n).toLocaleString('en-US');
}

/** Absolute local timestamp, e.g. "Jul 9, 2026, 2:47 PM". */
export function formatTimestamp(epochMs) {
  if (epochMs == null) return '—';
  return new Date(epochMs).toLocaleString(undefined, {
    dateStyle: 'medium',
    timeStyle: 'short',
  });
}

/** Price with 2 decimals + thousands separators, e.g. "4,123.78". */
export function formatPrice(n) {
  if (n == null || Number.isNaN(n)) return '—';
  return Number(n).toLocaleString('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}
