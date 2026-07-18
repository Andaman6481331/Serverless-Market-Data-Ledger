// XAUUSD trading-session awareness (client-side).
//
// Mirrors the weekly-close rule encoded in gold/session_calendar.py: spot gold
// trades ~24×5 and shuts for the weekend from Friday 22:00 UTC to Sunday 22:00
// UTC. During that window the feed returns a near-constant last price, so the
// dashboard would otherwise (a) render a flat band and (b) climb into a false
// "Delayed/Stale" health state — even though the pipeline is behaving perfectly.
//
// We deliberately model ONLY the weekly close here (not the ~1h daily
// maintenance break) — it's the closure long enough to matter for the health
// indicator. Uses UTC throughout so it's independent of the viewer's timezone.

const WEEKLY_CLOSE_MIN = 22 * 60; // Fri 22:00 UTC close / Sun 22:00 UTC reopen

const FRIDAY = 5; // Date.getUTCDay(): Sun=0 … Sat=6
const SATURDAY = 6;
const SUNDAY = 0;

/** True if the XAUUSD market is open at `now` (a Date). */
export function isMarketOpen(now = new Date()) {
  const day = now.getUTCDay();
  const minutes = now.getUTCHours() * 60 + now.getUTCMinutes();

  if (day === SATURDAY) return false; // all of Saturday
  if (day === FRIDAY && minutes >= WEEKLY_CLOSE_MIN) return false; // Fri after 22:00
  if (day === SUNDAY && minutes < WEEKLY_CLOSE_MIN) return false; // Sun before 22:00
  return true;
}

/**
 * Session status for display. When closed, `detail` names the reopen time so the
 * dashboard reads as intentional ("closed, back Sunday") rather than broken.
 */
export function marketStatus(now = new Date()) {
  if (isMarketOpen(now)) return { open: true, label: 'Live', detail: '' };
  return {
    open: false,
    label: 'Markets closed',
    detail: 'XAUUSD reopens Sunday 22:00 UTC',
  };
}
