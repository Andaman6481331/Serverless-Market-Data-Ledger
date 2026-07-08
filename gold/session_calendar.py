"""
Gold Layer — XAUUSD session calendar (optional gap classifier)

A SEPARATE, OPTIONAL filter that sits *downstream* of silver.gaps.detect_gaps.
It answers the second question detect_gaps deliberately refuses to answer:
*is this reported gap an expected market closure, or genuine data loss?*

Why this lives here and not in detect_gaps
-------------------------------------------
detect_gaps is pure detection — it has "no trading-calendar oracle" and will not
decide whether a gap is acceptable (see silver/gaps.py docstring). Encoding
session rules *inside* it would risk silently papering over real data loss. So
the trading-calendar knowledge lives here instead, as an OPT-IN post-filter:
detect_gaps stays session-unaware, and callers who want session-aware
suppression run their output through classify_gaps(). detect_gaps never imports
from this module — the dependency points one way only.

Encoded XAUUSD closure patterns (UTC)
-------------------------------------
Only the two well-known XAUUSD closures are modelled. Everything else — holidays
(e.g. the Jan 19 2015 MLK-shortened session), one-off outages, intraday
dropouts — is intentionally NOT modelled, so it stays expected=False and remains
visible as a "real" gap. This mirrors detect_gaps' philosophy: better to leave a
maybe-expected gap flagged than to hide genuine data loss.

1. Weekly close — Friday ~22:00 UTC → Sunday ~22:00 UTC.
   The market shuts Friday ~22:00 and reopens Sunday ~22:00. This has TWO
   manifestations depending on whether detect_gaps' weekend pre-filter ran:
     • WITHOUT the pre-filter (detect_gaps run on a raw grid): the gap spans the
       full ~48h Fri 22:00 → Sun 22:00. This is the case the pattern primarily
       exists for.
     • WITH the pre-filter (the normal detect_gaps path): Sat/Sun buckets are
       already removed from the expected grid, so the same close surfaces as a
       truncated Fri 22:00 → Sat 00:00 run (the missing weekday minutes are just
       Friday 22:00–23:59). Both are recognised.
   Recognised by: gap starts Friday ~22:00 UTC and ends inside the weekend
   (Saturday or Sunday), which covers both manifestations above.

2. Daily maintenance break — ~22:00–23:00 UTC.
   Each trading day has a ~1h metals maintenance break around 22:00–23:00 UTC
   where no ticks arrive and the feed resumes at 23:00. Recognised by: gap starts
   ~22:00 and ends ~23:00 on the same UTC day.

Tolerance: session boundaries are honoured to within TOL_MIN minutes (the "~"),
so a gap that starts 22:01 (because a stray tick landed in the 22:00 bucket) or
resumes a minute late still classifies correctly, while unrelated intraday
dropouts (e.g. a 1-minute hole at 04:07) never fall inside the window.

Public API
----------
  is_expected_gap(gap_start, gap_end) -> bool
      Classify a single [gap_start, gap_end) run against the two patterns above.
  classify_gaps(gaps_df) -> DataFrame
      Convenience: apply is_expected_gap across detect_gaps' output and append an
      `expected` boolean column, so callers filter real gaps with
      `gaps_df[~gaps_df.expected]`.
"""

import pandas as pd

# Weekday numbering matches pandas/​datetime: Monday=0 … Sunday=6.
FRIDAY = 4
SATURDAY = 5
SUNDAY = 6
_WEEKEND = (SATURDAY, SUNDAY)

# Session boundaries as minutes-past-UTC-midnight.
WEEKLY_CLOSE_MIN = 22 * 60   # Friday 22:00 UTC weekly close (and reopen hour)
MAINT_START_MIN = 22 * 60    # daily maintenance break opens ~22:00 UTC
MAINT_END_MIN = 23 * 60      # daily maintenance break closes ~23:00 UTC

# "~" tolerance, in minutes, applied to every session boundary above.
TOL_MIN = 15


def _to_utc(ts) -> pd.Timestamp:
    """Coerce any timestamp-like value to a UTC-aware pd.Timestamp. tz-naive
    input is assumed UTC (detect_gaps stamps everything UTC upstream)."""
    ts = pd.Timestamp(ts)
    if ts.tz is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _tod_min(ts: pd.Timestamp) -> int:
    """Minutes past UTC midnight for `ts` (0–1439)."""
    return ts.hour * 60 + ts.minute


def _near(minutes: int, target: int, tol: int = TOL_MIN) -> bool:
    """True if a time-of-day (in minutes) is within `tol` of `target`, measured
    circularly over the 1440-minute day so values near midnight still compare."""
    d = abs(minutes - target)
    return min(d, 1440 - d) <= tol


def is_expected_gap(gap_start, gap_end) -> bool:
    """
    Decide whether a single detected gap coincides with a known XAUUSD closure.

    A gap is the half-open interval [gap_start, gap_end): gap_start is the first
    missing bucket, gap_end is the resume point (first present bucket after the
    run) — exactly the semantics of detect_gaps' output columns.

    Returns True if the gap matches either encoded pattern:
      • Weekly close  — starts Friday ~22:00 UTC and ends inside the weekend
                        (Saturday/Sunday); covers both the full Fri 22:00 → Sun
                        22:00 form and the weekend-pre-filtered Fri 22:00 → Sat
                        00:00 form.
      • Daily maint.  — starts ~22:00 and ends ~23:00 UTC on the same day.
    Otherwise returns False (holidays, outages, intraday dropouts stay flagged).

    Args:
        gap_start: Gap start (first missing bucket). Any timestamp-like value;
                   tz-naive is treated as UTC.
        gap_end:   Gap end / resume point (exclusive). Same handling.

    Returns:
        bool — True if the gap is an expected session closure.
    """
    start = _to_utc(gap_start)
    end = _to_utc(gap_end)
    if pd.isna(start) or pd.isna(end):
        return False

    start_tod = _tod_min(start)
    end_tod = _tod_min(end)

    # Pattern 1 — weekly close: begins Friday ~22:00 UTC and runs into the
    # weekend. Keying on the Friday-22:00 start recognises the close whether the
    # weekend pre-filter truncated it at Sat 00:00 or it spans through to the
    # Sunday ~22:00 reopen — both land gap_end on a Saturday/Sunday.
    if start.weekday() == FRIDAY and _near(start_tod, WEEKLY_CLOSE_MIN):
        if end.weekday() in _WEEKEND:
            return True

    # Pattern 2 — daily maintenance break: a same-day run that opens ~22:00 and
    # resumes ~23:00. (The Friday break is absorbed by Pattern 1 above, since the
    # feed does not resume at 23:00 on Fridays.)
    if (
        start.date() == end.date()
        and _near(start_tod, MAINT_START_MIN)
        and _near(end_tod, MAINT_END_MIN)
    ):
        return True

    return False


def classify_gaps(gaps_df: pd.DataFrame) -> pd.DataFrame:
    """
    Append an `expected` boolean column to detect_gaps' output.

    Runs is_expected_gap over each [gap_start, gap_end) row so callers can split
    known session closures from genuine data loss without detect_gaps ever
    knowing session rules exist:

        real_gaps = gaps_df[~gaps_df.expected]

    Args:
        gaps_df: A gaps frame as returned by silver.gaps.detect_gaps — must carry
                 `gap_start` and `gap_end` columns. Not mutated.

    Returns:
        A copy of *gaps_df* with an added `expected` bool column, same row order.
        A 0-row input returns a 0-row frame with the column present (bool dtype).
    """
    out = gaps_df.copy()
    if out.empty:
        out["expected"] = pd.Series([], dtype=bool)
        return out
    out["expected"] = [
        is_expected_gap(start, end)
        for start, end in zip(out["gap_start"], out["gap_end"])
    ]
    return out
