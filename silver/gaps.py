"""
Silver Layer — gap detection

Genuinely new logic (nothing to port from Scout Sniper). Answers one question:
*which expected timestamp buckets are missing from a cleaned time series?*

`detect_gaps` builds the expected UTC grid for the data's own [min, max] span at
`expected_freq`, drops the buckets that fall on a weekend (the market is closed
Sat/Sun — same weekday filter Bronze's HistoryDownloader.download_range uses to
pre-skip weekend hours), then reports every contiguous run of expected buckets
that has no data.

Scope boundary — this function ONLY detects and reports. It does not interpolate,
forward-fill, or otherwise invent data, and it does not decide whether a gap is
"acceptable". Whether to fill a reported gap (and how) is a separate, explicit
decision for a later step — exactly as deduplicate_ticks stays out of price
policy. Keeping detection pure means a real data-loss gap can never be silently
papered over here.

What weekend-exclusion does and does NOT cover
----------------------------------------------
Excluding Sat/Sun removes the ~48h weekly close (Fri night → Sun night), which
would otherwise dominate every report as one giant false gap. It deliberately
does NOT model finer market-calendar closures — the daily ~1h metals maintenance
break, the Friday-evening close before midnight UTC, or holidays (e.g. New
Year's Day). Those are weekday minutes with no ticks, so they surface as gaps.
That is intentional: this layer has no trading-calendar oracle, and inventing one
here would risk masking genuine data loss. Callers that want session-aware
suppression should treat that as the separate "is this gap expected?" decision.

Input:  any DataFrame carrying a UTC timestamp column — Silver tick output
        (`timestamp_utc`) or Gold OHLC bars (`bar_time`). Tick-level input is
        floored to `expected_freq`, so a minute with >=1 tick counts as present.
Output: DataFrame[gap_start, gap_end, duration_minutes], one row per contiguous
        missing run, ascending by gap_start. Empty (0-row) frame when no gaps.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

_GAP_COLUMNS = ["gap_start", "gap_end", "duration_minutes"]


def _empty_gaps() -> pd.DataFrame:
    """A correctly-typed, 0-row gaps frame (returned when there is nothing to
    report, so callers get stable columns/dtypes regardless of outcome)."""
    return pd.DataFrame({
        "gap_start":        pd.Series([], dtype="datetime64[ns, UTC]"),
        "gap_end":          pd.Series([], dtype="datetime64[ns, UTC]"),
        "duration_minutes": pd.Series([], dtype="float64"),
    })


def _resolve_timestamp_col(df: pd.DataFrame, timestamp_col: str | None) -> str:
    """Pick the timestamp column: caller override wins, else the first known
    Silver/Gold timestamp column present."""
    if timestamp_col is not None:
        if timestamp_col not in df.columns:
            raise ValueError(
                f"detect_gaps: timestamp_col={timestamp_col!r} not in df columns "
                f"{list(df.columns)}."
            )
        return timestamp_col
    for candidate in ("timestamp_utc", "bar_time"):
        if candidate in df.columns:
            return candidate
    raise ValueError(
        "detect_gaps: no timestamp column found. Expected one of "
        "'timestamp_utc' / 'bar_time', or pass timestamp_col=... explicitly. "
        f"Got columns {list(df.columns)}."
    )


def detect_gaps(
    df: pd.DataFrame,
    symbol: str,
    expected_freq: str = "1min",
    timestamp_col: str | None = None,
) -> pd.DataFrame:
    """
    Detect missing periods in a cleaned time series and report them as gaps.

    The expected grid is derived from the data itself: every `expected_freq`
    bucket between the data's own earliest and latest timestamp. Weekend buckets
    (Sat/Sun) are removed before diffing, mirroring the weekday filter Bronze's
    download_range uses (`weekday() < 5` keeps Mon–Fri). A gap is any contiguous
    run of expected weekday buckets for which `df` contains no timestamp.

    Args:
        df:            DataFrame with a UTC timestamp column. Tick-level input is
                       floored to `expected_freq`, so any bucket containing >=1
                       row counts as present.
        symbol:        Instrument label — used only for logging/traceability; it
                       does not alter detection.
        expected_freq: Fixed pandas frequency defining the bucket width and the
                       expected cadence ('1min', '5min', '1h', ...). Default
                       '1min'.
        timestamp_col: Override the timestamp column. Defaults to 'timestamp_utc'
                       if present, else 'bar_time'.

    Returns:
        DataFrame with columns [gap_start, gap_end, duration_minutes], ascending
        by gap_start, one row per contiguous missing run. `gap_start` is the
        start of the first missing bucket; `gap_end` is the start of the first
        present bucket after the run (exclusive upper bound = the resume point),
        so `duration_minutes == (gap_end - gap_start)` in minutes. Returns a
        0-row frame when there are no gaps. Nothing is filled or interpolated.
    """
    col = _resolve_timestamp_col(df, timestamp_col)

    # Normalise to a UTC-aware, tz-consistent timestamp series. tz-naive input is
    # assumed to already be UTC (Bronze/Silver stamp everything UTC upstream).
    ts = pd.to_datetime(df[col], utc=True).dropna()
    if ts.empty:
        logger.info(f"[detect_gaps] {symbol}: no timestamps to analyse — 0 gaps.")
        return _empty_gaps()

    step = pd.tseries.frequencies.to_offset(expected_freq)
    step_delta = pd.Timedelta(step)  # fixed-frequency delta (e.g. '1min' -> 60s)

    # Present buckets: floor every actual timestamp to the grid, dedup. A minute
    # holding one tick or a thousand collapses to a single present bucket.
    present = pd.DatetimeIndex(ts.dt.floor(expected_freq).unique()).sort_values()

    # Expected grid over the data's own span, then drop weekend buckets. Anchor
    # the grid on the floored min so expected buckets align exactly with present
    # buckets (raw min could sit mid-bucket and offset the whole grid).
    start, end = present.min(), present.max()
    expected = pd.date_range(start=start, end=end, freq=expected_freq, tz="UTC")
    # Weekend skip — same rule as Bronze download_range: keep Mon–Fri (0–4).
    expected = expected[expected.weekday < 5]

    # Missing = expected weekday buckets with no data. (Weekend present buckets,
    # e.g. the Sunday-evening reopen, simply aren't in `expected`, so they neither
    # count as gaps nor interfere with the diff.)
    missing = expected.difference(present)
    if len(missing) == 0:
        logger.info(
            f"[detect_gaps] {symbol}: {len(present)} present / "
            f"{len(expected)} expected {expected_freq} buckets — 0 gaps."
        )
        return _empty_gaps()

    # Collapse consecutive missing buckets into runs. A new run begins wherever
    # the step from the previous missing bucket exceeds one grid step — i.e. a
    # present bucket or a removed weekend sits between them.
    missing = missing.sort_values()
    breaks = missing.to_series().diff() != step_delta  # first entry (NaT) -> True
    run_id = breaks.cumsum()

    rows = []
    for _, run in missing.to_series().groupby(run_id.values):
        gap_start = run.iloc[0]
        gap_end = run.iloc[-1] + step_delta  # exclusive: where data resumes
        rows.append((gap_start, gap_end, (gap_end - gap_start) / pd.Timedelta(minutes=1)))

    gaps = pd.DataFrame(rows, columns=_GAP_COLUMNS).sort_values("gap_start").reset_index(drop=True)

    logger.info(
        f"[detect_gaps] {symbol}: {len(gaps)} gap(s) across "
        f"{len(expected)} expected {expected_freq} buckets "
        f"({len(missing)} missing); total {gaps['duration_minutes'].sum():.0f} min."
    )
    return gaps
