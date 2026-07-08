"""
Gold Layer — tick -> OHLC resampling (DuckDB)

Aggregation lives in Gold (mirrors the original Scout Sniper architecture, where
the Gold layer owned all resampling/feature work). This module keeps ONLY the
plain OHLCV candle build — every strategy-flavoured column from the original
FeatureEngineer._resample_ohlc (CVD, cvd_slope, spread stats, session/feature
engineering) has been stripped out.

Input  (Silver tick output): timestamp_utc, price, bid, ask, bid_volume,
                             ask_volume, symbol   (price = mid = (bid+ask)/2)
Output (OHLCV candles):      bar_time, bar_open, bar_high, bar_low, bar_close,
                             bar_volume

OHLC is built from `price` (mid). bar_volume = sum(bid_volume + ask_volume).
bar_time is the bucket START (left edge), matching pandas resample() labelling
and the original architecture.

The output feeds gold.atr.compute_atr directly: bar_high / bar_low / bar_close
already match the column names ATR expects, and rows are returned in ascending
bar_time order (ATR reads row order, not an index).

UTC pinning: the DuckDB session TimeZone is forced to 'UTC' before bucketing.
time_bucket() on a TIMESTAMP WITH TIME ZONE otherwise anchors bucket edges to
the machine's local timezone — which silently shifts 4h/1d candle boundaries off
UTC midnight and makes bar_time's rendered tz machine-dependent. Pinning UTC
keeps candles deterministic and correct on any host (incl. serverless workers).
"""

import re
from pathlib import Path

import duckdb
import pandas as pd

# Whitelisted units → DuckDB INTERVAL words. Keeps the interval literal safe to
# interpolate into SQL (the numeric part is coerced to int, the unit is mapped
# from this fixed table — no user string reaches the query).
_UNIT_MAP = {
    "s": "seconds", "sec": "seconds", "secs": "seconds", "second": "seconds", "seconds": "seconds",
    "min": "minutes", "mins": "minutes", "minute": "minutes", "minutes": "minutes", "t": "minutes",
    "h": "hours", "hr": "hours", "hrs": "hours", "hour": "hours", "hours": "hours",
    "d": "days", "day": "days", "days": "days",
}

_REQUIRED_COLS = {"timestamp_utc", "price", "bid_volume", "ask_volume"}


def _to_duckdb_interval(timeframe: str) -> str:
    """Convert a pandas-style timeframe ('5min', '15min', '4h', '1d') to a
    DuckDB interval literal body ('5 minutes', '15 minutes', '4 hours', ...)."""
    m = re.fullmatch(r"\s*(\d+)\s*([a-zA-Z]+)\s*", timeframe)
    if not m:
        raise ValueError(
            f"Unrecognized timeframe {timeframe!r}; expected e.g. '1min', '5min', '15min', '4h', '1d'."
        )
    n, unit = int(m.group(1)), m.group(2).lower()
    if n <= 0:
        raise ValueError(f"Timeframe magnitude must be positive, got {n} in {timeframe!r}.")
    if unit not in _UNIT_MAP:
        raise ValueError(
            f"Unrecognized timeframe unit {unit!r} in {timeframe!r}; supported: s / min / h / d."
        )
    return f"{n} {_UNIT_MAP[unit]}"


def resample_to_ohlc(
    ticks_df_or_path,
    timeframe: str = "5min",
    con: "duckdb.DuckDBPyConnection | None" = None,
) -> pd.DataFrame:
    """
    Resample Silver's tick output into plain OHLCV candles using DuckDB.

    Args:
        ticks_df_or_path: Either a pandas DataFrame of Silver ticks, or a path
            (str / pathlib.Path) to a Parquet file of Silver ticks. Must expose
            columns: timestamp_utc, price, bid_volume, ask_volume.
        timeframe: Candle width, pandas-style ('1min', '5min', '15min', '4h',
            '1d'). Default '5min'.
        con: Optional existing DuckDB connection. If omitted, an in-memory
            connection is created and closed before returning.

    Returns:
        DataFrame with columns [bar_time, bar_open, bar_high, bar_low,
        bar_close, bar_volume], one row per bucket, ascending by bar_time.
    """
    interval = _to_duckdb_interval(timeframe)

    owns_con = con is None
    con = con or duckdb.connect()
    try:
        # Pin UTC so time_bucket() anchors bucket edges to UTC (not the host's
        # local tz) and bar_time renders in UTC regardless of machine.
        con.execute("SET TimeZone='UTC';")

        if isinstance(ticks_df_or_path, (str, Path)):
            source = f"read_parquet('{Path(ticks_df_or_path).as_posix()}')"
        elif isinstance(ticks_df_or_path, pd.DataFrame):
            con.register("_silver_ticks", ticks_df_or_path)
            source = "_silver_ticks"
        else:
            raise TypeError(
                "ticks_df_or_path must be a pandas DataFrame or a path to a "
                f"Parquet file, got {type(ticks_df_or_path).__name__}."
            )

        # Validate the source has the columns we need, uniformly for df/parquet.
        cols = set(con.execute(f"SELECT * FROM {source} LIMIT 0").df().columns)
        missing = _REQUIRED_COLS - cols
        if missing:
            raise ValueError(
                f"resample_to_ohlc: input missing required columns {sorted(missing)}; "
                f"got {sorted(cols)}."
            )

        # arg_min/arg_max(price, timestamp_utc) give open/close by time order
        # without relying on scan order; max/min give high/low.
        sql = f"""
            SELECT
                time_bucket(INTERVAL '{interval}', timestamp_utc) AS bar_time,
                arg_min(price, timestamp_utc)                     AS bar_open,
                max(price)                                        AS bar_high,
                min(price)                                        AS bar_low,
                arg_max(price, timestamp_utc)                     AS bar_close,
                sum(bid_volume + ask_volume)                      AS bar_volume
            FROM {source}
            GROUP BY bar_time
            ORDER BY bar_time
        """
        candles = con.execute(sql).df()

        # DuckDB returns TIMESTAMPTZ as datetime64[us, UTC]; normalize to
        # millisecond resolution so bar_time matches Bronze's datetime64[ms, UTC]
        # dtype across all three layers before anything is persisted to Parquet/D1.
        candles["bar_time"] = candles["bar_time"].dt.as_unit("ms")
        return candles
    finally:
        if owns_con:
            con.close()
