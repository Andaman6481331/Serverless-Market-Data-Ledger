"""
Silver Layer — cleaning utilities

Two standalone, strategy-free cleaning primitives ported from Scout Sniper
(GoldStream-ETL-Pipeline):

  _ensure_utc        — guarantee a UTC-aware datetime regardless of input form.
                       (from core/silver/silver_processor.py, verbatim)
  derive_price       — add a `price` column = mid = (bid + ask) / 2 from Bronze's
                       raw bid/ask, keeping bid/ask/bid_volume/ask_volume intact.
                       Must run BEFORE deduplicate_ticks so `price` exists in the
                       dedup key.
  deduplicate_ticks  — remove duplicate ticks. Ported from the dedup step inside
                       HistoryDownloader.merge_month, with the duplicate key
                       widened from (timestamp_utc, symbol) to
                       (timestamp_utc, symbol, price) so two ticks that share a
                       millisecond but carry different prices are BOTH kept
                       instead of being collapsed to one.

Canonical Silver order: _ensure_utc (as needed) -> derive_price -> deduplicate_ticks.

None of these functions import any strategy code, config, or trade-state object.
"""

from datetime import datetime, timezone

import pandas as pd


def _ensure_utc(dt) -> datetime:
    """
    Guarantee a UTC-aware datetime regardless of whether the input is a
    naive datetime, a tz-naive pd.Timestamp, or already UTC-aware.
    """
    if isinstance(dt, pd.Timestamp):
        if dt.tz is None:
            dt = dt.tz_localize("UTC")
        return dt.to_pydatetime()
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    # Fallback: parse as string
    ts = pd.to_datetime(dt)
    if ts.tz is None:
        ts = ts.tz_localize("UTC")
    return ts.to_pydatetime()


def derive_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a `price` column as the mid price (bid + ask) / 2, derived from Bronze's
    raw bid/ask columns.

    `price` is the tick-identity value consumed by deduplicate_ticks(). The
    original bid, ask, bid_volume, and ask_volume columns are preserved intact —
    price is *added*, nothing is dropped.

    Args:
        df: Tick DataFrame containing at least "bid" and "ask" columns.

    Returns:
        A copy of *df* with an added "price" column. Input is not mutated.
    """
    df = df.copy()
    df["price"] = (df["bid"] + df["ask"]) / 2.0
    return df


def deduplicate_ticks(
    df: pd.DataFrame,
    subset: list[str] | None = None,
) -> pd.DataFrame:
    """
    Remove duplicate ticks and return the frame sorted ascending by timestamp.

    A duplicate is defined as a row sharing the same values across *subset*.
    The default subset is ["timestamp_utc", "symbol", "price"] — deliberately
    price-inclusive so that two ticks stamped to the same millisecond but at
    different prices are preserved as distinct events rather than collapsed
    (the original Scout Sniper merge_month used only ["timestamp_utc", "symbol"],
    which lost same-ms price changes).

    Args:
        df:     Input tick DataFrame. Must contain every column named in *subset*.
        subset: Columns that jointly define tick identity. Defaults to
                ["timestamp_utc", "symbol", "price"].

    Returns:
        A new DataFrame with duplicates dropped, sorted by "timestamp_utc",
        and index reset.
    """
    if subset is None:
        subset = ["timestamp_utc", "symbol", "price"]

    return (
        df.drop_duplicates(subset=subset)
        .sort_values("timestamp_utc")
        .reset_index(drop=True)
    )
