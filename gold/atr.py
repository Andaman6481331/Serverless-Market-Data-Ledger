"""
Gold Layer — ATR (Average True Range)

De-classed port of FeatureEngineer._compute_atr from Scout Sniper
(GoldStream-ETL-Pipeline) core/gold/feature_engineer.py.

This is the pure price-math wrapper over ta.volatility.AverageTrueRange ONLY.
None of the surrounding strategy machinery (VPP, R-dynamic, milestone SL math,
entry gates) is included — those stay in Scout Sniper.

Expects OHLC candles with columns: bar_high, bar_low, bar_close.
"""

import numpy as np
import pandas as pd
from ta.volatility import AverageTrueRange


def compute_atr(candles: pd.DataFrame, period: int, col: str = "atr") -> pd.DataFrame:
    """
    Append an ATR column to an OHLC candle DataFrame.

    Pure function — operates only on the passed DataFrame (a copy is returned;
    the input is not mutated). Rows are left as NaN in the ATR column until the
    warm-up window (``period`` bars) is satisfied.

    Args:
        candles: OHLC DataFrame with columns "bar_high", "bar_low", "bar_close".
        period:  ATR look-back window (number of bars).
        col:     Name of the ATR column to write. Defaults to "atr".

    Returns:
        A copy of *candles* with the ATR column added.
    """
    candles = candles.copy()
    if len(candles) < period:
        candles[col] = np.nan
        return candles
    atr = AverageTrueRange(
        high=candles["bar_high"],
        low=candles["bar_low"],
        close=candles["bar_close"],
        window=period,
    ).average_true_range()
    # ta emits a literal 0.0 (not NaN) for the first `period - 1` warm-up bars,
    # where ATR is genuinely undefined. Mask them to NaN by POSITION (never by
    # value — a legitimately near-zero ATR during a flat/closed-market stretch
    # must survive) so downstream persistence writes SQL NULL, not a bogus 0.
    atr.iloc[: period - 1] = np.nan
    candles[col] = atr
    return candles
