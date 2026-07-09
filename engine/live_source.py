"""
Engine — live price source (Twelve Data adapter)

A thin, swappable adapter for LIVE market data, kept deliberately separate from
bronze.history_downloader (Dukascopy backfill) so the ingestion source can be
changed without touching the backfill path. Nothing here imports the downloader,
and the pipeline only reaches this module when run with --mode live.

Why /time_series, not /price (decision locked with the project owner):
  The free (Basic) tier gives 800 credits/day @ 8/min; BOTH /price and
  /time_series cost 1 credit/call. /price returns a single point-in-time number
  with no high/low and no history — so in a *serverless* cron (one call, then the
  process exits) an in-memory buffer holds exactly one price and Gold's resampler
  can only emit a degenerate candle (open==high==low==close, volume 0). One
  /time_series?interval=1min call instead returns the last N *real* OHLC bars
  (true highs/lows, computed by Twelve Data from the full tick stream), so the
  Gold resample + ATR chain works from a single stateless invocation.

  /time_series bars are bridged into the existing tick -> Silver -> Gold path by
  exploding each bar into four synthetic ticks (open, high, low, close at
  increasing sub-bar timestamps). gold.resample_to_ohlc — which derives
  open/close via arg_min/arg_max over timestamp and high/low via max/min —
  reconstructs the identical candle, and composes correctly when the target
  timeframe is a multiple of the fetch interval (e.g. 1min bars -> 5min candles).

Public surface:
  to_twelvedata_symbol(symbol)     XAUUSD -> "XAU/USD" (mapping lives HERE, not in the caller)
  fetch_latest_price(symbol)       /price  -> {timestamp_utc, symbol, price, source}   (req #1)
  fetch_time_series(symbol, ...)   /time_series -> normalized OHLCV bars DataFrame
  bars_to_ticks(bars, symbol)      OHLC bars -> Bronze-schema synthetic ticks
  fetch_live_ticks(symbol, ...)    fetch_time_series + bars_to_ticks (what the pipeline calls)

Rate-limit guard (req #3): a process-wide throttle blocks so no more than ~1
Twelve Data call is issued per TWELVEDATA_MIN_INTERVAL_S (default 60s), so a
runaway in-process loop cannot burn through the daily credit budget. In the happy
path a pipeline run makes a single call and the throttle never fires.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger("engine.live_source")

# ── Configuration (env-overridable, mirroring the rest of the codebase) ─────────
TWELVEDATA_BASE_URL: str = os.getenv("TWELVEDATA_BASE_URL", "https://api.twelvedata.com")
DEFAULT_TIMEOUT: int = int(os.getenv("TWELVEDATA_TIMEOUT_S", "15"))
# Process-wide minimum spacing between Twelve Data calls. 0 disables (tests).
MIN_CALL_INTERVAL_S: float = float(os.getenv("TWELVEDATA_MIN_INTERVAL_S", "60"))
SOURCE_NAME = "twelvedata"

# Internal symbol convention -> Twelve Data format. Translation is handled INSIDE
# the adapter (req #2) so callers only ever speak the internal convention.
SYMBOL_MAP = {
    "XAUUSD": "XAU/USD",
    "XAGUSD": "XAG/USD",
    "XAUEUR": "XAU/EUR",
    "XAGEUR": "XAG/EUR",
}

# Bronze tick columns the synthetic ticks must expose so the Silver/Gold chain
# ("same as before") consumes them unchanged.
_BRONZE_TICK_COLS = ["timestamp_utc", "ask", "bid", "ask_volume", "bid_volume", "symbol"]

# Sub-bar offsets (seconds) for the 4 synthetic ticks. Must be strictly
# increasing and < 60 so all four stay inside a >=1min bar. open earliest,
# close latest => resample's arg_min/arg_max pick them correctly; high/low in the
# middle are picked by max/min regardless of order.
_TICK_OFFSETS = (0, 20, 40, 59)


class TwelveDataError(RuntimeError):
    """Raised on any Twelve Data failure: non-200, status:error, or missing fields."""


# ── Rate-limit guard (process-wide) ─────────────────────────────────────────────
_RATE_LOCK = threading.Lock()
_last_call_at: float = 0.0  # time.monotonic() of the last issued call


def _throttle() -> None:
    """
    Block until at least MIN_CALL_INTERVAL_S has elapsed since the last Twelve Data
    call in THIS process, then record the new call time. Holding the lock across
    the sleep serializes concurrent callers so the rate cap holds under threads.
    """
    global _last_call_at
    if MIN_CALL_INTERVAL_S <= 0:
        return
    with _RATE_LOCK:
        wait = _last_call_at + MIN_CALL_INTERVAL_S - time.monotonic()
        if wait > 0:
            logger.info(
                "Twelve Data rate-limit guard: sleeping %.1fs (max ~1 call / %.0fs per process)",
                wait, MIN_CALL_INTERVAL_S,
            )
            time.sleep(wait)
        _last_call_at = time.monotonic()


def reset_rate_limiter() -> None:
    """Clear the throttle's last-call marker (test helper)."""
    global _last_call_at
    with _RATE_LOCK:
        _last_call_at = 0.0


# ── Credentials & symbol mapping ────────────────────────────────────────────────
def _get_api_key() -> str:
    """
    Read TWELVEDATA_API_KEY from the environment. A local .env is loaded via
    python-dotenv when available (optional — CI injects the secret directly), so
    this module works even when imported without the pipeline's load_env().
    """
    if not os.getenv("TWELVEDATA_API_KEY"):
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            logger.debug("python-dotenv not installed; using ambient environment only")
    key = os.getenv("TWELVEDATA_API_KEY")
    if not key:
        raise TwelveDataError(
            "TWELVEDATA_API_KEY is not set. Add it to .env (local) or the CI secret store."
        )
    return key


def to_twelvedata_symbol(symbol: str) -> str:
    """
    Map an internal symbol to Twelve Data's format. XAUUSD -> 'XAU/USD'. Symbols
    already containing '/' pass through; unmapped 6-char codes fall back to a
    3/3 split (EURUSD -> 'EUR/USD').
    """
    s = symbol.upper().strip()
    if s in SYMBOL_MAP:
        return SYMBOL_MAP[s]
    if "/" in s:
        return s
    if len(s) == 6 and s.isalpha():
        return f"{s[:3]}/{s[3:]}"
    return s  # let Twelve Data reject anything genuinely unknown, with its own message


# ── HTTP ────────────────────────────────────────────────────────────────────────
def _scrub(text: str, secret: str) -> str:
    """Redact the API key from any text before it reaches a log or exception."""
    return text.replace(secret, "***") if secret and text else text


def _get_json(path: str, params: dict, api_key: str, session, timeout: int) -> dict:
    """
    GET a Twelve Data endpoint and return the parsed JSON. Raises TwelveDataError
    on network failure, non-200, non-JSON, or a body-level {"status": "error"}
    (Twelve Data reports logical errors that way even under HTTP 200). The API key
    is scrubbed from every error message so it never lands in logs.
    """
    url = TWELVEDATA_BASE_URL.rstrip("/") + path
    sess = session or requests
    try:
        resp = sess.get(url, params=params, timeout=timeout)
    except requests.RequestException as exc:
        raise TwelveDataError(f"network error calling {path}: {_scrub(str(exc), api_key)}") from exc

    if resp.status_code != 200:
        raise TwelveDataError(
            f"{path} returned HTTP {resp.status_code}: {_scrub(resp.text[:300], api_key)}"
        )
    try:
        data = resp.json()
    except ValueError as exc:
        raise TwelveDataError(f"{path} returned non-JSON: {_scrub(resp.text[:300], api_key)}") from exc

    if isinstance(data, dict) and data.get("status") == "error":
        raise TwelveDataError(
            f"{path} error (code={data.get('code')}): {_scrub(str(data.get('message')), api_key)}"
        )
    return data


# ── /price (req #1) ─────────────────────────────────────────────────────────────
def fetch_latest_price(
    symbol: str,
    *,
    session: "requests.Session | None" = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict:
    """
    Fetch the current price for *symbol* from Twelve Data's /price endpoint.

    Returns {timestamp_utc, symbol, price, source}. /price carries no timestamp,
    so timestamp_utc is stamped at fetch time (UTC-aware). Raises TwelveDataError
    on a non-200 response or a missing/non-numeric 'price' field — errors are
    never silently swallowed.
    """
    api_key = _get_api_key()
    params = {"symbol": to_twelvedata_symbol(symbol), "apikey": api_key}

    _throttle()
    data = _get_json("/price", params, api_key, session, timeout)

    if "price" not in data:
        raise TwelveDataError(f"/price for {symbol!r} returned no 'price' field: {data}")
    try:
        price = float(data["price"])
    except (TypeError, ValueError) as exc:
        raise TwelveDataError(
            f"/price for {symbol!r} returned non-numeric price {data.get('price')!r}"
        ) from exc

    return {
        "timestamp_utc": datetime.now(timezone.utc),
        "symbol": symbol.upper(),
        "price": price,
        "source": SOURCE_NAME,
    }


# ── /time_series (the live feed) ────────────────────────────────────────────────
def fetch_time_series(
    symbol: str,
    interval: str = "1min",
    outputsize: int = 500,
    *,
    session: "requests.Session | None" = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """
    Fetch recent OHLC(V) bars for *symbol* from Twelve Data's /time_series.

    Requests UTC datetimes in ascending order. Returns a DataFrame with columns
    [timestamp_utc (datetime64[ms, UTC]), open, high, low, close, volume, symbol],
    sorted ascending. volume is 0.0 when Twelve Data omits it (typical for
    forex/metals such as XAU/USD). Raises TwelveDataError on any API failure or an
    empty 'values' array.
    """
    api_key = _get_api_key()
    params = {
        "symbol": to_twelvedata_symbol(symbol),
        "interval": interval,
        "outputsize": int(outputsize),
        "timezone": "UTC",   # unambiguous UTC datetimes
        "order": "ASC",      # oldest -> newest
        "apikey": api_key,
    }

    _throttle()
    data = _get_json("/time_series", params, api_key, session, timeout)

    values = data.get("values") if isinstance(data, dict) else None
    if not values:
        raise TwelveDataError(f"/time_series for {symbol!r} returned no 'values': {data}")

    df = pd.DataFrame(values)
    df["timestamp_utc"] = pd.to_datetime(df["datetime"], utc=True).dt.as_unit("ms")
    for col in ("open", "high", "low", "close"):
        df[col] = df[col].astype(float)
    df["volume"] = df["volume"].astype(float) if "volume" in df.columns else 0.0
    df["symbol"] = symbol.upper()

    return (
        df[["timestamp_utc", "open", "high", "low", "close", "volume", "symbol"]]
        .sort_values("timestamp_utc")
        .reset_index(drop=True)
    )


def bars_to_ticks(bars: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """
    Explode OHLC bars into Bronze-schema synthetic ticks (four per bar) so the
    existing Silver/Gold chain reconstructs the same candles.

    For each bar, four ticks are emitted at increasing sub-bar offsets carrying
    open / high / low / close as the price. bid==ask==price (Silver's derive_price
    mid then equals the OHLC price; /time_series has no spread). The bar's volume
    rides on the close tick's bid_volume so Gold's sum(bid_volume + ask_volume)
    over the bucket reproduces it exactly (0 for volumeless forex/metals).
    """
    sym = symbol.upper()
    if bars.empty:
        return pd.DataFrame({c: pd.Series(dtype=t) for c, t in {
            "timestamp_utc": "datetime64[ms, UTC]", "ask": "float64", "bid": "float64",
            "ask_volume": "float64", "bid_volume": "float64", "symbol": "object",
        }.items()})

    price = bars[["open", "high", "low", "close"]].astype(float)
    vol = bars["volume"].astype(float)
    # (price column, sub-bar offset, whether this tick carries the bar's volume)
    specs = [("open", 0, False), ("high", 1, False), ("low", 2, False), ("close", 3, True)]

    frames = []
    for col, idx, carry_vol in specs:
        frames.append(pd.DataFrame({
            "timestamp_utc": bars["timestamp_utc"] + pd.Timedelta(seconds=_TICK_OFFSETS[idx]),
            "ask": price[col].to_numpy(),
            "bid": price[col].to_numpy(),
            "ask_volume": 0.0,
            "bid_volume": vol.to_numpy() if carry_vol else 0.0,
            "symbol": sym,
        }))

    out = pd.concat(frames, ignore_index=True).sort_values("timestamp_utc").reset_index(drop=True)
    out["timestamp_utc"] = out["timestamp_utc"].dt.as_unit("ms")
    return out[_BRONZE_TICK_COLS]


def fetch_live_ticks(
    symbol: str,
    interval: str = "1min",
    outputsize: int = 500,
    *,
    session: "requests.Session | None" = None,
) -> pd.DataFrame:
    """
    Convenience: fetch recent /time_series bars and return them as Bronze-schema
    synthetic ticks ready to feed the Silver/Gold chain. This is the single entry
    point the pipeline's live mode calls.
    """
    bars = fetch_time_series(symbol, interval=interval, outputsize=outputsize, session=session)
    ticks = bars_to_ticks(bars, symbol)
    logger.info(
        "Twelve Data live: %d %s bars -> %d synthetic ticks for %s (span %s -> %s)",
        len(bars), interval, len(ticks), symbol.upper(),
        bars["timestamp_utc"].min(), bars["timestamp_utc"].max(),
    )
    return ticks


# ── Manual smoke test ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    import json

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s | %(message)s")
    ap = argparse.ArgumentParser(description="Twelve Data live source smoke test")
    ap.add_argument("--symbol", default="XAUUSD")
    ap.add_argument("--interval", default="1min")
    ap.add_argument("--outputsize", type=int, default=5)
    ap.add_argument("--price-only", action="store_true", help="only call /price")
    args = ap.parse_args()

    print("map:", args.symbol, "->", to_twelvedata_symbol(args.symbol))
    print("latest:", json.dumps(fetch_latest_price(args.symbol), default=str))
    if not args.price_only:
        ticks = fetch_live_ticks(args.symbol, interval=args.interval, outputsize=args.outputsize)
        print(f"synthetic ticks ({len(ticks)}):")
        print(ticks.head(12).to_string(index=False))
