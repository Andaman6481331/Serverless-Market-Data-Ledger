"""
Engine — end-to-end orchestration (Bronze -> Silver -> Gold -> R2 + D1)

Ties the three layers into one runnable pipeline. Given a symbol and a time
range it:

  1. Bronze  — HistoryDownloader fetches Dukascopy tick data to local Parquet
               (per-hour files), then each day's ticks are merged and uploaded
               to Cloudflare R2 under bronze/symbol=/year=/month=/day=/.
  2. Silver  — _ensure_utc (defensive column normalise) -> derive_price ->
               deduplicate_ticks.
  3. Gold    — resample_to_ohlc -> compute_atr -> detect_gaps -> classify_gaps.
  4. D1      — Gold candles (with ATR) are upserted into Cloudflare D1 via its
               HTTP query API (plain `requests`, no SDK).

Design decisions (locked with the project owner):

  * bar_time is stored in D1 as INTEGER epoch-milliseconds (UTC, bucket start).
  * Idempotency: the D1 table's PRIMARY KEY is (symbol, timeframe, bar_time) and
    writes use INSERT ... ON CONFLICT DO UPDATE, so re-running over an
    overlapping range refreshes existing candles instead of duplicating them.
    DO UPDATE (not DO NOTHING) is deliberate: a trailing candle written from an
    incomplete final hour gets corrected once more ticks arrive (matters for the
    future live-poll mode).
  * D1 writes use literal-value batching: candles become INSERT statements with
    numeric literals + whitelist-validated symbol/timeframe strings (no bound
    params), chunked under D1's ~100 KB per-statement cap. This is far fewer
    round-trips than the ~100-bound-parameter cap would allow. Every literal is
    either an int/float rendered by Python (round-trippable) or a string matched
    against a strict `^[A-Z0-9]+$` / `^[0-9]+[a-z]+$` whitelist, so no untrusted
    text ever reaches the SQL.

Entry point (req #5): a single mode-agnostic `run_pipeline(...)` core, wrapped by
a CLI that accepts --mode {backfill,live} and an explicit/derived time range —
neither behaviour is hardcoded. The live *data source* is intentionally NOT built
yet; --mode live currently resolves a recent window and reuses the historical
HistoryDownloader path so the R2/D1 plumbing is exercised. Swap the source in
`_resolve_range` / the bronze stage when a live API is chosen.

Run:
  py -3.11 -m engine.pipeline --symbol XAUUSD --start 2015-01-01 --end 2015-01-31
  py -3.11 -m engine.pipeline --symbol XAUUSD --start 2015-01-01 --end 2015-01-05 --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging
import math
import os
import random
import re
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from bronze.history_downloader import HistoryDownloader, BRONZE_SCHEMA
from silver.cleaning import _ensure_utc, derive_price, deduplicate_ticks
from silver.gaps import detect_gaps
from gold.resample import resample_to_ohlc
from gold.atr import compute_atr
from gold.session_calendar import classify_gaps

logger = logging.getLogger("engine.pipeline")

# ── Credentials ───────────────────────────────────────────────────────────────
R2_ENV_KEYS = ["R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT_URL", "R2_BUCKET_NAME"]
D1_ENV_KEYS = ["D1_ACCOUNT_ID", "D1_DATABASE_ID", "D1_API_TOKEN"]
TWELVEDATA_ENV_KEYS = ["TWELVEDATA_API_KEY"]


def load_env(require_r2: bool = True, require_d1: bool = True, require_twelvedata: bool = False) -> dict:
    """
    Load credentials from the environment. python-dotenv is used to read a local
    .env if present, but is optional — GitHub Actions injects the real env vars
    directly, so a missing dotenv package is not an error.

    Raises SystemExit listing every missing variable so misconfiguration fails
    loudly before any network work begins.
    """
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        logger.debug("python-dotenv not installed; using ambient environment only")

    cfg = {k: os.getenv(k) for k in (R2_ENV_KEYS + D1_ENV_KEYS + TWELVEDATA_ENV_KEYS)}
    missing: list[str] = []
    if require_r2:
        missing += [k for k in R2_ENV_KEYS if not cfg.get(k)]
    if require_d1:
        missing += [k for k in D1_ENV_KEYS if not cfg.get(k)]
    if require_twelvedata:
        missing += [k for k in TWELVEDATA_ENV_KEYS if not cfg.get(k)]
    if missing:
        raise SystemExit(
            "Missing required environment variable(s): " + ", ".join(missing) +
            "\nSet them in .env (local) or the CI secret store, or pass --dry-run "
            "to run the transform chain without writing to R2/D1."
        )
    return cfg


# ══════════════════════════════════════════════════════════════════════════════
# R2 (Bronze) — S3-compatible upload via boto3
# ══════════════════════════════════════════════════════════════════════════════
class R2Uploader:
    """
    Thin wrapper over an S3-compatible client pointed at Cloudflare R2. boto3 is
    the standard S3 client; the project's "use requests, not a heavy SDK" rule
    was scoped to D1, where the surface is a single HTTP endpoint. boto3 is
    imported lazily so --dry-run works on a host without it installed.
    """

    def __init__(self, cfg: dict):
        import boto3  # lazy: only needed when actually writing to R2
        self.bucket = cfg["R2_BUCKET_NAME"]
        self.client = boto3.client(
            "s3",
            endpoint_url=cfg["R2_ENDPOINT_URL"],
            aws_access_key_id=cfg["R2_ACCESS_KEY_ID"],
            aws_secret_access_key=cfg["R2_SECRET_ACCESS_KEY"],
            region_name="auto",  # R2 ignores region but boto3 requires one
        )

    def put_parquet(self, key: str, df: pd.DataFrame, schema: pa.Schema) -> int:
        """Serialise *df* to snappy Parquet in memory and PUT it at *key*.
        Returns the object size in bytes."""
        buf = io.BytesIO()
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        pq.write_table(table, buf, compression="snappy")
        body = buf.getvalue()
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body)
        return len(body)


def _r2_bronze_key(symbol: str, d: date) -> str:
    """Canonical R2 object key for a day's merged Bronze ticks."""
    return (
        f"bronze/symbol={symbol}"
        f"/year={d.year}"
        f"/month={d.month:02d}"
        f"/day={d.day:02d}"
        f"/ticks.parquet"
    )


# ══════════════════════════════════════════════════════════════════════════════
# D1 (Gold) — Cloudflare D1 HTTP query API
# ══════════════════════════════════════════════════════════════════════════════
_D1_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS gold_candles (
    symbol      TEXT    NOT NULL,
    timeframe   TEXT    NOT NULL,
    bar_time    INTEGER NOT NULL,          -- epoch milliseconds, UTC, bucket START
    bar_open    REAL    NOT NULL,
    bar_high    REAL    NOT NULL,
    bar_low     REAL    NOT NULL,
    bar_close   REAL    NOT NULL,
    bar_volume  REAL    NOT NULL,
    atr         REAL,                       -- NULL during the ATR warm-up window
    ingested_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000),
    PRIMARY KEY (symbol, timeframe, bar_time)
)
""".strip()

_D1_INSERT_HEAD = (
    "INSERT INTO gold_candles "
    "(symbol,timeframe,bar_time,bar_open,bar_high,bar_low,bar_close,bar_volume,atr) VALUES "
)
_D1_ON_CONFLICT = (
    " ON CONFLICT(symbol,timeframe,bar_time) DO UPDATE SET "
    "bar_open=excluded.bar_open,"
    "bar_high=excluded.bar_high,"
    "bar_low=excluded.bar_low,"
    "bar_close=excluded.bar_close,"
    "bar_volume=excluded.bar_volume,"
    "atr=excluded.atr"
)
# Headroom under D1's ~100 KB per-statement limit; the statement is pure ASCII so
# character count == byte count.
_D1_MAX_SQL_BYTES = 90_000

# Whitelists — the ONLY strings interpolated into SQL. Anything outside these
# character classes is rejected, so no quote/escape handling is needed.
_SYMBOL_RE = re.compile(r"^[A-Z0-9]{1,20}$")
_TIMEFRAME_RE = re.compile(r"^[0-9]{1,4}[a-z]{1,10}$")


class D1Error(RuntimeError):
    """Raised when a D1 HTTP query returns success=false or a non-recoverable HTTP status."""


class D1Client:
    """Minimal Cloudflare D1 HTTP client (query endpoint), using `requests`."""

    def __init__(self, cfg: dict, timeout: int = 30, max_retries: int = 4):
        self.url = (
            f"https://api.cloudflare.com/client/v4/accounts/{cfg['D1_ACCOUNT_ID']}"
            f"/d1/database/{cfg['D1_DATABASE_ID']}/query"
        )
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {cfg['D1_API_TOKEN']}",
            "Content-Type": "application/json",
        })

    def query(self, sql: str, params: Optional[list] = None) -> list:
        """
        POST one SQL statement (optionally with bound params) to D1. Retries
        transient HTTP 429/5xx and network errors with jittered backoff. Returns
        the `result` array on success; raises D1Error otherwise.
        """
        payload: dict = {"sql": sql}
        if params is not None:
            payload["params"] = params

        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                resp = self.session.post(self.url, json=payload, timeout=self.timeout)
            except requests.RequestException as exc:
                last_err = exc
                logger.warning("D1 network error (attempt %d/%d): %s",
                               attempt + 1, self.max_retries, exc)
                self._backoff(attempt)
                continue

            if resp.status_code in (429, 500, 502, 503, 504):
                last_err = D1Error(f"HTTP {resp.status_code}: {resp.text[:300]}")
                logger.warning("D1 transient HTTP %s (attempt %d/%d)",
                               resp.status_code, attempt + 1, self.max_retries)
                self._backoff(attempt)
                continue

            try:
                data = resp.json()
            except ValueError:
                raise D1Error(f"D1 returned non-JSON (HTTP {resp.status_code}): {resp.text[:300]}")

            if resp.status_code >= 400 or not data.get("success", False):
                raise D1Error(
                    f"D1 query failed (HTTP {resp.status_code}): "
                    f"{data.get('errors') or resp.text[:300]}"
                )
            return data.get("result", [])

        raise D1Error(f"D1 query failed after {self.max_retries} attempts: {last_err}")

    @staticmethod
    def _backoff(attempt: int) -> None:
        wait = min(2 ** attempt, 8) + random.uniform(0, 1.0)
        time.sleep(wait)

    def ensure_table(self) -> None:
        """Create gold_candles if it does not exist (idempotent, cheap)."""
        self.query(_D1_CREATE_TABLE)
        logger.info("D1: gold_candles table ensured")

    def upsert_candles(self, candles: pd.DataFrame, symbol: str, timeframe: str) -> int:
        """
        Upsert every candle in *candles* into gold_candles using literal-value
        batched INSERT ... ON CONFLICT DO UPDATE. Returns the total rows written
        (inserted + updated) as reported by D1's meta.
        """
        values = _candle_value_literals(candles, symbol, timeframe)
        if not values:
            return 0

        total = 0
        batches = 0
        for chunk in _chunk_by_size(
            values, len(_D1_INSERT_HEAD), len(_D1_ON_CONFLICT), _D1_MAX_SQL_BYTES
        ):
            sql = _D1_INSERT_HEAD + ",".join(chunk) + _D1_ON_CONFLICT
            result = self.query(sql)
            total += _rows_upserted(result)
            batches += 1
        logger.info("D1: upserted %d candle rows in %d batch(es)", total, batches)
        return total


def _sql_ident(value: str, pattern: re.Pattern, kind: str) -> str:
    """Validate *value* against a whitelist pattern; return it ready to single-quote."""
    if not isinstance(value, str) or not pattern.match(value):
        raise ValueError(
            f"Refusing to build SQL: {kind}={value!r} is not a valid identifier "
            f"(must match {pattern.pattern})."
        )
    return value


def _num_literal(x) -> str:
    """Render a finite float as a round-trippable SQL numeric literal."""
    f = float(x)
    if not math.isfinite(f):
        raise ValueError(f"non-finite OHLCV value {x!r} cannot be written to D1")
    return repr(f)


def _atr_literal(x) -> str:
    """ATR literal — NaN/inf (warm-up or bad input) becomes SQL NULL."""
    f = float(x)
    return repr(f) if math.isfinite(f) else "NULL"


def _candle_value_literals(candles: pd.DataFrame, symbol: str, timeframe: str) -> list[str]:
    """
    Turn each candle row into a `(...)` VALUES literal string. symbol/timeframe
    are whitelist-validated once; bar_time is converted to epoch-ms int; OHLCV
    are rendered as round-trippable numeric literals; NaN ATR becomes NULL.
    """
    sym = _sql_ident(symbol, _SYMBOL_RE, "symbol")
    tf = _sql_ident(timeframe, _TIMEFRAME_RE, "timeframe")

    # datetime64[ms, UTC] -> epoch-ms int64 (to_numpy converts to UTC then drops tz).
    bar_ms = candles["bar_time"].to_numpy(dtype="datetime64[ms]").astype("int64")
    o = candles["bar_open"].to_numpy()
    h = candles["bar_high"].to_numpy()
    lo = candles["bar_low"].to_numpy()
    c = candles["bar_close"].to_numpy()
    v = candles["bar_volume"].to_numpy()
    a = candles["atr"].to_numpy() if "atr" in candles.columns else [float("nan")] * len(candles)

    rows: list[str] = []
    for i in range(len(candles)):
        rows.append(
            f"('{sym}','{tf}',{int(bar_ms[i])},"
            f"{_num_literal(o[i])},{_num_literal(h[i])},{_num_literal(lo[i])},"
            f"{_num_literal(c[i])},{_num_literal(v[i])},{_atr_literal(a[i])})"
        )
    return rows


def _chunk_by_size(values: list[str], head_len: int, tail_len: int, max_bytes: int) -> Iterator[list[str]]:
    """Yield lists of value-literals whose joined SQL stays under *max_bytes*."""
    chunk: list[str] = []
    size = head_len + tail_len
    for v in values:
        extra = len(v) + (1 if chunk else 0)  # +1 for the joining comma
        if chunk and size + extra > max_bytes:
            yield chunk
            chunk, size = [], head_len + tail_len
            extra = len(v)
        chunk.append(v)
        size += extra
    if chunk:
        yield chunk


def _rows_upserted(result: list) -> int:
    """
    Logical rows inserted+updated by a D1 upsert — read from meta.changes, NOT
    meta.rows_written.

    meta.rows_written counts *physical* b-tree writes across the table AND every
    index. gold_candles is a rowid table with a composite (non-INTEGER) PRIMARY
    KEY, so SQLite/D1 maintains a separate unique index on
    (symbol, timeframe, bar_time): each inserted row is one table write + one
    index write = 2 rows_written. Verified against live D1 — a single fresh
    upsert returns {changes: 1, rows_written: 2}. meta.changes is the SQLite
    changes() count (inserted + updated logical rows), which is what "rows
    upserted" means here.
    """
    if result and isinstance(result, list):
        meta = result[0].get("meta", {}) if isinstance(result[0], dict) else {}
        val = meta.get("changes")
        if val is None:                       # defensive fallback only
            val = meta.get("rows_written", 0)
        return int(val or 0)
    return 0


# ══════════════════════════════════════════════════════════════════════════════
# Run summary / structured staging
# ══════════════════════════════════════════════════════════════════════════════
_PLAN = ["bronze_download", "bronze_to_r2", "silver_clean", "gold_resample", "gold_gaps", "d1_upsert"]


@dataclass
class Stage:
    name: str
    status: str = "pending"   # pending | ok | fail | skipped
    elapsed_s: float = 0.0
    detail: str = ""


@dataclass
class RunSummary:
    symbol: str
    timeframe: str
    mode: str
    start: str
    end: str
    dry_run: bool
    stages: dict[str, Stage] = field(default_factory=lambda: {n: Stage(n) for n in _PLAN})
    ticks_ingested: int = 0
    candles_produced: int = 0
    gaps_found: int = 0
    gaps_expected: int = 0
    gaps_real: int = 0
    r2_objects: int = 0
    r2_rows: int = 0
    d1_rows: int = 0
    reconcile_detail: str = ""
    notes: list[str] = field(default_factory=list)
    elapsed_s: float = 0.0

    @contextmanager
    def stage(self, name: str):
        st = self.stages[name]
        t0 = time.perf_counter()
        logger.info(">> %s", name)
        try:
            yield st
            if st.status == "pending":
                st.status = "ok"
        except Exception as exc:
            st.status = "fail"
            if not st.detail:
                st.detail = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            st.elapsed_s = time.perf_counter() - t0
            logger.info(
                "    %s: %s (%.2fs)%s",
                name, st.status.upper(), st.elapsed_s,
                f" - {st.detail}" if st.detail else "",
            )

    @property
    def failed(self) -> bool:
        return any(s.status == "fail" for s in self.stages.values())

    def finalize_pending(self) -> None:
        for s in self.stages.values():
            if s.status == "pending":
                s.status = "skipped"

    def log_summary(self) -> None:
        bar = "=" * 64
        lines = [
            bar,
            "RUN SUMMARY",
            f"  symbol={self.symbol}  timeframe={self.timeframe}  mode={self.mode}"
            f"  range={self.start}..{self.end}  dry_run={self.dry_run}",
            "  stages:",
        ]
        for name in _PLAN:
            s = self.stages[name]
            lines.append(
                f"    {name:<16} {s.status.upper():<8} {s.elapsed_s:6.2f}s"
                + (f"  {s.detail}" if s.detail else "")
            )
        lines += [
            "  metrics:",
            f"    ticks_ingested    {self.ticks_ingested:>12,}",
            f"    candles_produced  {self.candles_produced:>12,}",
            f"    gaps_found        {self.gaps_found:>12,}  (expected={self.gaps_expected} real={self.gaps_real})",
            f"    r2_written        {self.r2_objects:>12,} objects / {self.r2_rows:,} tick-rows",
            f"    d1_rows_upserted  {self.d1_rows:>12,}",
        ]
        if self.reconcile_detail:
            lines.append(f"    reconcile         {self.reconcile_detail}")
        for note in self.notes:
            lines.append(f"  note: {note}")
        lines += [
            f"  elapsed: {self.elapsed_s:.2f}s",
            f"  result: {'FAIL' if self.failed else 'PASS'}",
            bar,
        ]
        logger.info("\n" + "\n".join(lines))


# ══════════════════════════════════════════════════════════════════════════════
# Bronze helpers
# ══════════════════════════════════════════════════════════════════════════════
def _iter_days(start: str, end: str) -> Iterator[date]:
    d = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    while d <= e:
        yield d
        d += timedelta(days=1)


def _read_day_ticks(bronze_dir: str, symbol: str, d: date) -> Optional[pd.DataFrame]:
    """
    Read and merge all per-hour Bronze Parquet files HistoryDownloader wrote for
    a single day. Returns None if that day has no files (weekend / holiday / no
    data). Exact full-row duplicates (re-downloaded hours) are dropped; genuine
    tick identity is left to the Silver layer.
    """
    part = Path(bronze_dir) / symbol / f"year={d.year}" / f"month={d.month:02d}"
    files = sorted(part.glob(f"{d.day:02d}_[0-9][0-9].parquet"))
    if not files:
        return None
    frames = [pq.read_table(str(f)).to_pandas() for f in files]
    return (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates()
        .sort_values("timestamp_utc")
        .reset_index(drop=True)
    )


# ══════════════════════════════════════════════════════════════════════════════
# Reconciliation guard
# ══════════════════════════════════════════════════════════════════════════════
def _reconcile_d1_counts(summary: RunSummary) -> None:
    """
    Compare rows upserted to D1 against candles produced, as a divergence guard.

    Because d1_rows is read from meta.changes (inserted + updated logical rows),
    a full upsert of N candles reports N whether the rows are new (fresh backfill)
    or already present (backfill re-run) — so on backfill the two SHOULD match. A
    mismatch means rows silently went missing (a partial or dropped batch) and is
    worth surfacing.

    Informational only — this NEVER fails the run. On a live/incremental run the
    comparison isn't treated as meaningful, so a divergence is logged at INFO
    rather than WARNING. Callers should only invoke this after a real (non-dry-run)
    D1 write actually happened.
    """
    produced, written = summary.candles_produced, summary.d1_rows
    delta = written - produced
    if delta == 0:
        summary.reconcile_detail = f"OK ({written:,} rows == {produced:,} candles)"
        logger.info("D1 reconciliation OK: %d rows upserted == %d candles produced", written, produced)
        return

    summary.reconcile_detail = f"MISMATCH d1={written:,} vs candles={produced:,} (delta {delta:+,})"
    if summary.mode == "backfill":
        logger.warning(
            "D1 reconciliation MISMATCH on backfill: %d rows upserted != %d candles produced "
            "(delta %+d). These should match on a fresh or overlapping backfill — check for a "
            "partial or dropped batch write. (Not failing the run.)",
            written, produced, delta,
        )
    else:
        logger.info(
            "D1 reconciliation: %d rows upserted vs %d candles produced (delta %+d) — "
            "informational in %s mode (existing candles update rather than insert).",
            written, produced, delta, summary.mode,
        )


# ══════════════════════════════════════════════════════════════════════════════
# Core pipeline
# ══════════════════════════════════════════════════════════════════════════════
def run_pipeline(
    *,
    symbol: str,
    start: str,
    end: str,
    timeframe: str = "5min",
    atr_period: int = 14,
    mode: str = "backfill",
    bronze_dir: str = "data/bronze",
    cfg: Optional[dict] = None,
    dry_run: bool = False,
    max_concurrent: int = 2,
    live_interval: str = "1min",
    live_outputsize: int = 500,
) -> RunSummary:
    """
    Run the full Bronze->Silver->Gold->R2/D1 chain for one symbol + date range.

    Returns a RunSummary (also logged). On the first stage failure the chain
    stops, remaining stages are marked 'skipped', and the summary still prints.

    The Bronze *source* is the only mode-dependent part: backfill downloads
    Dukascopy ticks (HistoryDownloader) and archives them to R2; live pulls recent
    OHLC bars from Twelve Data (engine.live_source) and synthesizes ticks in
    memory. Everything from Silver onward is identical for both modes. live_interval
    / live_outputsize are ignored in backfill mode.
    """
    symbol = symbol.upper()
    if mode == "live":
        # Fail fast before any network/credit spend on a misconfigured interval.
        _validate_live_intervals(live_interval, timeframe)
    summary = RunSummary(symbol, timeframe, mode, start, end, dry_run)
    t0 = time.perf_counter()

    ticks: Optional[pd.DataFrame] = None
    candles: Optional[pd.DataFrame] = None

    try:
        if mode == "live":
            # ── Bronze (live): pull recent OHLC from Twelve Data, synthesize ticks ──
            with summary.stage("bronze_download") as st:
                # Lazy import keeps the backfill path free of the live adapter and
                # its (optional) dependencies.
                from engine.live_source import fetch_live_ticks
                ticks = fetch_live_ticks(symbol, interval=live_interval, outputsize=live_outputsize)
                if ticks.empty:
                    raise RuntimeError(
                        f"Twelve Data returned no live bars for {symbol} "
                        f"(interval={live_interval}, outputsize={live_outputsize})."
                    )
                summary.ticks_ingested = len(ticks)
                # Reflect the true fetched span in the summary header.
                lo = _ensure_utc(ticks["timestamp_utc"].min())
                hi = _ensure_utc(ticks["timestamp_utc"].max())
                summary.start, summary.end = f"{lo:%Y-%m-%d}", f"{hi:%Y-%m-%d}"
                st.detail = (
                    f"twelvedata {live_interval} x{live_outputsize} -> "
                    f"{len(ticks):,} synthetic ticks, span {lo:%Y-%m-%d %H:%M} -> {hi:%Y-%m-%d %H:%M}Z"
                )

            # ── Bronze -> R2 (live): held in memory; archival intentionally skipped ─
            with summary.stage("bronze_to_r2") as st:
                # TODO(live-archive): a live run fetches only a recent PARTIAL day.
                # PUTting it under the per-day Bronze key (bronze/.../day=/ticks.parquet)
                # would overwrite a fully-backfilled day with a sparse slice. Archiving
                # live Bronze safely needs a read-existing -> merge -> write strategy
                # (or a separate live key prefix) — deliberately deferred, not a bug.
                st.status = "skipped"
                st.detail = f"{len(ticks):,} live ticks held in memory (R2 archival skipped in live mode)"
                summary.notes.append(
                    "live Bronze->R2 archival intentionally SKIPPED — a partial-day live "
                    "window would clobber backfilled Bronze without a merge strategy "
                    "(separate task; live ticks still flow to Silver/Gold/D1)."
                )
        else:
            # ── Bronze: download raw ticks to local Parquet ─────────────────────────
            with summary.stage("bronze_download") as st:
                downloader = HistoryDownloader(
                    symbol=symbol, output_dir=bronze_dir, max_concurrent=max_concurrent
                )
                dl = asyncio.run(downloader.download_range(start, end))
                st.detail = (
                    f"saved={dl['ticks_saved']:,} files={dl['files_written']} "
                    f"resumed={dl['hours_resumed']} skipped={dl['hours_skipped']}"
                )

            # ── Bronze -> R2: merge each day, upload, and assemble the working set ──
            with summary.stage("bronze_to_r2") as st:
                r2 = None if dry_run else R2Uploader(cfg)  # type: ignore[arg-type]
                frames: list[pd.DataFrame] = []
                for d in _iter_days(start, end):
                    day_df = _read_day_ticks(bronze_dir, symbol, d)
                    if day_df is None or day_df.empty:
                        continue
                    frames.append(day_df)
                    if r2 is not None:
                        key = _r2_bronze_key(symbol, d)
                        nbytes = r2.put_parquet(key, day_df, BRONZE_SCHEMA)
                        summary.r2_objects += 1
                        summary.r2_rows += len(day_df)
                        logger.info("R2 PUT %s (%d ticks, %d bytes)", key, len(day_df), nbytes)

                if not frames:
                    raise RuntimeError(
                        f"No Bronze ticks found for {symbol} in {start}..{end} — "
                        "nothing downloaded (all weekends/404s?)."
                    )
                ticks = pd.concat(frames, ignore_index=True)
                summary.ticks_ingested = len(ticks)
                st.detail = (
                    f"{summary.r2_objects} objects / {summary.r2_rows:,} ticks uploaded"
                    if not dry_run else f"{len(ticks):,} ticks loaded (dry-run: R2 upload skipped)"
                )

        # ── Silver: normalise UTC -> derive mid price -> deduplicate ───────────
        with summary.stage("silver_clean") as st:
            assert ticks is not None
            # Defensive column-level UTC guarantee (Bronze already stamps UTC).
            ticks["timestamp_utc"] = pd.to_datetime(ticks["timestamp_utc"], utc=True)
            ticks = derive_price(ticks)
            ticks = deduplicate_ticks(ticks)
            lo = _ensure_utc(ticks["timestamp_utc"].min())
            hi = _ensure_utc(ticks["timestamp_utc"].max())
            st.detail = f"{len(ticks):,} ticks after dedup, span {lo:%Y-%m-%d %H:%M} -> {hi:%Y-%m-%d %H:%M}Z"

        # ── Gold: resample to OHLC candles + append ATR ────────────────────────
        with summary.stage("gold_resample") as st:
            assert ticks is not None
            candles = resample_to_ohlc(ticks, timeframe=timeframe)
            candles = compute_atr(candles, period=atr_period)
            summary.candles_produced = len(candles)
            st.detail = f"{len(candles):,} {timeframe} candles, atr_period={atr_period}"

        # ── Gold: detect gaps + classify against the session calendar ──────────
        with summary.stage("gold_gaps") as st:
            assert candles is not None
            gaps = detect_gaps(candles, symbol=symbol, expected_freq=timeframe, timestamp_col="bar_time")
            gaps = classify_gaps(gaps)
            summary.gaps_found = len(gaps)
            summary.gaps_expected = int(gaps["expected"].sum()) if len(gaps) else 0
            summary.gaps_real = summary.gaps_found - summary.gaps_expected
            st.detail = f"{summary.gaps_found} gaps (expected={summary.gaps_expected}, real={summary.gaps_real})"

        # ── D1: upsert Gold candles ────────────────────────────────────────────
        with summary.stage("d1_upsert") as st:
            assert candles is not None
            if dry_run:
                st.status = "skipped"
                st.detail = f"dry-run: {len(candles):,} candles NOT written to D1"
            elif candles.empty:
                st.detail = "no candles to write"
            else:
                d1 = D1Client(cfg)  # type: ignore[arg-type]
                d1.ensure_table()
                summary.d1_rows = d1.upsert_candles(candles, symbol, timeframe)
                st.detail = f"{summary.d1_rows:,} rows upserted"

        # Reconciliation guard — only meaningful when a real D1 write happened.
        if not dry_run and summary.stages["d1_upsert"].status == "ok" and summary.candles_produced:
            _reconcile_d1_counts(summary)

    except Exception as exc:
        logger.error("Pipeline stopped: %s", exc, exc_info=logger.isEnabledFor(logging.DEBUG))

    summary.finalize_pending()
    summary.elapsed_s = time.perf_counter() - t0
    summary.log_summary()
    return summary


# ══════════════════════════════════════════════════════════════════════════════
# CLI / entry point
# ══════════════════════════════════════════════════════════════════════════════
def _validate_date(s: str) -> str:
    datetime.strptime(s, "%Y-%m-%d")  # raises ValueError on bad format
    return s


_INTERVAL_SECONDS = {
    "s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
    "min": 60, "mins": 60, "minute": 60, "minutes": 60, "t": 60,
    "h": 3600, "hr": 3600, "hrs": 3600, "hour": 3600, "hours": 3600,
    "d": 86400, "day": 86400, "days": 86400,
}


def _interval_seconds(tf: str) -> int:
    """Parse a pandas-style timeframe ('1min', '5min', '4h', '1d') to seconds."""
    m = re.fullmatch(r"\s*(\d+)\s*([a-zA-Z]+)\s*", tf)
    if not m:
        raise ValueError(f"Unrecognized timeframe {tf!r}; expected e.g. '1min', '5min', '4h', '1d'.")
    n, unit = int(m.group(1)), m.group(2).lower()
    if n <= 0:
        raise ValueError(f"Timeframe magnitude must be positive, got {n} in {tf!r}.")
    if unit not in _INTERVAL_SECONDS:
        raise ValueError(f"Unrecognized timeframe unit {unit!r} in {tf!r}; supported: s / min / h / d.")
    return n * _INTERVAL_SECONDS[unit]


def _validate_live_intervals(live_interval: str, timeframe: str) -> None:
    """
    Hard-fail if the live fetch interval is not an exact divisor of (and <=) the
    target timeframe. Otherwise synthetic ticks from a coarse fetch interval would
    land in only the first sub-bucket of each target candle, silently producing
    malformed OHLC. Fail loud at config time rather than emit bad candles.
    """
    iv, tf = _interval_seconds(live_interval), _interval_seconds(timeframe)
    if iv > tf or tf % iv != 0:
        raise ValueError(
            f"--live-interval {live_interval!r} ({iv}s) must be <= and an exact divisor of "
            f"--timeframe {timeframe!r} ({tf}s), or resampling produces malformed candles. "
            f"Pick a live-interval that divides the timeframe (e.g. 1min into 5min)."
        )


def _resolve_range(mode: str, start: Optional[str], end: Optional[str], lookback_days: int) -> tuple[str, str]:
    """
    Resolve (start, end) dates from the mode. backfill requires explicit dates.
    live derives a placeholder recent window only for the summary header — the
    live source (engine.live_source, Twelve Data /time_series) pulls the last
    live_outputsize bars rather than a date range, and run_pipeline overwrites
    start/end with the actual fetched span.
    """
    if mode == "backfill":
        if not (start and end):
            raise SystemExit("backfill mode requires --start and --end (YYYY-MM-DD).")
        return _validate_date(start), _validate_date(end)

    if mode == "live":
        today = datetime.now(timezone.utc).date()
        s = _validate_date(start) if start else (today - timedelta(days=lookback_days)).isoformat()
        e = _validate_date(end) if end else today.isoformat()
        logger.info(
            "live mode: sourcing recent OHLC from Twelve Data /time_series "
            "(window shown %s..%s is approximate; the fetched bar span is authoritative).", s, e,
        )
        return s, e

    raise SystemExit(f"unknown mode {mode!r}")


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(
        prog="engine.pipeline",
        description="Bronze->Silver->Gold->R2/D1 market-data pipeline.",
    )
    p.add_argument("--symbol", default="XAUUSD", help="Instrument symbol (default: XAUUSD)")
    p.add_argument("--mode", choices=["backfill", "live"], default="backfill",
                   help="backfill (explicit range) or live (recent window; placeholder source)")
    p.add_argument("--start", help="Range start YYYY-MM-DD (required for backfill)")
    p.add_argument("--end", help="Range end YYYY-MM-DD (required for backfill)")
    p.add_argument("--lookback-days", type=int, default=1,
                   help="live mode: days back from today when --start is omitted (default: 1)")
    p.add_argument("--live-interval", default="1min",
                   help="live mode: Twelve Data time_series interval to fetch (default: 1min). "
                        "Keep it <= --timeframe and a divisor of it so resampling stays correct.")
    p.add_argument("--live-outputsize", type=int, default=500,
                   help="live mode: number of bars to fetch from Twelve Data (default: 500). "
                        "Size it so at least --atr-period candles exist after resampling.")
    p.add_argument("--timeframe", default="5min", help="Candle width, pandas-style (default: 5min)")
    p.add_argument("--atr-period", type=int, default=14, help="ATR look-back in bars (default: 14)")
    p.add_argument("--bronze-dir", default="data/bronze", help="Local Bronze Parquet dir (default: data/bronze)")
    p.add_argument("--max-concurrent", type=int, default=2, help="Dukascopy download concurrency (default: 2)")
    p.add_argument("--dry-run", action="store_true",
                   help="Run the transform chain but skip R2/D1 writes (no creds needed)")
    p.add_argument("--log-level", default="INFO", help="Logging level (default: INFO)")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )

    start, end = _resolve_range(args.mode, args.start, args.end, args.lookback_days)
    if args.mode == "live":
        try:
            _validate_live_intervals(args.live_interval, args.timeframe)
        except ValueError as exc:
            raise SystemExit(str(exc))
    # Live mode sources Bronze from Twelve Data (needs the API key, skips R2
    # archival); backfill sources Dukascopy and archives to R2. Both write D1.
    if args.dry_run:
        cfg = None
    else:
        cfg = load_env(
            require_r2=(args.mode == "backfill"),
            require_d1=True,
            require_twelvedata=(args.mode == "live"),
        )

    summary = run_pipeline(
        symbol=args.symbol,
        start=start,
        end=end,
        timeframe=args.timeframe,
        atr_period=args.atr_period,
        mode=args.mode,
        bronze_dir=args.bronze_dir,
        cfg=cfg,
        dry_run=args.dry_run,
        max_concurrent=args.max_concurrent,
        live_interval=args.live_interval,
        live_outputsize=args.live_outputsize,
    )
    return 1 if summary.failed else 0


if __name__ == "__main__":
    sys.exit(main())
