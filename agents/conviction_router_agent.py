"""
Conviction Router Agent (v2) - routes approved allocations to the trade
structure whose empirically-validated edge matches the setup's character.

IMPORTANT METHODOLOGY NOTE: an earlier version of this docstring claimed a
74.4% WR for EQUITY_CONTINUATION based on a hand-reimplementation of the
screener's filter logic in a lab script. That reimplementation did not exactly
match pipeline/screener.py (real screener output is stage_2 UNION transition/
incubator, not pure stage_2), and when re-tested against the REAL screener
(scripts/real_screener_validation.py) the edge did NOT hold up (52.4% WR,
n=105). A follow-up attempt to preserve the edge by bypassing the critic/
entry-trigger/fundamental-audit gauntlet (to get more raw candidates) made
things WORSE, not better (105 raw candidates -> 52.4% WR vs 19
gauntlet-survived candidates -> 73.7% WR) - proof the existing gauntlet is
doing real quality-filtering work that must not be bypassed. Lesson: always
validate against the real production code path, never a reimplementation.

Current validated state:
  - OPTIONS_IGNITION (2x target / trail-10%-after-30%-cushion / -50% stop):
    breaking 2-day high + volume >= 1.5x + close in top 40% of range +
    market breadth >= 60-70% (of the universe above its own 20-SMA - this
    setup only has edge when the broad tape is participating, not just the
    single stock). Gauntlet-survived, June window: 73.7% WR (n=19).
    Entry timing: SAME-DAY, at/near the close of the signal day (the 3PM
    pulse runs while the market is still open until 15:30 IST), NOT
    next-day open. Time is the enemy of options (theta) - a paired test on
    the identical 19 setups, only changing entry timing, showed same-day
    entry beats next-day-open (78.9% vs 73.7% WR, both n=19). Requires a
    live intraday order during the pulse window instead of an AMO.

  - EQUITY_CONTINUATION: kept as a fallback classification (signal-day
    return 0-2%, volume < 1.5x, breadth >= 65%, RS rank >= 70th percentile)
    but its live sample size is too small (n=3 over 20 days) to trust any
    specific win rate for it. Do not tighten these thresholds further
    without a larger validated sample - the temptation to curve-fit a small
    sample is exactly what produced the false 74.4% result above.

CACHE/STALENESS NOTE: market breadth and RS-rank are computed FRESH from
daily_ohlcv once per run (in-memory, not file-cached) - there is no cache to
invalidate here. If you change these thresholds, no cache file needs
clearing; only backtest_fast_results*.json (prior BACKTEST OUTPUT, not input
caches) become stale and should be regenerated.
"""
import os
import csv
import logging
from typing import Dict, Any, Optional, List

import psycopg2
import polars as pl

from core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Equity-continuation profile (see module docstring for validation) ---
EQ_RET_MIN = 0.0
EQ_RET_MAX = 2.0
EQ_VOL_MAX = 1.5
EQ_BREADTH_MIN = 65.0
EQ_RS_MIN = 70.0

# --- Options-ignition profile ---
OPT_VOL_MIN = 1.5
OPT_CLOSE_RANGE_MIN = 0.6
OPT_BREADTH_MIN = 60.0

_MASTER_UNIVERSE_PATH = "pipeline/master_universe.csv"


def _get_conn():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'), port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'quant'),
        password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
        dbname=os.getenv('POSTGRES_DB', 'market_data'))


def _load_universe_symbols() -> List[str]:
    syms = []
    if not os.path.exists(_MASTER_UNIVERSE_PATH):
        return syms
    with open(_MASTER_UNIVERSE_PATH) as f:
        for row in csv.DictReader(f):
            if row.get("Symbol"):
                syms.append(row["Symbol"])
    return syms


def compute_market_context(target_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Computes, ONCE per run (not per symbol): universe-wide breadth (% of
    stocks above their own 20-SMA) and each stock's cross-sectional RS
    percentile rank (10-day return vs every other stock), both as of
    target_date. Always fresh from the DB - no file cache, so there is
    nothing that can go stale here.
    """
    universe = _load_universe_symbols()
    if not universe:
        logging.warning("ConvictionRouter: master_universe.csv not found; market context unavailable.")
        return {"breadth_pct": 50.0, "rs_by_symbol": {}}

    conn = _get_conn()
    try:
        query = "SELECT time::date as time, symbol, close FROM daily_ohlcv WHERE symbol = ANY(%(s)s)"
        params = {"s": universe}
        if target_date:
            query += " AND time::date <= %(d)s"
            params["d"] = target_date
        query += " ORDER BY symbol, time"
        df = pl.read_database(query, conn, execute_options={"parameters": params})
    finally:
        conn.close()

    if df.is_empty():
        return {"breadth_pct": 50.0, "rs_by_symbol": {}}

    df = df.with_columns([
        pl.col("close").rolling_mean(20).over("symbol").alias("sma_20"),
        (((pl.col("close") - pl.col("close").shift(10).over("symbol")) /
          pl.col("close").shift(10).over("symbol")) * 100).alias("roc_10"),
    ])

    latest = df.group_by("symbol").tail(1)
    with_sma = latest.filter(pl.col("sma_20").is_not_null())
    breadth_pct = 50.0
    if len(with_sma) > 0:
        breadth_pct = float((with_sma["close"] > with_sma["sma_20"]).mean() * 100)

    rs_by_symbol = {}
    with_roc = latest.filter(pl.col("roc_10").is_not_null())
    if len(with_roc) > 1:
        ranked = with_roc.with_columns(
            (pl.col("roc_10").rank(method="average") / pl.col("roc_10").count() * 100).alias("rs_pct")
        )
        rs_by_symbol = {r["symbol"]: r["rs_pct"] for r in ranked.select(["symbol", "rs_pct"]).to_dicts()}

    logging.info(f"ConvictionRouter market context ({target_date or 'latest'}): "
                 f"breadth={breadth_pct:.1f}% of {len(with_sma)} stocks above 20-SMA")
    return {"breadth_pct": breadth_pct, "rs_by_symbol": rs_by_symbol}


def compute_signal_features(symbol: str, target_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Single-stock signal-day character features from OHLCV."""
    try:
        conn = _get_conn()
        cur = conn.cursor()
        if target_date:
            cur.execute("""SELECT time::date, open, high, low, close, volume FROM daily_ohlcv
                           WHERE symbol=%s AND time::date <= %s ORDER BY time DESC LIMIT 22""",
                        (symbol, target_date))
        else:
            cur.execute("""SELECT time::date, open, high, low, close, volume FROM daily_ohlcv
                           WHERE symbol=%s ORDER BY time DESC LIMIT 22""", (symbol,))
        rows = cur.fetchall()
        cur.close(); conn.close()
    except Exception as e:
        logging.error(f"ConvictionRouter: feature fetch failed for {symbol}: {e}")
        return None

    if len(rows) < 22:
        return None
    rows = rows[::-1]  # chronological
    sig = rows[-1]
    prev = rows[-2]
    _, o, h, l, c, v = sig
    o, h, l, c, v = float(o), float(h), float(l), float(c), float(v)
    prev_c = float(prev[4])
    vol20 = sum(float(r[5]) for r in rows[-21:-1]) / 20.0
    rng = h - l
    return {
        "sig_day_ret": (c - prev_c) / prev_c * 100 if prev_c else 0.0,
        "vol_ratio": v / vol20 if vol20 else 1.0,
        "close_range": (c - l) / rng if rng > 0 else 1.0,
        "broke_2d_high": c >= max(float(r[4]) for r in rows[-3:-1]),
        "green": c >= o,
        "signal_close": c,
    }


def classify_route(f: Dict[str, Any], breadth_pct: float = 100.0, rs_pct: Optional[float] = None) -> str:
    """Returns 'OPTIONS_IGNITION', 'EQUITY_CONTINUATION', or 'NO_EDGE'.

    breadth_pct/rs_pct default to permissive values (100.0 / None->pass) so
    existing callers that don't pass market context (e.g. older lab scripts)
    keep working - but the validated, current thresholds require both to be
    supplied for a true apples-to-apples match of the backtested edge.
    """
    ignition = (f["broke_2d_high"]
                and f["vol_ratio"] >= OPT_VOL_MIN
                and f["close_range"] >= OPT_CLOSE_RANGE_MIN
                and breadth_pct >= OPT_BREADTH_MIN)
    calm_continuation = (EQ_RET_MIN <= f["sig_day_ret"] <= EQ_RET_MAX
                         and f["vol_ratio"] < EQ_VOL_MAX
                         and breadth_pct >= EQ_BREADTH_MIN
                         and (rs_pct is None or rs_pct >= EQ_RS_MIN))
    if ignition:
        return "OPTIONS_IGNITION"
    if calm_continuation:
        return "EQUITY_CONTINUATION"
    return "NO_EDGE"


def run_conviction_router(state: SovereignState) -> Dict[str, Any]:
    approved = state.get("approved_allocations", {})
    if not approved:
        return {}
    target_date = state.get("target_date")

    # Market context computed ONCE for this run, reused for every candidate.
    ctx = compute_market_context(target_date)
    breadth_pct = ctx["breadth_pct"]
    rs_by_symbol = ctx["rs_by_symbol"]

    routed = {}
    for symbol, alloc in approved.items():
        if alloc.get("route") == "PE_BEARISH_DIVERGENCE":
            # Pre-tagged by pring_divergence_agent.py - a genuinely different
            # (bearish) thesis validated standalone, never passed through the
            # bullish-tuned classify_route logic below. Preserve as-is.
            routed[symbol] = alloc
            logging.info(f"ConvictionRouter: {symbol} -> PE_BEARISH_DIVERGENCE (pre-tagged, not re-classified)")
            continue

        f = compute_signal_features(symbol, target_date)
        if f is None:
            logging.warning(f"ConvictionRouter: no features for {symbol}; defaulting to EQUITY.")
            routed[symbol] = {**alloc, "route": "EQUITY_CONTINUATION", "entry_condition": None}
            continue

        rs_pct = rs_by_symbol.get(symbol)
        route = classify_route(f, breadth_pct, rs_pct)
        if route == "NO_EDGE":
            # NOTE: approved_allocations uses a merge_dicts reducer in LangGraph
            # state, so omitting a symbol would NOT remove it - we must mark the
            # drop explicitly and downstream consumers must skip dropped entries.
            logging.info(f"ConvictionRouter: DROP {symbol} (no profile match: "
                         f"ret={f['sig_day_ret']:+.1f}%, vol={f['vol_ratio']:.1f}x, "
                         f"cr={f['close_range']:.2f}, 2dhi={f['broke_2d_high']}, "
                         f"breadth={breadth_pct:.0f}%, rs={rs_pct})")
            routed[symbol] = {**alloc, "route": "NO_EDGE", "dropped": True,
                              "suggested_instrument": "NONE", "entry_condition": None}
            continue

        entry = dict(alloc)
        entry["route"] = route
        entry["signal_features"] = {k: v for k, v in f.items() if k != "signal_close"}
        entry["market_context"] = {"breadth_pct": breadth_pct, "rs_pct": rs_pct}
        if route == "OPTIONS_IGNITION":
            # Time is the enemy of options (theta): enter SAME-DAY, at/near
            # the close of the signal day itself, not next-day open. Validated
            # paired test on identical setups: 78.9% WR (same-day close) vs
            # 73.7% WR (next-day open), n=19 both. This requires a live
            # intraday order during the 3PM pulse window (market is open
            # until 15:30 IST) instead of an AMO - see execution_agent.py.
            entry["suggested_instrument"] = "NFO_OPTION"
            entry["entry_condition"] = {"type": "SAME_DAY_CLOSE_ENTRY"}
        else:
            entry["suggested_instrument"] = "EQUITY"
            entry["entry_condition"] = None
        routed[symbol] = entry
        logging.info(f"ConvictionRouter: {symbol} -> {route} "
                     f"(ret={f['sig_day_ret']:+.1f}%, vol={f['vol_ratio']:.1f}x, cr={f['close_range']:.2f}, "
                     f"breadth={breadth_pct:.0f}%, rs={rs_pct})")

    return {"approved_allocations": routed}
