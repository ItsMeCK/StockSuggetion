"""
Breadth Thrust Detector - validated regime-level watchlist widener.

Validated (scripts/regime_signal_lab.py): n=181 real trades, win rate 50.3%,
profit factor 1.31, expectancy +7.1%/trade. Matches core/context_rules_3.json's
documented "breadth_thrust" pattern (market breadth crossing from <40% to
>60% within ~10 days = institutional rotation) - this agent operationalizes
a rule that was written down in the rulebook months ago and never used.

CRITICAL DESIGN CONSTRAINT: this agent ONLY widens the candidate pool. It
never triggers a buy by itself. The actual entry still requires the stock's
own same-day ignition confirmation (existing conviction_router_agent logic) -
"be there exactly when momentum starts, not before, not after" applies
per-stock regardless of the regime state. This agent answers "is it worth
casting a wider net today", not "buy this now".
"""
import os
import csv
import logging
from typing import Dict, Any, List

import psycopg2
import polars as pl

from core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

THRUST_LOW_THRESHOLD = 45.0   # breadth must have been this low recently
THRUST_3D_CHANGE_MIN = 15.0   # and risen by at least this many points in 3 days
LOOKBACK_DAYS = 30            # enough history to evaluate the recent-low condition


def _get_conn():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'), port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'quant'),
        password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
        dbname=os.getenv('POSTGRES_DB', 'market_data'))


def _load_universe_symbols() -> List[str]:
    syms = []
    path = "pipeline/master_universe.csv"
    if not os.path.exists(path):
        return syms
    with open(path) as f:
        for row in csv.DictReader(f):
            if row.get("Symbol"):
                syms.append(row["Symbol"])
    return syms


def is_thrust_active(target_date: str = None) -> Dict[str, Any]:
    """Computes breadth (% of universe above 20-SMA) for the trailing window
    and checks the thrust condition as of target_date (or latest available)."""
    universe = _load_universe_symbols()
    if not universe:
        return {"active": False, "reason": "no universe file"}

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
        return {"active": False, "reason": "no data"}

    df = df.with_columns(pl.col("close").rolling_mean(20).over("symbol").alias("sma_20"))
    b = (df.filter(pl.col("sma_20").is_not_null())
           .with_columns((pl.col("close") > pl.col("sma_20")).alias("above20"))
           .group_by("time").agg((pl.col("above20").mean() * 100).alias("breadth")))
    rows = sorted(b.to_dicts(), key=lambda r: r["time"])
    if len(rows) < 6:
        return {"active": False, "reason": "insufficient breadth history"}

    vals = [r["breadth"] for r in rows[-LOOKBACK_DAYS:]]
    recent_min = min(vals[-6:-1]) if len(vals) >= 6 else min(vals)
    change_3d = vals[-1] - vals[-4] if len(vals) >= 4 else 0
    current = vals[-1]

    active = recent_min < THRUST_LOW_THRESHOLD and change_3d >= THRUST_3D_CHANGE_MIN
    return {"active": active, "current_breadth": current, "recent_min": recent_min, "change_3d": change_3d}


def run_breadth_thrust_agent(state: SovereignState) -> Dict[str, Any]:
    """
    LangGraph node: widens the candidate pool when a breadth thrust is active,
    using the "broader continuation" profile (validated, profit-factor 1.31,
    n=181) instead of the narrow normal/titan/stealth momentum bands.

    Writes directly to state["candidates"] (add_lists reducer -> unions with
    whatever the screener/momentum_adaptation already produced) rather than
    flagged_momentum_candidates, deliberately bypassing momentum_adaptation's
    OWN (different, stricter) promotion thresholds - this agent's widening
    rule was validated on its own terms and shouldn't be re-filtered by a
    different rule it was never tested against.

    Does NOT touch approved_allocations or trigger any buy directly - these
    still go through the full existing gauntlet (news catalyst, entry
    trigger, critic, fundamental audit) exactly like any other candidate,
    and the actual option entry still requires that stock's own same-day
    ignition confirmation downstream.
    """
    target_date = state.get("target_date")
    thrust = is_thrust_active(target_date)
    if not thrust.get("active"):
        return {}

    logging.info(f"BREADTH THRUST ACTIVE (breadth {thrust['current_breadth']:.1f}%, "
                 f"recent low {thrust['recent_min']:.1f}%, 3d change {thrust['change_3d']:+.1f}pts) "
                 f"- widening watchlist to broader-continuation profile.")

    universe = _load_universe_symbols()
    if not universe:
        return {}

    conn = _get_conn()
    try:
        query = "SELECT time::date as time, symbol, close, volume FROM daily_ohlcv WHERE symbol = ANY(%(s)s)"
        params = {"s": universe}
        if target_date:
            query += " AND time::date <= %(d)s"
            params["d"] = target_date
        query += " ORDER BY symbol, time"
        df = pl.read_database(query, conn, execute_options={"parameters": params})
    finally:
        conn.close()

    df = df.with_columns([
        pl.col("close").rolling_mean(10).over("symbol").alias("sma_10"),
        pl.col("close").rolling_mean(20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_mean(50).over("symbol").alias("sma_50"),
        pl.col("volume").rolling_mean(20).over("symbol").alias("vol_avg_20"),
        (((pl.col("close") - pl.col("close").shift(10).over("symbol")) /
          pl.col("close").shift(10).over("symbol")) * 100).alias("roc_10"),
    ])
    latest = df.group_by("symbol").tail(1).filter(
        (pl.col("close") > pl.col("sma_50")) &
        (pl.col("sma_10") > pl.col("sma_20")) &
        (pl.col("roc_10") > 0) &
        (pl.col("volume") >= 1.0 * pl.col("vol_avg_20"))
    )
    widened = latest["symbol"].to_list()
    logging.info(f"BreadthThrustAgent: {len(widened)} additional candidates from widened watchlist.")

    return {"candidates": widened}
