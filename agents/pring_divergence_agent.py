"""
Pring bearish momentum-divergence agent (PE) - the one signal, out of six
tested, that survived rigorous out-of-sample validation (scripts/
pring_divergence_lab.py, run against real 15-min/daily data with the 919
corrupted duplicate rows removed from daily_ohlcv).

Pring's central thesis (Technical Analysis Explained): momentum LEADS price
at turning points. Price can make a fresh N-day high while a 14-day ROC
oscillator makes a LOWER high than its prior swing peak - a bearish
divergence warning of distribution, before the breakdown is visible in
price/moving-average terms. Confirmed here by volume >= 1.5x average on the
divergence day (a churn/distribution signature, not accumulation).

Gated to fire ONLY when macro_regime == "BEARISH" - the only regime bucket
that held up across BOTH independent calendar clusters tested (May 10-31:
n=35, PF=2.34, +17.7%; June 1-10: n=14, PF=1.21, +4.6%). NEUTRAL regime
looked promising in May but June's neutral days were too few (n=6) to trust
yet - not gated in here. BULLISH regime is a clear, well-sampled loser
(n=43-104, PF 0.57-0.85) and is excluded. Combined BEARISH-only: n=49,
WR=53.1%, PF=1.88, expectancy=+13.9%.

CRITICAL DESIGN CONSTRAINT: unlike breadth_thrust_agent (which widens the
SAME bullish continuation profile and safely reuses the full gauntlet), this
signal is a genuinely different (bearish) thesis that was validated
STANDALONE. The bullish-tuned gauntlet (news_catalyst_agent, critic_agent,
pattern_agent, fundamental_audit) was built and tuned to score bullish
continuation setups and has never been tested on a bearish thesis - routing
these candidates through it risks either wrongly vetoing good setups or
worse, misclassifying one as a bullish buy. This agent therefore bypasses
that gauntlet entirely and writes DIRECTLY to approved_allocations with a
pre-set "route" tag that conviction_router_agent.py and
derivatives_routing_agent.py must respect (skip re-classification, resolve
PE not CE).

Entry timing: SAME-DAY, at/near the close of the signal day (matches the
validated CE convention - same-day-close beats next-day-open because theta
is the enemy of options).
"""
import os
import csv
import logging
from typing import Dict, Any

import psycopg2
import polars as pl

from core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

LOCAL_HIGH_WINDOW = 20
DIVERGENCE_LOOKBACK_MIN = 5
DIVERGENCE_LOOKBACK_MAX = 60
ROC_PERIOD = 14
VOL_CONFIRM = 1.5

FIXED_ALLOCATION = 5000.0  # matches risk_agent.py's existing standard/titan sizing convention


def _get_conn():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'), port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'quant'),
        password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
        dbname=os.getenv('POSTGRES_DB', 'market_data'))


def _load_universe_symbols():
    syms = []
    path = "pipeline/master_universe.csv"
    if not os.path.exists(path):
        return syms
    with open(path) as f:
        for row in csv.DictReader(f):
            if row.get("Symbol"):
                syms.append(row["Symbol"])
    return syms


def _detect_divergence_at_latest(rows) -> bool:
    """Sequential, no-lookahead scan: checks whether the LAST row (always
    target_date - the row we're deciding on) is itself a divergence event."""
    last_high_val, last_high_roc, last_high_idx = None, None, None
    for i, r in enumerate(rows):
        if r["roc_14"] is None or r["rolling_high_20"] is None:
            continue
        is_fresh_high = r["close"] >= r["rolling_high_20"] * 0.999
        if not is_fresh_high:
            continue
        is_divergence = (last_high_idx is not None
                         and DIVERGENCE_LOOKBACK_MIN <= i - last_high_idx <= DIVERGENCE_LOOKBACK_MAX
                         and r["close"] > last_high_val and r["roc_14"] < last_high_roc)
        if i == len(rows) - 1:
            return is_divergence
        last_high_val, last_high_roc, last_high_idx = r["close"], r["roc_14"], i
    return False


def find_bearish_divergence_candidates(target_date: str = None):
    """Returns {symbol: {"entry": close, "roc_14": ..., "vol_ratio": ...}} for
    every universe stock showing a confirmed bearish divergence as of
    target_date (or latest available data if None)."""
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

    if df.is_empty():
        return {}

    df = df.with_columns([
        pl.col("volume").rolling_mean(20).over("symbol").alias("vol_avg_20"),
        (((pl.col("close") - pl.col("close").shift(ROC_PERIOD).over("symbol")) /
          pl.col("close").shift(ROC_PERIOD).over("symbol")) * 100).alias("roc_14"),
        pl.col("close").rolling_max(LOCAL_HIGH_WINDOW).over("symbol").alias("rolling_high_20"),
    ])

    found = {}
    for sym_df in df.partition_by("symbol"):
        sym = sym_df["symbol"][0]
        rows = sym_df.sort("time").to_dicts()
        if len(rows) < LOCAL_HIGH_WINDOW + ROC_PERIOD:
            continue
        last = rows[-1]
        va = last["vol_avg_20"] or last["volume"]
        if last["volume"] < VOL_CONFIRM * va:
            continue
        if _detect_divergence_at_latest(rows):
            found[sym] = {
                "entry": float(last["close"]),
                "roc_14": round(float(last["roc_14"]), 2),
                "vol_ratio": round(float(last["volume"] / va), 2) if va else None,
                "signal_date": last["time"].strftime("%Y-%m-%d"),
            }
    return found


def run_pring_divergence_agent(state: SovereignState) -> Dict[str, Any]:
    macro_regime = state.get("macro_regime")
    if macro_regime != "BEARISH":
        return {}

    target_date = state.get("target_date")
    found = find_bearish_divergence_candidates(target_date)
    if not found:
        return {}

    logging.info(f"PringDivergenceAgent: BEARISH regime active, {len(found)} bearish-divergence "
                 f"PE candidates found: {list(found.keys())}")

    approved_allocations = {}
    for symbol, sig in found.items():
        approved_allocations[symbol] = {
            "approved": True,
            "route": "PE_BEARISH_DIVERGENCE",
            "entry": sig["entry"],
            "capital_allocated": FIXED_ALLOCATION,
            "conviction_score": None,
            "entry_condition": {"type": "SAME_DAY_CLOSE_ENTRY"},
            "signal_features": {"roc_14": sig["roc_14"], "vol_ratio": sig["vol_ratio"]},
            "signal_date": sig["signal_date"],
        }

    return {"bearish_divergence_candidates": list(found.keys()), "approved_allocations": approved_allocations}
