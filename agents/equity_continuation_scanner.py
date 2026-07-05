"""
Equity Continuation Scanner - taps Gate-1 (deterministic screener) candidates
DIRECTLY, independent of the critic/entry-trigger/fundamental-audit gauntlet.

Why this agent exists: the "calm continuation" edge (74.4% WR combined,
81.2%/69.6% across two independent regimes - see conviction_router_agent.py
docstring) was discovered against the FULL raw Gate-1 candidate pool. The
existing critic (>=65-80 conviction score) + entry-trigger (>=80 momentum
score) + fundamental-audit gauntlet is tuned to find HIGH-CONVICTION IGNITION
setups - by construction it filters out exactly the mild, low-volume,
"nothing dramatic happening" days that the calm-continuation edge lives in.
Routing this signal through that gauntlet starves it down to near-zero
candidates (validated: n=3 live vs n=39 on the raw pool).

This agent runs in PARALLEL to the existing critic->risk chain, straight off
state["candidates"], and injects its own fixed-size allocations directly into
approved_allocations (LangGraph's merge_dicts reducer combines them additively
with whatever the existing risk_agent chain also approves - no collision
handling needed unless the exact same symbol matches both profiles, which
the router's classify_route already resolves deterministically).

No caching: market context (breadth, RS rank) and signal features are always
computed fresh from daily_ohlcv - there is nothing here to go stale.
"""
import logging
from typing import Dict, Any

from core.state import SovereignState
from agents.conviction_router_agent import (
    compute_market_context, compute_signal_features, classify_route, EQ_BREADTH_MIN,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

FIXED_ALLOCATION = 5000.0   # matches SovereignConvictionGate.standard_allocation
EQUITY_TARGET_PCT = 0.10
EQUITY_STOP_PCT = 0.05


def run_equity_continuation_scanner(state: SovereignState) -> Dict[str, Any]:
    # NOTE: state["candidates"] from the screener is actually stage_2 UNION
    # transition/incubator symbols (see pipeline/screener.py's combined_pool),
    # not pure stage-2. The validated 74.4% WR edge was measured against
    # stage_2 + flagged_momentum ONLY - transition/incubator names dilute it
    # (confirmed empirically: including them drops live WR from ~74% to 55%).
    # Subtract incubator to recover the validated population.
    candidates = state.get("candidates", [])
    incubator = state.get("incubator", [])
    flagged_momentum = state.get("flagged_momentum_candidates", [])
    stage2_only = set(candidates) - set(incubator)
    universe = list(stage2_only | set(flagged_momentum))
    if not universe:
        return {}

    target_date = state.get("target_date")
    ctx = compute_market_context(target_date)
    breadth_pct = ctx["breadth_pct"]
    rs_by_symbol = ctx["rs_by_symbol"]

    if breadth_pct < EQ_BREADTH_MIN:
        logging.info(f"EquityContinuationScanner: breadth {breadth_pct:.1f}% < {EQ_BREADTH_MIN}% gate - skipping scan.")
        return {}

    new_allocations = {}
    for symbol in universe:
        f = compute_signal_features(symbol, target_date)
        if f is None:
            continue
        rs_pct = rs_by_symbol.get(symbol)
        route = classify_route(f, breadth_pct, rs_pct)
        if route != "EQUITY_CONTINUATION":
            continue

        entry_price = f["signal_close"]
        stop_loss = entry_price * (1 - EQUITY_STOP_PCT)
        target = entry_price * (1 + EQUITY_TARGET_PCT)
        shares = int(FIXED_ALLOCATION / entry_price) if entry_price > 0 else 0
        if shares <= 0:
            continue

        new_allocations[symbol] = {
            "approved": True,
            "shares": shares,
            "capital_allocated": FIXED_ALLOCATION,
            "entry": entry_price,
            "stop_loss": stop_loss,
            "target": target,
            "route": "EQUITY_CONTINUATION",
            "suggested_instrument": "EQUITY",
            "entry_condition": None,
            "source": "equity_continuation_scanner",
            "signal_features": {k: v for k, v in f.items() if k != "signal_close"},
            "market_context": {"breadth_pct": breadth_pct, "rs_pct": rs_pct},
        }
        logging.info(f"EquityContinuationScanner: {symbol} -> EQUITY_CONTINUATION "
                     f"(ret={f['sig_day_ret']:+.1f}%, vol={f['vol_ratio']:.1f}x, "
                     f"breadth={breadth_pct:.0f}%, rs={rs_pct})")

    if new_allocations:
        logging.info(f"EquityContinuationScanner: {len(new_allocations)} calm-continuation signal(s) found.")
    return {"approved_allocations": new_allocations} if new_allocations else {}
