"""
FAST backtest harness for rapid strategy iteration.

Runs the REAL pipeline code (screener -> LangGraph agent chain -> trade sim),
but eliminates the three slow externalities:

  1. Market data loads ONCE per process (was: full-table DB load per day).
  2. News-catalyst agent replays from news_catalyst_cache.json (no RSS, no LLM).
     Missing cache key => treated as "no catalyst" (documented bias: newly
     surfaced candidates get no catalyst boost unless --llm-fallback is set).
  3. Fundamental-audit agent replays from audit_cache.json (no RSS, no LLM).
     Missing key => neutral grade B / ACCUMULATE (no veto, no boost).

CACHE CORRECTNESS RULES (read this before every iteration):
  - vision_cache.json is BYPASSED entirely here: the deterministic vision
    scorer recomputes fresh every run (it's ~ms), so changing vision logic can
    never leak stale scores into results.
  - news_catalyst_cache.json / audit_cache.json hold EXTERNAL FACTS (news
    headlines' LLM interpretation for a symbol+date). Strategy-logic changes
    do NOT invalidate them. Only delete them if the prompt/model changed.
  - If you change SCREENER logic, new candidates may appear that have no news/
    audit cache entries -> they run with neutral news/audit. Use --llm-fallback
    for a final validation pass with real LLM calls on missing keys only.

Usage:
  PYTHONPATH=. venv/bin/python3 scripts/fast_backtest.py --days 20
  PYTHONPATH=. venv/bin/python3 scripts/fast_backtest.py --days 20 --llm-fallback
"""
import os
import sys
import json
import time
import logging
import argparse

from dotenv import load_dotenv
load_dotenv()
os.environ["TRADING_MODE"] = "HISTORICAL"

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger("fast_backtest")
log.setLevel(logging.INFO)

import polars as pl

# ---------------------------------------------------------------------------
# Patch 1: single market-data load for the whole process
# ---------------------------------------------------------------------------
from pipeline.screener import SovereignScreener

_MARKET_DF = None
_orig_fetch = SovereignScreener.fetch_market_data

def _cached_fetch(self):
    global _MARKET_DF
    if _MARKET_DF is None:
        log.info("Loading market data ONCE for the whole run...")
        t0 = time.time()
        _MARKET_DF = _orig_fetch(self)
        log.info(f"Loaded {len(_MARKET_DF)} rows in {time.time()-t0:.1f}s")
    return _MARKET_DF

SovereignScreener.fetch_market_data = _cached_fetch

# ---------------------------------------------------------------------------
# Patch 2: news catalyst agent -> cache replay (no RSS / no LLM by default)
# ---------------------------------------------------------------------------
import agents.news_catalyst_agent as nca_mod

LLM_FALLBACK = False
_orig_news_eval = nca_mod.NewsCatalystAgent.evaluate_news_catalysts

NO_CATALYST = {
    "catalyst_detected": False, "catalyst_type": "NONE",
    "summary": "FAST-BACKTEST: no cache entry (treated as no catalyst).",
    "score_boost": 0.0,
}

def _replay_news(self, symbol, target_date=None, bypass_prefilter=False):
    from datetime import datetime, timezone
    date_str = target_date if target_date else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"{symbol}_{date_str}"
    if key in self.cache:
        return self.cache[key]
    if LLM_FALLBACK:
        return _orig_news_eval(self, symbol, target_date, bypass_prefilter)
    return dict(NO_CATALYST)

nca_mod.NewsCatalystAgent.evaluate_news_catalysts = _replay_news

# ---------------------------------------------------------------------------
# Patch 3: fundamental audit -> cache replay; RSS fetches skipped
# ---------------------------------------------------------------------------
import agents.fundamental_audit as fa_mod

_orig_audit = fa_mod.FundamentalAuditAgent.analyze_sovereign_setup

NEUTRAL_AUDIT = {
    "narrative": "FAST-BACKTEST: no cache entry (neutral).",
    "red_flags": [], "grade": "B", "sentiment": "NEUTRAL", "action": "ACCUMULATE",
}

def _replay_audit(self, symbol, news_data, fraud_data, tech_metrics, target_date=None):
    import datetime
    date_str = target_date if target_date else datetime.datetime.now().strftime("%Y-%m-%d")
    key = f"{symbol}_{date_str}"
    if key in self.cache:
        return self.cache[key]
    if LLM_FALLBACK:
        return _orig_audit(self, symbol, news_data, fraud_data, tech_metrics, target_date)
    return dict(NEUTRAL_AUDIT)

fa_mod.FundamentalAuditAgent.analyze_sovereign_setup = _replay_audit
fa_mod.fetch_rss_headlines = lambda query: ""  # skip network entirely

# ---------------------------------------------------------------------------
# Patch 4: vision cache bypass -> deterministic scorer always recomputes fresh
# (guarantees stale vision scores can never leak in after logic changes)
# ---------------------------------------------------------------------------
import agents.pattern_agent as pa_mod

pa_mod.VisionPatternAgent._load_cache = lambda self: {}
pa_mod.VisionPatternAgent._save_cache = lambda self: None

# ---------------------------------------------------------------------------
# Patch 5: derivatives routing is a no-op in backtests. It exists to pick the
# actual NFO contract via live Kite API - historical NFO availability isn't
# queryable, and without creds it would DROP every OPTIONS_IGNITION trade.
# The option leg is simulated via Black-Scholes proxy downstream instead.
# ---------------------------------------------------------------------------
import graph.builder as gb_mod

gb_mod.derivatives_routing_agent = lambda state: {}

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
from run_historical import run_historical_engine
from scripts.backtest_3pm_options_vs_equity import (
    get_recent_trading_dates, fetch_symbol_series,
    simulate_equity_trade, simulate_option_trade, summarize,
)


def _route_wr(trades, route, leg):
    rows = [t[leg] for t in trades if t.get("route") == route and t[leg] and t[leg].get("pnl_pct") is not None]
    if not rows:
        return None
    pnls = [r["pnl_pct"] for r in rows]
    wins = sum(1 for p in pnls if p > 0)
    return {"n": len(pnls), "wr": round(100 * wins / len(pnls), 1), "avg": round(sum(pnls) / len(pnls), 2)}


def run_fast_backtest(num_days: int, out_path: str, end_date: str = None):
    dates = get_recent_trading_dates(num_days, end_date)
    log.info(f"FAST backtest: {len(dates)} trading days {dates[0]} -> {dates[-1]} "
             f"(llm_fallback={LLM_FALLBACK})")

    all_trades = []
    approved_by_date = {}
    skipped_by_condition = []
    symbol_cache = {}
    t_start = time.time()

    for date in dates:
        t0 = time.time()
        try:
            run_data = run_historical_engine(date)
        except Exception as e:
            log.error(f"{date}: engine failed: {e}")
            continue
        approved = (run_data or {}).get("approved", [])
        allocations = (run_data or {}).get("approved_allocations", {})
        approved_by_date[date] = approved
        log.info(f"{date}: {len(approved)} approved {approved} ({time.time()-t0:.1f}s)")

        for symbol in approved:
            if symbol not in symbol_cache:
                symbol_cache[symbol] = fetch_symbol_series(symbol)
            series = symbol_cache[symbol]
            dl = [b["date"] for b in series]
            if date not in dl:
                continue
            signal_idx = dl.index(date)
            entry_idx = signal_idx + 1
            if entry_idx >= len(series):
                continue

            alloc = allocations.get(symbol, {})
            route = alloc.get("route")  # None when router disabled/not tagged

            rec = {"signal_date": date, "symbol": symbol, "route": route,
                   "entry_date": series[entry_idx]["date"],
                   "equity": None, "options": None}
            if route == "OPTIONS_IGNITION":
                # Same-day close entry (validated: 78.9% vs 73.7% WR on
                # identical setups) - see agents/conviction_router_agent.py
                rec["options"] = simulate_option_trade(series, signal_idx, entry_price_field="close")
            elif route == "EQUITY_CONTINUATION":
                rec["equity"] = simulate_equity_trade(series, entry_idx)
            else:  # untagged fallback: simulate both (baseline behavior)
                rec["equity"] = simulate_equity_trade(series, entry_idx)
                rec["options"] = simulate_option_trade(series, signal_idx, entry_price_field="close")
            all_trades.append(rec)

    elapsed = time.time() - t_start
    log.info(f"FAST backtest complete in {elapsed:.0f}s ({elapsed/60:.1f} min)")

    with open(out_path, "w") as f:
        json.dump({"approved_by_date": approved_by_date, "trades": all_trades,
                   "skipped_by_condition": skipped_by_condition}, f, indent=2, default=str)
    log.info(f"Saved {len(all_trades)} trades -> {out_path} "
             f"({len(skipped_by_condition)} skipped by open-confirmation)")

    # Per-route summaries (the numbers that matter post-router)
    print("\n=== PER-ROUTE RESULTS ===")
    for route, leg in (("EQUITY_CONTINUATION", "equity"), ("OPTIONS_IGNITION", "options")):
        s = _route_wr(all_trades, route, leg)
        if s:
            print(f"{route:24s} n={s['n']:3d}  WR={s['wr']}%  avg={s['avg']:+.2f}%")
        else:
            print(f"{route:24s} (no closed trades)")
    if any(t.get("route") is None for t in all_trades):
        print("\n(untagged fallback trades below - router was not active)")
        summarize([t for t in all_trades if t.get("route") is None and t["equity"] and t["options"]])
    return all_trades


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=20)
    ap.add_argument("--end-date", default=None,
                    help="Last trading date of the window (YYYY-MM-DD); default = latest in DB")
    ap.add_argument("--llm-fallback", action="store_true",
                    help="Call real LLM for news/audit cache misses (final validation runs)")
    ap.add_argument("--out", default="backtest_fast_results.json")
    args = ap.parse_args()
    LLM_FALLBACK = args.llm_fallback
    run_fast_backtest(args.days, args.out, args.end_date)
