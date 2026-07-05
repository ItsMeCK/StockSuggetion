"""
Re-validates the equity-continuation edge against the REAL pipeline/screener.py
output (not a hand-reimplementation of its filter logic) for every trading day
in May+June. This is slower (calls the actual SovereignScreener class per day)
but eliminates the lab-vs-live mismatch found when the fast-backtest integration
gave 52-55% WR instead of the lab's 74.4% - that gap turned out to be caused
by testing against approximate filter logic instead of the real screener.

No LLM involved (pure screener + market-context math), so this still runs in
a few minutes, not hours.
"""
import os
import datetime
import logging
from dotenv import load_dotenv

load_dotenv()
os.environ["TRADING_MODE"] = "HISTORICAL"
import sys
sys.path.insert(0, "/Users/poonamsalke/Workplace/StockSuggetion")
os.chdir("/Users/poonamsalke/Workplace/StockSuggetion")
logging.basicConfig(level=logging.WARNING)

from pipeline.screener import SovereignScreener
from agents.conviction_router_agent import compute_market_context, compute_signal_features, classify_route
from scripts.backtest_3pm_options_vs_equity import get_recent_trading_dates, fetch_symbol_series, simulate_equity_trade, simulate_option_trade


def main():
    dates = get_recent_trading_dates(45, end_date="2026-06-30")  # covers May+June trading days
    dates = [d for d in dates if d >= "2026-05-01"]
    print(f"Days: {len(dates)} ({dates[0]} -> {dates[-1]})")

    screener = SovereignScreener()
    series_cache = {}
    all_rows = []

    for date in dates:
        try:
            candidates, incubator, flagged_momentum, base_scores, macro_regime = screener.run_pipeline(target_date=date)
        except Exception as e:
            print(f"  {date}: screener failed: {e}")
            continue
        stage2_only = set(candidates) - set(incubator)
        universe = stage2_only | set(flagged_momentum)
        if not universe:
            continue

        ctx = compute_market_context(date)
        breadth_pct = ctx["breadth_pct"]
        rs_by_symbol = ctx["rs_by_symbol"]

        for symbol in universe:
            f = compute_signal_features(symbol, date)
            if f is None:
                continue
            rs_pct = rs_by_symbol.get(symbol)
            route = classify_route(f, breadth_pct, rs_pct)
            if route == "NO_EDGE":
                continue

            if symbol not in series_cache:
                series_cache[symbol] = fetch_symbol_series(symbol)
            series = series_cache[symbol]
            dl = [b["date"] for b in series]
            if date not in dl:
                continue
            entry_idx = dl.index(date) + 1
            if entry_idx >= len(series):
                continue

            if route == "OPTIONS_IGNITION":
                r = simulate_option_trade(series, entry_idx)
                pnl = r.get("pnl_pct")
            else:
                r = simulate_equity_trade(series, entry_idx)
                pnl = r.get("pnl_pct")
            if pnl is not None:
                all_rows.append({"date": date, "symbol": symbol, "route": route, "pnl": pnl})

    print(f"\nTotal routed trades (real screener): {len(all_rows)}")
    for route in ("EQUITY_CONTINUATION", "OPTIONS_IGNITION"):
        rows = [r for r in all_rows if r["route"] == route]
        if not rows:
            print(f"{route}: n=0")
            continue
        n = len(rows)
        wr = 100 * sum(1 for r in rows if r["pnl"] > 0) / n
        avg = sum(r["pnl"] for r in rows) / n
        may = [r for r in rows if r["date"] < "2026-06-01"]
        june = [r for r in rows if r["date"] >= "2026-06-01"]
        def sub(rr):
            if not rr: return "n=0"
            return f"n={len(rr)} WR={100*sum(1 for r in rr if r['pnl']>0)/len(rr):.1f}%"
        print(f"{route}: n={n} WR={wr:.1f}% avg={avg:+.2f}%  | MAY {sub(may)}  JUNE {sub(june)}")


if __name__ == "__main__":
    main()
