"""
V5 Mesh Orchestrator — fresh build, no reuse of the old agents/ pipeline.

Pipeline per day:
  1. Cheap technical agents run on EVERY stock (vectorized features already
     computed). Build a shortlist = stocks with ANY technical agent score > 0
     (real production would do this same cheap-first-pass).
  2. News/earnings/sector agents run ONLY on that day's shortlist (bounded,
     realistic API usage - not 504 stocks x 40 days).
  3. Router combines all agent outputs with regime-conditional weights +
     weight-of-evidence bonus -> composite score per (symbol, date).
  4. Select the single best candidate per day (the 1-slot constraint).
  5. Simulate EQUITY entry (buy at day's close) with the validated ATR exit
     (1.5*ATR stop, trail after +5%, 5-day max hold - V4's proven fix).

⚠️ METHODOLOGY: this run is on May-June, the SAME window the archetype
thresholds were mined from (mover_pattern_classifier.py). This is an IN-SAMPLE
CONSISTENCY CHECK - it should reproduce close to the ~94% coverage already
found in mover_categorization_final.csv, proving the mesh correctly
operationalizes that categorization. It is NOT proof of forward-looking edge.
A real edge test requires FUTURE/unseen data.

Usage: TRADING_MODE=HISTORICAL PYTHONPATH=. venv/bin/python3 mesh_v1/backtest_mesh.py
"""
import os
import json
import csv
import statistics
from collections import defaultdict

os.environ["TRADING_MODE"] = "HISTORICAL"
from dotenv import load_dotenv
load_dotenv()

from mesh_v1.features import build_stock_frame, load_universe, load_company_map, frame_lookup
from mesh_v1.regime_agent import build_regime_map
from mesh_v1.technical_agents import (
    trend_continuation_agent, volume_thrust_agent, relative_strength_agent, oversold_reversal_agent,
)
from mesh_v1.news_agents import earnings_calendar_agent, news_catalyst_agent_v2
from mesh_v1.sector_agent import build_sector_move_map, sector_rotation_agent
from mesh_v1.router import combine

WIN_START, WIN_END = "2026-05-01", "2026-06-30"
STOP_ATR, TRAIL_TRIGGER, TRAIL_ATR, MAX_HOLD, CAPITAL = 1.5, 0.05, 2.0, 5, 5000.0

TECH_AGENTS = {
    "trend_continuation": trend_continuation_agent,
    "volume_thrust": volume_thrust_agent,
    "relative_strength": relative_strength_agent,
    "oversold_reversal": oversold_reversal_agent,
}


def simulate_equity(series, i, atr):
    entry = series[i]["close"]
    stop = entry - STOP_ATR * atr
    peak = entry
    for d in range(1, MAX_HOLD + 1):
        if i + d >= len(series):
            return None
        bar = series[i + d]
        if peak >= entry * (1 + TRAIL_TRIGGER):
            stop = max(stop, peak - TRAIL_ATR * atr)
        if bar["low"] <= stop:
            return {"pnl": (stop - entry) / entry * 100, "status": "STOP/TRAIL", "days": d}
        peak = max(peak, bar["high"])
    exit_c = series[i + MAX_HOLD]["close"]
    return {"pnl": (exit_c - entry) / entry * 100, "status": f"{MAX_HOLD}DAY_CLOSE", "days": MAX_HOLD}


SHORTLIST_MIN_SCORE = 1   # gate = "any technical agent fired at all" (matches the ORIGINAL
SHORTLIST_MIN_AGENTS = 99  # archetype-fires definition used to build mover_categorization_final.csv,
                           # so the consistency check is apples-to-apples). Weight-of-evidence
                           # scoring/bonus still happens in the ROUTER's ranking, not the gate.
                           # Safe now that NEWS_LIVE_FETCH=0 makes Pass 2 network-free/fast.


def main():
    import sys
    universe = load_universe()
    company_map = load_company_map()
    print(f"Universe: {len(universe)}. Building feature frame {WIN_START}..{WIN_END}...", flush=True)
    df = build_stock_frame(universe, start="2026-03-01", end=WIN_END)  # extra lookback for indicators
    by_sym, lut = frame_lookup(df)
    print(f"Frame built: {len(by_sym)} symbols. Building regime map...", flush=True)
    regime_map = build_regime_map(df)

    # --- Pass 1: cheap technical agents on every stock-day -> shortlist ---
    print("Pass 1: technical agents on full universe...", flush=True)
    daily_candidates = defaultdict(list)  # date -> [(symbol, tech_scores_dict)]
    day_returns = defaultdict(list)       # date -> [(symbol, day_return_pct)] for sector agent
    for sym, series in by_sym.items():
        for j, r in enumerate(series):
            d = r["time"].strftime("%Y-%m-%d")
            if not (WIN_START <= d <= WIN_END) or r.get("atr14") is None:
                continue
            prev_c = r.get("prev_close")
            if prev_c:
                day_returns[d].append((sym, (r["close"] - prev_c) / prev_c * 100))
            tech = {name: fn(r) for name, fn in TECH_AGENTS.items()}
            n_fired = sum(1 for s, _ in tech.values() if s > 0)
            max_score = max((s for s, _ in tech.values()), default=0)
            if max_score >= SHORTLIST_MIN_SCORE or n_fired >= SHORTLIST_MIN_AGENTS:
                daily_candidates[d].append((sym, j, tech))

    n_shortlist = sum(len(v) for v in daily_candidates.values())
    print(f"Shortlist size (score>={SHORTLIST_MIN_SCORE} or >={SHORTLIST_MIN_AGENTS} agents fired): "
          f"{n_shortlist} stock-days across {len(daily_candidates)} days", flush=True)

    sector_move_map = build_sector_move_map(day_returns, company_map)

    # --- Pass 2: news/sector agents on shortlist only, then route ---
    print(f"Pass 2: news/sector agents on {n_shortlist} shortlisted stock-days + routing...", flush=True)
    all_scores_by_day = defaultdict(list)
    detail = []
    processed = 0
    for d, rows in daily_candidates.items():
        regime = regime_map.get(d, {}).get("regime", "UNKNOWN")
        for sym, j, tech in rows:
            earn = earnings_calendar_agent(sym, d)
            news = news_catalyst_agent_v2(sym, d)
            sect = sector_rotation_agent(sym, d, sector_move_map)
            agent_results = {**tech, "earnings_calendar": earn, "news_catalyst_v2": news, "sector_rotation": sect}
            composite, n_fired, reasons = combine(agent_results, regime)
            all_scores_by_day[d].append((sym, composite, n_fired, reasons, j))
            detail.append({"date": d, "symbol": sym, "composite": composite, "n_fired": n_fired,
                           "regime": regime, "reasons": reasons})
            processed += 1
            if processed % 100 == 0:
                print(f"  Pass 2 progress: {processed}/{n_shortlist}", flush=True)

    # --- select top-1/day, simulate equity ---
    print("Selecting top-1/day and simulating equity exits...")
    trades = []
    for d in sorted(all_scores_by_day):
        ranked = sorted(all_scores_by_day[d], key=lambda x: -x[1])
        sym, composite, n_fired, reasons, j = ranked[0]
        series = by_sym[sym]
        atr = series[j].get("atr14")
        res = simulate_equity(series, j, atr) if atr else None
        trades.append({"date": d, "symbol": sym, "composite": composite, "n_fired": n_fired,
                      "regime": regime_map.get(d, {}).get("regime"), "reasons": reasons, "res": res})

    done = [t for t in trades if t["res"]]
    print(f"\nTotal trading days: {len(trades)}   with forward data: {len(done)}")

    p = [t["res"]["pnl"] for t in done]
    w = [x for x in p if x > 0]
    print(f"\n=== V5 MESH SINGLE-SLOT EQUITY RESULT (in-sample, May-Jun) ===")
    print(f"n={len(p)}  WR={100*len(w)/len(p):.1f}%  avg={statistics.mean(p):+.2f}%  "
          f"total=Rs{sum(CAPITAL*x/100 for x in p):+,.0f}")
    for reg in ("TRENDING_UP", "TRENDING_DOWN", "CHOPPY", "UNKNOWN"):
        sub = [t["res"]["pnl"] for t in done if t["regime"] == reg]
        if sub:
            sw = [x for x in sub if x > 0]
            print(f"  {reg:14s} n={len(sub):3d} WR={100*len(sw)/len(sub):.1f}% avg={statistics.mean(sub):+.2f}%")

    # --- consistency check vs the master categorization table ---
    print("\n=== CONSISTENCY CHECK vs mover_categorization_final.csv (IN-SAMPLE, expected ~94%) ===")
    cat_rows = list(csv.DictReader(open("mover_categorization_final.csv")))
    mover_keys = set((r["symbol"], r["start"]) for r in cat_rows)
    shortlisted_keys = set((sym, d) for d, rows in daily_candidates.items() for sym, j, t in rows)
    hit = len(mover_keys & shortlisted_keys)
    print(f"5% movers whose start-date the MESH shortlist would have flagged: {hit} / {len(mover_keys)} "
          f"({100*hit/len(mover_keys):.1f}%)")
    print("(This is a wiring/consistency check, NOT forward-looking validation - see module docstring.)")

    json.dump({"trades": trades, "detail_sample": detail[:200]}, open("mesh_v1_results.json", "w"), indent=2, default=str)
    print("\nSaved -> mesh_v1_results.json")


if __name__ == "__main__":
    main()
