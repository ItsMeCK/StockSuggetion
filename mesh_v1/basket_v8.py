"""
V8: adds the regime-FRESHNESS gate on top of V7's quality gate (near-high
penalty, climax/exhaustion hard reject, earnings/news reweight). Directly
targets the confirmed 2026-06-22..06-30 loss cluster (9 trades, 22.2% WR,
-Rs5,799) which sat at day 4-9 of an aging TRENDING_UP regime - the mesh
previously treated day 1 and day 9 of a trend identically.

Rule (mesh_v1/regime_freshness.py): for TRENDING_UP/TRENDING_DOWN days,
full weight days 1-3 of the regime, 0.55x discount days 4-6, HARD CUT
(no trades at all) day 7+. CHOPPY untouched (already regime-penalized).
"""
import os
import csv
import statistics
from collections import defaultdict

os.environ["TRADING_MODE"] = "HISTORICAL"
os.environ.setdefault("NEWS_LIVE_FETCH", "0")
from dotenv import load_dotenv
load_dotenv()

from mesh_v1.features import build_stock_frame, load_universe, load_company_map, frame_lookup
from mesh_v1.regime_agent import build_regime_map
from mesh_v1.technical_agents import (
    trend_continuation_agent, relative_strength_agent, oversold_reversal_agent,
)
from mesh_v1.quality_gate_v7 import volume_thrust_agent_v7, hard_reject_v7
from mesh_v1.news_agents import earnings_calendar_agent, news_catalyst_agent_v2
from mesh_v1.sector_agent import build_sector_move_map, sector_rotation_agent
from mesh_v1.basket_v7 import combine_v7
from mesh_v1.regime_freshness import build_freshness_map, freshness_multiplier

WIN_START, WIN_END = "2026-05-01", "2026-06-30"
STOP_ATR, TRAIL_TRIGGER, TRAIL_ATR, MAX_HOLD = 1.5, 0.05, 2.0, 5
TOTAL_CAPITAL = 100000.0
N_SIZES = [3, 5, 10]

TECH_AGENTS = {
    "trend_continuation": trend_continuation_agent,
    "volume_thrust": volume_thrust_agent_v7,
    "relative_strength": relative_strength_agent,
    "oversold_reversal": oversold_reversal_agent,
}
SHORTLIST_MIN_SCORE, SHORTLIST_MIN_AGENTS = 40, 2


def simulate_equity(series, i, atr, per_trade_capital):
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
            pnl_pct = (stop - entry) / entry * 100
            return {"pnl_pct": pnl_pct, "pnl_rs": per_trade_capital * pnl_pct / 100}
        peak = max(peak, bar["high"])
    exit_c = series[i + MAX_HOLD]["close"]
    pnl_pct = (exit_c - entry) / entry * 100
    return {"pnl_pct": pnl_pct, "pnl_rs": per_trade_capital * pnl_pct / 100}


def main():
    universe = load_universe()
    company_map = load_company_map()
    print("Building feature frame...", flush=True)
    df = build_stock_frame(universe, start="2026-03-01", end=WIN_END)
    by_sym, lut = frame_lookup(df)
    regime_map = build_regime_map(df)
    freshness_map = build_freshness_map(regime_map)

    print("Pass 1: technical agents + V7 hard reject gate...", flush=True)
    daily_candidates = defaultdict(list)
    day_returns = defaultdict(list)
    n_hard_rejected = 0
    n_stale_skipped_days = 0
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
                rejected, _ = hard_reject_v7(r)
                if rejected:
                    n_hard_rejected += 1
                    continue
                daily_candidates[d].append((sym, j, tech))
    n_pool = sum(len(v) for v in daily_candidates.values())
    print(f"Pool after V7 hard reject: {n_pool} (rejected {n_hard_rejected})", flush=True)

    sector_move_map = build_sector_move_map(day_returns, company_map)

    print("Pass 2: V7 scoring + V8 freshness gate...", flush=True)
    ranked_by_day = {}
    stale_days = []
    for d, rows in daily_candidates.items():
        regime = regime_map.get(d, {}).get("regime", "UNKNOWN")
        day_in_regime = freshness_map.get(d, 1)
        mult, hard_cut = freshness_multiplier(regime, day_in_regime)
        if hard_cut:
            ranked_by_day[d] = []
            stale_days.append((d, regime, day_in_regime))
            continue
        scored = []
        for sym, j, tech in rows:
            earn = earnings_calendar_agent(sym, d)
            news = news_catalyst_agent_v2(sym, d)
            sect = sector_rotation_agent(sym, d, sector_move_map)
            agent_results = {**tech, "earnings_calendar": earn, "news_catalyst_v2": news, "sector_rotation": sect}
            composite, n_fired, reasons = combine_v7(agent_results, regime)
            composite *= mult
            scored.append((sym, composite, j))
        ranked_by_day[d] = sorted(scored, key=lambda x: -x[1])

    print(f"Stale-trend days hard-cut (day>=7 of TRENDING regime): {len(stale_days)}")
    for d, reg, day_n in stale_days:
        print(f"  {d}  {reg}  day_in_regime={day_n}")

    for N in N_SIZES:
        per_trade_capital = TOTAL_CAPITAL / N
        all_trades = []
        for d, ranked in ranked_by_day.items():
            for sym, composite, j in ranked[:N]:
                series = by_sym[sym]
                atr = series[j].get("atr14")
                if not atr:
                    continue
                res = simulate_equity(series, j, atr, per_trade_capital)
                if res:
                    all_trades.append({"date": d, "symbol": sym, **res})
        if not all_trades:
            print(f"N={N}: no trades"); continue
        pnls = [t["pnl_pct"] for t in all_trades]
        rs = [t["pnl_rs"] for t in all_trades]
        w = [x for x in pnls if x > 0]
        print(f"\n=== V8 BASKET (N={N}) ===")
        print(f"trades={len(all_trades)}  WR={100*len(w)/len(pnls):.1f}%  avg/trade={statistics.mean(pnls):+.2f}%  "
              f"total_Rs={sum(rs):+,.0f}")
        for m, lab in ((lambda d: d < "2026-06-01", "May"), (lambda d: d >= "2026-06-01", "June")):
            sub = [t["pnl_pct"] for t in all_trades if m(t["date"])]
            if sub:
                sw = [x for x in sub if x > 0]
                print(f"  {lab}: n={len(sub)} WR={100*len(sw)/len(sub):.1f}% avg={statistics.mean(sub):+.2f}%")

    return ranked_by_day, by_sym


if __name__ == "__main__":
    main()
