"""
V7: quality-gated basket. Same universe/detectors/exit as basket_backtest.py
(V6) but with the loss-autopsy fixes applied:
  1. volume_thrust_agent's near-20d-high bonus inverted to a penalty
  2. hard reject on climax volume (>4x) or exhaustion-scale bar (>=2.2x ATR)
  3. router reweighted: earnings/news up, sector_rotation down (autopsy
     showed sector_rotation firing was NEGATIVE alpha, 44.3% WR)

Goal: see how close a genuinely quality-first gate gets to 70% WR, and at
what cost in trade frequency - report the real number, not a rounded one.
"""
import os
import json
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

WIN_START, WIN_END = "2026-05-01", "2026-06-30"
STOP_ATR, TRAIL_TRIGGER, TRAIL_ATR, MAX_HOLD = 1.5, 0.05, 2.0, 5
TOTAL_CAPITAL = 100000.0
N_SIZES = [3, 5, 10]
SKIP_CHOPPY = os.environ.get("SKIP_CHOPPY", "0") == "1"

TECH_AGENTS = {
    "trend_continuation": trend_continuation_agent,
    "volume_thrust": volume_thrust_agent_v7,
    "relative_strength": relative_strength_agent,
    "oversold_reversal": oversold_reversal_agent,
}
SHORTLIST_MIN_SCORE, SHORTLIST_MIN_AGENTS = 40, 2

# V7 reweight: earnings_calendar (68.2% WR) and news_catalyst_v2 (59.3% WR)
# were the best individual agents in the autopsy - raised. sector_rotation
# (44.3% WR, negative alpha) - cut hard. Technicals left as base, since the
# quality gate (near-high penalty + hard reject) already fixes their worst offender.
BASE_WEIGHTS = {
    "trend_continuation": 1.0,
    "volume_thrust": 1.0,
    "relative_strength": 0.8,
    "oversold_reversal": 0.9,       # was 0.6 -> autopsy showed 63.3% WR, raise
    "earnings_calendar": 1.4,       # was 0.9 -> autopsy showed 68.2% WR, raise
    "news_catalyst_v2": 1.1,        # was 0.7 -> autopsy showed 59.3% WR, raise
    "sector_rotation": 0.15,        # was 0.5 -> autopsy showed 44.3% WR, cut hard
}
REGIME_MULTIPLIERS = {
    "TRENDING_UP": {"trend_continuation": 1.3, "relative_strength": 1.2},
    "CHOPPY": {"trend_continuation": 0.5, "volume_thrust": 1.1, "earnings_calendar": 1.3,
              "news_catalyst_v2": 1.3, "sector_rotation": 1.2},
    "TRENDING_DOWN": {"trend_continuation": 0.3, "relative_strength": 0.6, "oversold_reversal": 1.4},
    "UNKNOWN": {},
}
REGIME_GLOBAL_PENALTY = {"TRENDING_UP": 1.0, "CHOPPY": 0.75, "TRENDING_DOWN": 0.65, "UNKNOWN": 0.85}
# CHOPPY penalty tightened 0.9->0.75: autopsy showed CHOPPY was the weakest
# regime by avg P&L (+0.22%/trade) even under the old discount.


def weights_for_regime(regime):
    w = dict(BASE_WEIGHTS)
    for k, mult in REGIME_MULTIPLIERS.get(regime, {}).items():
        w[k] = w[k] * mult
    return w


def combine_v7(agent_results, regime):
    w = weights_for_regime(regime)
    total_w = sum(w.values())
    weighted_sum = sum(w[a] * s for a, (s, _) in agent_results.items() if a in w)
    n_fired = sum(1 for s, _ in agent_results.values() if s > 0)
    composite = weighted_sum / total_w if total_w else 0
    if n_fired >= 3:
        composite *= 1.15
    elif n_fired >= 2:
        composite *= 1.05
    composite *= REGIME_GLOBAL_PENALTY.get(regime, 0.85)
    composite = min(composite, 100)
    reasons = {a: r for a, (s, r) in agent_results.items() if s > 0}
    return round(composite, 1), n_fired, reasons


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

    print("Pass 1: technical agents + V7 hard reject gate...", flush=True)
    daily_candidates = defaultdict(list)
    day_returns = defaultdict(list)
    n_hard_rejected = 0
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
    print(f"Pool after V7 hard reject: {n_pool} (rejected {n_hard_rejected} climax/exhaustion bars)", flush=True)

    sector_move_map = build_sector_move_map(day_returns, company_map)

    print("Pass 2: V7 reweighted scoring...", flush=True)
    ranked_by_day = {}
    for d, rows in daily_candidates.items():
        regime = regime_map.get(d, {}).get("regime", "UNKNOWN")
        if SKIP_CHOPPY and regime == "CHOPPY":
            ranked_by_day[d] = []
            continue
        scored = []
        for sym, j, tech in rows:
            earn = earnings_calendar_agent(sym, d)
            news = news_catalyst_agent_v2(sym, d)
            sect = sector_rotation_agent(sym, d, sector_move_map)
            agent_results = {**tech, "earnings_calendar": earn, "news_catalyst_v2": news, "sector_rotation": sect}
            composite, n_fired, reasons = combine_v7(agent_results, regime)
            scored.append((sym, composite, j))
        ranked_by_day[d] = sorted(scored, key=lambda x: -x[1])

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
        pnls = [t["pnl_pct"] for t in all_trades]
        rs = [t["pnl_rs"] for t in all_trades]
        w = [x for x in pnls if x > 0]
        print(f"\n=== V7 BASKET (N={N}) ===")
        print(f"trades={len(all_trades)}  WR={100*len(w)/len(pnls):.1f}%  avg/trade={statistics.mean(pnls):+.2f}%  "
              f"total_Rs={sum(rs):+,.0f}")
        for m, lab in ((lambda d: d < "2026-06-01", "May"), (lambda d: d >= "2026-06-01", "June")):
            sub = [t["pnl_pct"] for t in all_trades if m(t["date"])]
            if sub:
                sw = [x for x in sub if x > 0]
                print(f"  {lab}: n={len(sub)} WR={100*len(sw)/len(sub):.1f}% avg={statistics.mean(sub):+.2f}%")


if __name__ == "__main__":
    main()
