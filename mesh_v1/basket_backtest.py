"""
BASKET STRATEGY: instead of picking THE ONE best candidate per day (proven,
4 separate ways, to be an unsolved ranking problem), take the top-N ranked
candidates each day, equal-weight capital across them, and let the pool's
aggregate edge play out (V4 showed the qualifying pool itself is profitable
in aggregate even when the single top-pick wasn't).

Same detectors, same router/composite scoring, same ATR exit as
backtest_mesh.py - just a different SELECTION rule (top-N basket instead of
top-1). Tests N = 5, 10, 15, 20 to find a practical, capital-realistic size.

Usage: TRADING_MODE=HISTORICAL PYTHONPATH=. venv/bin/python3 mesh_v1/basket_backtest.py
"""
import os
import json
import statistics
from collections import defaultdict

os.environ["TRADING_MODE"] = "HISTORICAL"
os.environ.setdefault("NEWS_LIVE_FETCH", "0")  # cache-only for speed; coverage caveat noted
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
STOP_ATR, TRAIL_TRIGGER, TRAIL_ATR, MAX_HOLD = 1.5, 0.05, 2.0, 5
TOTAL_CAPITAL = 100000.0  # Rs1L, split across the basket each day
BASKET_SIZES = [3, 5, 10, 15, 20]

TECH_AGENTS = {
    "trend_continuation": trend_continuation_agent,
    "volume_thrust": volume_thrust_agent,
    "relative_strength": relative_strength_agent,
    "oversold_reversal": oversold_reversal_agent,
}
SHORTLIST_MIN_SCORE, SHORTLIST_MIN_AGENTS = 40, 2  # a middling gate - loose enough for a real
                                                    # ranked list to select a top-N FROM


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
            return {"pnl_pct": pnl_pct, "pnl_rs": per_trade_capital * pnl_pct / 100, "days": d}
        peak = max(peak, bar["high"])
    exit_c = series[i + MAX_HOLD]["close"]
    pnl_pct = (exit_c - entry) / entry * 100
    return {"pnl_pct": pnl_pct, "pnl_rs": per_trade_capital * pnl_pct / 100, "days": MAX_HOLD}


def main():
    universe = load_universe()
    company_map = load_company_map()
    print(f"Universe: {len(universe)}. Building feature frame {WIN_START}..{WIN_END}...", flush=True)
    df = build_stock_frame(universe, start="2026-03-01", end=WIN_END)
    by_sym, lut = frame_lookup(df)
    print("Building regime map...", flush=True)
    regime_map = build_regime_map(df)

    print("Pass 1: technical agents + ranking pool...", flush=True)
    daily_candidates = defaultdict(list)
    day_returns = defaultdict(list)
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
    n_pool = sum(len(v) for v in daily_candidates.values())
    print(f"Ranking pool: {n_pool} stock-days across {len(daily_candidates)} days "
          f"(median/day: {sorted(len(v) for v in daily_candidates.values())[len(daily_candidates)//2]})", flush=True)

    sector_move_map = build_sector_move_map(day_returns, company_map)

    print("Pass 2: composite scoring (cache-only news)...", flush=True)
    ranked_by_day = {}
    processed = 0
    for d, rows in daily_candidates.items():
        regime = regime_map.get(d, {}).get("regime", "UNKNOWN")
        scored = []
        for sym, j, tech in rows:
            earn = earnings_calendar_agent(sym, d)
            news = news_catalyst_agent_v2(sym, d)
            sect = sector_rotation_agent(sym, d, sector_move_map)
            agent_results = {**tech, "earnings_calendar": earn, "news_catalyst_v2": news, "sector_rotation": sect}
            composite, n_fired, reasons = combine(agent_results, regime)
            scored.append((sym, composite, j))
            processed += 1
            if processed % 2000 == 0:
                print(f"  scored {processed}/{n_pool}", flush=True)
        ranked_by_day[d] = sorted(scored, key=lambda x: -x[1])

    print("\nRunning basket backtests at multiple sizes...", flush=True)
    for N in BASKET_SIZES:
        per_trade_capital = TOTAL_CAPITAL / N
        all_trades = []
        for d, ranked in ranked_by_day.items():
            top_n = ranked[:N]
            for sym, composite, j in top_n:
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
        # daily portfolio return (avg of that day's basket, not sum - reflects capital reality)
        by_day_ret = defaultdict(list)
        for t in all_trades:
            by_day_ret[t["date"]].append(t["pnl_pct"])
        daily_rets = [statistics.mean(v) for v in by_day_ret.values()]
        cum_capital = TOTAL_CAPITAL
        for dr in [by_day_ret[d] for d in sorted(by_day_ret)]:
            cum_capital *= (1 + statistics.mean(dr) / 100)
        print(f"\nN={N:2d} (Rs{per_trade_capital:,.0f}/position): trades={len(all_trades)}  "
              f"WR={100*len(w)/len(pnls):.1f}%  avg/trade={statistics.mean(pnls):+.2f}%  "
              f"total_Rs={sum(rs):+,.0f}  compounded_end_capital=Rs{cum_capital:,.0f} "
              f"({100*(cum_capital-TOTAL_CAPITAL)/TOTAL_CAPITAL:+.1f}%)")

    json.dump({d: [(s, c) for s, c, j in v] for d, v in ranked_by_day.items()},
              open("basket_ranked_by_day.json", "w"), indent=2, default=str)
    print("\nSaved -> basket_ranked_by_day.json")


if __name__ == "__main__":
    main()
