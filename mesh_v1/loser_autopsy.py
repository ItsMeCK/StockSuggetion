"""
TRADER-HAT DIAGNOSTIC: instead of building another generic book-chapter filter,
look at OUR OWN actual basket trades (N=10, May-June) and find what actually
differentiates winners from losers. This is the step that was skipped -
Pring/Shannon filters were being added on theory, never checked against our
own loss autopsy.

For every one of the ~357 basket trades, capture rich entry-day features
(composite score, rank-in-basket, regime, which agents fired, extension %,
gap %, volume ratio, bar width vs ATR, day of week) and compare the
distributions between winners and losers to find real, data-backed levers
for tightening the shortlist toward higher win rate (at the cost of frequency).
"""
import os
import json
import statistics
from collections import defaultdict, Counter

os.environ["TRADING_MODE"] = "HISTORICAL"
os.environ.setdefault("NEWS_LIVE_FETCH", "0")
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
N = 10

TECH_AGENTS = {
    "trend_continuation": trend_continuation_agent,
    "volume_thrust": volume_thrust_agent,
    "relative_strength": relative_strength_agent,
    "oversold_reversal": oversold_reversal_agent,
}
SHORTLIST_MIN_SCORE, SHORTLIST_MIN_AGENTS = 40, 2


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
            return {"pnl_pct": (stop - entry) / entry * 100, "days": d, "exit": "stopped"}
        peak = max(peak, bar["high"])
    exit_c = series[i + MAX_HOLD]["close"]
    return {"pnl_pct": (exit_c - entry) / entry * 100, "days": MAX_HOLD, "exit": "time"}


def main():
    universe = load_universe()
    company_map = load_company_map()
    print("Building feature frame...", flush=True)
    df = build_stock_frame(universe, start="2026-03-01", end=WIN_END)
    by_sym, lut = frame_lookup(df)
    regime_map = build_regime_map(df)

    print("Pass 1: technical agents...", flush=True)
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
                daily_candidates[d].append((sym, j, tech, r))

    sector_move_map = build_sector_move_map(day_returns, company_map)

    print("Pass 2: scoring + capturing entry-day features...", flush=True)
    trades = []
    for d, rows in daily_candidates.items():
        regime = regime_map.get(d, {}).get("regime", "UNKNOWN")
        scored = []
        for sym, j, tech, r in rows:
            earn = earnings_calendar_agent(sym, d)
            news = news_catalyst_agent_v2(sym, d)
            sect = sector_rotation_agent(sym, d, sector_move_map)
            agent_results = {**tech, "earnings_calendar": earn, "news_catalyst_v2": news, "sector_rotation": sect}
            composite, n_fired, reasons = combine(agent_results, regime)
            scored.append((sym, composite, j, r, n_fired, list(reasons.keys())))
        ranked = sorted(scored, key=lambda x: -x[1])
        top = ranked[:N]
        max_composite = top[0][1] if top else 0
        for rank, (sym, composite, j, r, n_fired, agent_names) in enumerate(top, 1):
            series = by_sym[sym]
            atr = r.get("atr14")
            if not atr:
                continue
            res = simulate_equity(series, j, atr)
            if not res:
                continue
            sma20 = r.get("sma20")
            ext_pct = (r["close"] - sma20) / sma20 * 100 if sma20 else None
            vol_avg20 = r.get("vol_avg20")
            vol_ratio = r["volume"] / vol_avg20 if vol_avg20 else None
            gap_pct = r.get("gap_pct")
            rng = r["high"] - r["low"]
            bar_width_atr = rng / atr if atr else None
            weekday = r["time"].strftime("%A")
            prior_high20 = r.get("prior_high20")
            near_20d_high = bool(prior_high20 and r["close"] >= 0.98 * prior_high20)
            trades.append({
                "date": d, "symbol": sym, "regime": regime, "rank": rank,
                "composite": composite, "score_gap_from_top": max_composite - composite,
                "n_fired": n_fired, "agents": agent_names,
                "ext_pct_above_sma20": ext_pct, "vol_ratio": vol_ratio, "gap_pct": gap_pct,
                "bar_width_atr": bar_width_atr, "weekday": weekday, "near_20d_high": near_20d_high,
                **res,
            })

    win = [t for t in trades if t["pnl_pct"] > 0]
    lose = [t for t in trades if t["pnl_pct"] <= 0]
    print(f"\n{'='*90}\nTotal trades: {len(trades)}  Wins: {len(win)} ({100*len(win)/len(trades):.1f}%)  "
          f"Losses: {len(lose)} ({100*len(lose)/len(trades):.1f}%)\n{'='*90}")

    def compare(label, key, fn=lambda x: x):
        wv = [fn(t[key]) for t in win if t[key] is not None]
        lv = [fn(t[key]) for t in lose if t[key] is not None]
        if not wv or not lv:
            print(f"{label:35s}  (insufficient data)")
            return
        print(f"{label:35s}  WIN avg={statistics.mean(wv):8.2f}  median={statistics.median(wv):8.2f}   "
              f"LOSS avg={statistics.mean(lv):8.2f}  median={statistics.median(lv):8.2f}")

    print("\n--- Continuous feature comparison (winners vs losers) ---")
    compare("composite score", "composite")
    compare("rank in basket (1=best)", "rank")
    compare("score gap from day's #1", "score_gap_from_top")
    compare("n_fired agents", "n_fired")
    compare("extension % above 20sma", "ext_pct_above_sma20")
    compare("volume ratio (vs 20d avg)", "vol_ratio")
    compare("gap % at entry", "gap_pct")
    compare("bar width / ATR", "bar_width_atr")
    compare("days held", "days")

    print("\n--- Categorical: regime ---")
    for cat in ["TRENDING_UP", "CHOPPY", "TRENDING_DOWN", "UNKNOWN"]:
        wc = sum(1 for t in trades if t["regime"] == cat)
        wwins = sum(1 for t in trades if t["regime"] == cat and t["pnl_pct"] > 0)
        if wc:
            print(f"  {cat:15s} n={wc:4d}  WR={100*wwins/wc:5.1f}%  avg_pnl={statistics.mean([t['pnl_pct'] for t in trades if t['regime']==cat]):+.2f}%")

    print("\n--- Categorical: rank bucket (top-3 vs 4-10) ---")
    for lo, hi, lab in [(1, 3, "rank 1-3"), (4, 10, "rank 4-10")]:
        sub = [t for t in trades if lo <= t["rank"] <= hi]
        if sub:
            w2 = sum(1 for t in sub if t["pnl_pct"] > 0)
            print(f"  {lab:15s} n={len(sub):4d}  WR={100*w2/len(sub):5.1f}%  avg_pnl={statistics.mean([t['pnl_pct'] for t in sub]):+.2f}%")

    print("\n--- Categorical: near 20d high at entry ---")
    for flag in [True, False]:
        sub = [t for t in trades if t["near_20d_high"] == flag]
        if sub:
            w2 = sum(1 for t in sub if t["pnl_pct"] > 0)
            print(f"  near_20d_high={str(flag):6s}  n={len(sub):4d}  WR={100*w2/len(sub):5.1f}%  avg_pnl={statistics.mean([t['pnl_pct'] for t in sub]):+.2f}%")

    print("\n--- Categorical: weekday ---")
    for wd in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]:
        sub = [t for t in trades if t["weekday"] == wd]
        if sub:
            w2 = sum(1 for t in sub if t["pnl_pct"] > 0)
            print(f"  {wd:10s} n={len(sub):4d}  WR={100*w2/len(sub):5.1f}%  avg_pnl={statistics.mean([t['pnl_pct'] for t in sub]):+.2f}%")

    print("\n--- Categorical: exit type ---")
    for ex in ["stopped", "time"]:
        sub = [t for t in trades if t["exit"] == ex]
        if sub:
            w2 = sum(1 for t in sub if t["pnl_pct"] > 0)
            print(f"  {ex:10s} n={len(sub):4d}  WR={100*w2/len(sub):5.1f}%  avg_pnl={statistics.mean([t['pnl_pct'] for t in sub]):+.2f}%")

    print("\n--- Which single agent contributed, win rate when present ---")
    all_agent_names = set(a for t in trades for a in t["agents"])
    for a in sorted(all_agent_names):
        sub = [t for t in trades if a in t["agents"]]
        if sub:
            w2 = sum(1 for t in sub if t["pnl_pct"] > 0)
            print(f"  {a:20s} n={len(sub):4d}  WR={100*w2/len(sub):5.1f}%  avg_pnl={statistics.mean([t['pnl_pct'] for t in sub]):+.2f}%")

    print("\n--- Composite score bucket ---")
    for lo, hi in [(0, 30), (30, 40), (40, 50), (50, 60), (60, 200)]:
        sub = [t for t in trades if lo <= t["composite"] < hi]
        if sub:
            w2 = sum(1 for t in sub if t["pnl_pct"] > 0)
            print(f"  composite [{lo:3d},{hi:3d})  n={len(sub):4d}  WR={100*w2/len(sub):5.1f}%  avg_pnl={statistics.mean([t['pnl_pct'] for t in sub]):+.2f}%")

    json.dump(trades, open("mesh_v1/loser_autopsy_trades.json", "w"), indent=2, default=str)
    print("\nSaved -> mesh_v1/loser_autopsy_trades.json")


if __name__ == "__main__":
    main()
