"""
Exports the full N=5 V7 basket trade log (every real trade, not a sample) to
CSV: symbol, entry_date, entry_price, exit_date, exit_price, exit_reason,
days_held, pnl_pct, pnl_rs, composite, regime. Same engine as basket_v7.py,
just N=5 only with per-trade detail retained instead of aggregated.
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
from mesh_v1.basket_v7 import combine_v7, BASE_WEIGHTS

WIN_START, WIN_END = "2026-05-01", "2026-06-30"
STOP_ATR, TRAIL_TRIGGER, TRAIL_ATR, MAX_HOLD = 1.5, 0.05, 2.0, 5
TOTAL_CAPITAL = 100000.0
N = 5
per_trade_capital = TOTAL_CAPITAL / N

TECH_AGENTS = {
    "trend_continuation": trend_continuation_agent,
    "volume_thrust": volume_thrust_agent_v7,
    "relative_strength": relative_strength_agent,
    "oversold_reversal": oversold_reversal_agent,
}
SHORTLIST_MIN_SCORE, SHORTLIST_MIN_AGENTS = 40, 2


def simulate_equity_detailed(series, i, atr):
    entry_date = series[i]["time"].strftime("%Y-%m-%d")
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
            return {
                "entry_date": entry_date, "entry_price": round(entry, 2),
                "exit_date": bar["time"].strftime("%Y-%m-%d"), "exit_price": round(stop, 2),
                "exit_reason": "stopped_out", "days_held": d,
                "pnl_pct": round(pnl_pct, 2), "pnl_rs": round(per_trade_capital * pnl_pct / 100, 2),
            }
        peak = max(peak, bar["high"])
    last_bar = series[i + MAX_HOLD]
    exit_c = last_bar["close"]
    pnl_pct = (exit_c - entry) / entry * 100
    return {
        "entry_date": entry_date, "entry_price": round(entry, 2),
        "exit_date": last_bar["time"].strftime("%Y-%m-%d"), "exit_price": round(exit_c, 2),
        "exit_reason": "time_exit_5d", "days_held": MAX_HOLD,
        "pnl_pct": round(pnl_pct, 2), "pnl_rs": round(per_trade_capital * pnl_pct / 100, 2),
    }


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
                    continue
                daily_candidates[d].append((sym, j, tech))

    sector_move_map = build_sector_move_map(day_returns, company_map)

    print("Pass 2: V7 reweighted scoring...", flush=True)
    ranked_by_day = {}
    for d, rows in daily_candidates.items():
        regime = regime_map.get(d, {}).get("regime", "UNKNOWN")
        scored = []
        for sym, j, tech in rows:
            earn = earnings_calendar_agent(sym, d)
            news = news_catalyst_agent_v2(sym, d)
            sect = sector_rotation_agent(sym, d, sector_move_map)
            agent_results = {**tech, "earnings_calendar": earn, "news_catalyst_v2": news, "sector_rotation": sect}
            composite, n_fired, reasons = combine_v7(agent_results, regime)
            scored.append((sym, composite, j, regime))
        ranked_by_day[d] = sorted(scored, key=lambda x: -x[1])

    print("Pass 3: simulating each trade + building full log...", flush=True)
    rows_out = []
    for d in sorted(ranked_by_day):
        for sym, composite, j, regime in ranked_by_day[d][:N]:
            series = by_sym[sym]
            atr = series[j].get("atr14")
            if not atr:
                continue
            res = simulate_equity_detailed(series, j, atr)
            if res:
                rows_out.append({"symbol": sym, "regime": regime, "composite": composite, **res})

    out_path = "mesh_v1/n5_trades_full.csv"
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "symbol", "entry_date", "entry_price", "exit_date", "exit_price",
            "exit_reason", "days_held", "pnl_pct", "pnl_rs", "composite", "regime",
        ])
        writer.writeheader()
        writer.writerows(rows_out)

    pnls = [r["pnl_pct"] for r in rows_out]
    w = [x for x in pnls if x > 0]
    print(f"\nTotal trades exported: {len(rows_out)}  WR={100*len(w)/len(pnls):.1f}%  "
          f"avg={statistics.mean(pnls):+.2f}%  total_Rs={sum(r['pnl_rs'] for r in rows_out):+,.0f}")
    print(f"Saved -> {out_path}")


if __name__ == "__main__":
    main()
