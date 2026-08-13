"""
Runs the SAME mesh (technical agents + router, cache-only news for speed) for
Thursday 2026-07-02 and Friday 2026-07-03 using REAL closing data (not live
intraday), gets the top-10 basket for each day, then checks what each of
those exact stocks did from that day's close through TODAY's live price -
the real test of whether the mesh catches moves early (follow-through) or is
just chasing stocks that already popped that same day.

Usage: TRADING_MODE=HISTORICAL PYTHONPATH=. venv/bin/python3 mesh_v1/thu_fri_followthrough.py
"""
import os
import json
import time
from collections import defaultdict

os.environ["TRADING_MODE"] = "HISTORICAL"
os.environ.setdefault("NEWS_LIVE_FETCH", "0")
from dotenv import load_dotenv
load_dotenv()
from kiteconnect import KiteConnect

from mesh_v1.features import build_stock_frame, load_universe, load_company_map, frame_lookup
from mesh_v1.regime_agent import build_regime_map
from mesh_v1.technical_agents import (
    trend_continuation_agent, volume_thrust_agent, relative_strength_agent, oversold_reversal_agent,
)
from mesh_v1.news_agents import earnings_calendar_agent, news_catalyst_agent_v2
from mesh_v1.sector_agent import build_sector_move_map, sector_rotation_agent
from mesh_v1.router import combine

TECH_AGENTS = {
    "trend_continuation": trend_continuation_agent,
    "volume_thrust": volume_thrust_agent,
    "relative_strength": relative_strength_agent,
    "oversold_reversal": oversold_reversal_agent,
}
SHORTLIST_MIN_SCORE, SHORTLIST_MIN_AGENTS = 40, 2
TARGET_DATES = ["2026-07-02", "2026-07-03"]


def main():
    universe = load_universe()
    company_map = load_company_map()
    print("Building feature frame through 2026-07-03...", flush=True)
    df = build_stock_frame(universe, start="2026-04-01", end="2026-07-03")
    by_sym, lut = frame_lookup(df)
    regime_map = build_regime_map(df)

    day_returns = defaultdict(list)
    daily_candidates = defaultdict(list)
    for sym, series in by_sym.items():
        for j, r in enumerate(series):
            d = r["time"].strftime("%Y-%m-%d")
            if d not in TARGET_DATES or r.get("atr14") is None:
                continue
            prev_c = r.get("prev_close")
            if prev_c:
                day_returns[d].append((sym, (r["close"] - prev_c) / prev_c * 100))
            tech = {name: fn(r) for name, fn in TECH_AGENTS.items()}
            n_fired = sum(1 for s, _ in tech.values() if s > 0)
            max_score = max((s for s, _ in tech.values()), default=0)
            if max_score >= SHORTLIST_MIN_SCORE or n_fired >= SHORTLIST_MIN_AGENTS:
                daily_candidates[d].append((sym, j, tech, r["close"]))

    sector_move_map = build_sector_move_map(day_returns, company_map)

    all_picks = {}
    for d in TARGET_DATES:
        regime = regime_map.get(d, {}).get("regime", "UNKNOWN")
        scored = []
        for sym, j, tech, close_price in daily_candidates.get(d, []):
            earn = earnings_calendar_agent(sym, d)
            news = news_catalyst_agent_v2(sym, d)
            sect = sector_rotation_agent(sym, d, sector_move_map)
            agent_results = {**tech, "earnings_calendar": earn, "news_catalyst_v2": news, "sector_rotation": sect}
            composite, n_fired, reasons = combine(agent_results, regime)
            scored.append((sym, composite, close_price, list(reasons.keys())))
        ranked = sorted(scored, key=lambda x: -x[1])[:10]
        all_picks[d] = ranked
        print(f"\n{d} (regime={regime}): top-10 picks, {len(scored)} candidates total")
        for i, (sym, comp, price, reasons) in enumerate(ranked, 1):
            print(f"  {i:2d} {sym:12s} composite={comp:5.1f}  close={price:9.2f}  {reasons}")

    # --- fetch TODAY's live price for every picked symbol ---
    print("\nFetching live prices for follow-through check...", flush=True)
    kite = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
    kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN").strip("'"))
    all_syms = sorted(set(sym for picks in all_picks.values() for sym, *_ in picks))
    live = {}
    CHUNK = 200
    keys = [f"NSE:{s}" for s in all_syms]
    for i in range(0, len(keys), CHUNK):
        q = kite.quote(keys[i:i + CHUNK])
        for k, v in q.items():
            live[k.split(":", 1)[1]] = v["last_price"]
        time.sleep(0.34)

    print("\n" + "=" * 100)
    print("FOLLOW-THROUGH CHECK: pick-day close -> TODAY's live price")
    print("=" * 100)
    for d, ranked in all_picks.items():
        print(f"\n--- Picks from {d} ---")
        print(f"{'Symbol':12s} {'Pick close':>11s} {'Live now':>10s} {'Change':>8s}  Interpretation")
        rets = []
        for sym, comp, price, reasons in ranked:
            now_p = live.get(sym)
            if now_p is None:
                print(f"{sym:12s}  (no live quote)")
                continue
            chg = (now_p - price) / price * 100
            rets.append(chg)
            tag = "continued UP" if chg > 1 else ("continued DOWN" if chg < -1 else "flat/chop")
            print(f"{sym:12s} {price:11.2f} {now_p:10.2f} {chg:+7.1f}%  {tag}")
        if rets:
            import statistics
            print(f"  --> avg change since pick: {statistics.mean(rets):+.2f}%  "
                  f"({sum(1 for r in rets if r>0)}/{len(rets)} still up)")

    json.dump({d: [(s, c, p, r) for s, c, p, r in v] for d, v in all_picks.items()},
              open("thu_fri_picks.json", "w"), indent=2, default=str)
    print("\nSaved -> thu_fri_picks.json")


if __name__ == "__main__":
    main()
