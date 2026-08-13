"""
For each category in mover_categorization_final.csv, compute what fraction of
those moves the V5 mesh's shortlist (Pass 1 technical agents + the news/sector
cache) actually flags on the move's start date. Adds a 'captured_by_mesh'
column per-move and prints a per-category capture-rate summary.

Reuses the SAME shortlist-building logic as mesh_v1/backtest_mesh.py (not a
reimplementation - imports directly) so the numbers are consistent with the
mesh actually built.
"""
import os
import csv
import json
from collections import defaultdict

os.environ["TRADING_MODE"] = "HISTORICAL"
from dotenv import load_dotenv
load_dotenv()

from mesh_v1.features import build_stock_frame, load_universe, load_company_map, frame_lookup
from mesh_v1.technical_agents import (
    trend_continuation_agent, volume_thrust_agent, relative_strength_agent, oversold_reversal_agent,
)
from mesh_v1.news_agents import earnings_calendar_agent, news_catalyst_agent_v2
from mesh_v1.sector_agent import build_sector_move_map, sector_rotation_agent

WIN_START, WIN_END = "2026-05-01", "2026-06-30"
TECH_AGENTS = {
    "trend_continuation": trend_continuation_agent,
    "volume_thrust": volume_thrust_agent,
    "relative_strength": relative_strength_agent,
    "oversold_reversal": oversold_reversal_agent,
}


def main():
    universe = load_universe()
    company_map = load_company_map()
    print("Building feature frame...")
    df = build_stock_frame(universe, start="2026-03-01", end=WIN_END)
    by_sym, lut = frame_lookup(df)

    print("Pass 1: technical agents (shortlist = any agent fires, matches original archetype def)...")
    shortlisted = set()          # (symbol, date) flagged by >=1 technical agent
    shortlisted_with_news = set()  # additionally confirmed by news/sector (cache-available only)
    day_returns = defaultdict(list)
    tech_hits = defaultdict(list)  # date -> [(symbol, tech_dict)]
    for sym, series in by_sym.items():
        for j, r in enumerate(series):
            d = r["time"].strftime("%Y-%m-%d")
            if not (WIN_START <= d <= WIN_END) or r.get("atr14") is None:
                continue
            prev_c = r.get("prev_close")
            if prev_c:
                day_returns[d].append((sym, (r["close"] - prev_c) / prev_c * 100))
            tech = {name: fn(r) for name, fn in TECH_AGENTS.items()}
            if any(s > 0 for s, _ in tech.values()):
                shortlisted.add((sym, d))
                tech_hits[d].append((sym, tech))

    sector_move_map = build_sector_move_map(day_returns, company_map)

    # --- annotate every mover with capture flags ---
    rows = list(csv.DictReader(open("mover_categorization_final.csv")))

    # BUGFIX: news/sector agents must be checked for EVERY mover directly, not
    # gated behind "a technical agent already fired" - that gate excluded
    # almost every pure-news mover (they have no chart signature BY DEFINITION,
    # that's why they're in a NEWS/NO_NEWS category), so the news agent was
    # never even asked about them. Check the actual mover list independently.
    print(f"Checking news/sector confirmation for all {len(rows)} movers directly "
          f"(NEWS_LIVE_FETCH={os.getenv('NEWS_LIVE_FETCH', '0')})...")
    for i, r in enumerate(rows):
        sym, d = r["symbol"], r["start"]
        if i % 200 == 0:
            print(f"  progress {i}/{len(rows)}", flush=True)
        earn = earnings_calendar_agent(sym, d)
        news = news_catalyst_agent_v2(sym, d)
        sect = sector_rotation_agent(sym, d, sector_move_map)
        if earn[0] > 0 or news[0] > 0 or sect[0] > 0:
            shortlisted_with_news.add((sym, d))
    for r in rows:
        key = (r["symbol"], r["start"])
        r["captured_by_tech_shortlist"] = "YES" if key in shortlisted else "NO"
        r["captured_with_news_confirm"] = "YES" if key in shortlisted_with_news else "NO"
        r["captured_combined"] = "YES" if (key in shortlisted or key in shortlisted_with_news) else "NO"

    fieldnames = ["month", "symbol", "start", "end", "days", "move_pct", "primary", "all_patterns",
                 "captured_by_tech_shortlist", "captured_with_news_confirm", "captured_combined"]
    with open("mover_categorization_with_capture.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(sorted(rows, key=lambda x: (x["month"], -float(x["move_pct"]))))

    # --- per-category capture summary (tech-only vs tech+news combined) ---
    by_cat = defaultdict(lambda: [0, 0, 0])  # total, tech_captured, tech_or_news_captured
    for r in rows:
        key = (r["symbol"], r["start"])
        by_cat[r["primary"]][0] += 1
        if r["captured_by_tech_shortlist"] == "YES":
            by_cat[r["primary"]][1] += 1
        if r["captured_by_tech_shortlist"] == "YES" or key in shortlisted_with_news:
            by_cat[r["primary"]][2] += 1

    order = ["ma_stack_rising", "volume_thrust_2x", "hh_hl_uptrend", "rs_beats_nifty", "above_rising_20sma",
             "strong_close", "momentum_roc10>5", "near_20d_high", "base_breakout",
             "NEWS:earnings/results", "NEWS:order/contract win", "NEWS:regulatory/approval",
             "NEWS:m&a/stake/block_deal", "NEWS:litigation/penalty", "NEWS:analyst/rating",
             "NEWS:corp_action", "NEWS:management/leadership",
             "SECTOR_PEER_MOVE", "TECHNICAL:deep_oversold_bounce", "TECHNICAL:near_ma_ambiguous", "NO_NEWS_FOUND"]

    print("\n" + "=" * 90)
    print("CAPTURE RATE BY CATEGORY: technical-only vs technical+news (bug fixed: news no longer gated)")
    print("=" * 90)
    print(f"{'Category':30s} {'Total':>6s} {'Tech-only':>10s} {'Tech%':>7s} {'+News':>7s} {'+News%':>8s}")
    total = tech_tot = comb_tot = 0
    for cat in order:
        if cat in by_cat:
            t, tc, cc = by_cat[cat]
            total += t; tech_tot += tc; comb_tot += cc
            print(f"{cat:30s} {t:6d} {tc:10d} {100*tc/t:6.1f}% {cc:7d} {100*cc/t:7.1f}%")
    print(f"{'TOTAL':30s} {total:6d} {tech_tot:10d} {100*tech_tot/total:6.1f}% {comb_tot:7d} {100*comb_tot/total:7.1f}%")

    print("\nSaved -> mover_categorization_with_capture.csv")


if __name__ == "__main__":
    main()
