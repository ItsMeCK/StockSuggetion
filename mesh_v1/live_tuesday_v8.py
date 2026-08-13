"""
Runs the full V8 mesh (V7 quality gate + asymmetric freshness gate + reweighted
router) on the most recently completed trading day (2026-07-06, Monday - EOD
data confirmed finalized, 499 symbols) to produce the Top-5 basket for the
NEXT session (Tuesday 2026-07-07).

Honest entry-timing note: this session's own validated finding (V1) is that
SAME-DAY-CLOSE entry beats next-day-open. These picks are scored off
Monday's close, but Monday's close has already passed - so in live practice
these become Tuesday-open entries, which the backtest never specifically
validated for V8. Flag this to the user, don't hide it.
"""
import os
from dotenv import load_dotenv

os.environ["TRADING_MODE"] = "HISTORICAL"
os.environ.setdefault("NEWS_LIVE_FETCH", "1")  # want live/fresh news for the actual live pick
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

SIGNAL_DATE = "2026-07-06"
N = 5

TECH_AGENTS = {
    "trend_continuation": trend_continuation_agent,
    "volume_thrust": volume_thrust_agent_v7,
    "relative_strength": relative_strength_agent,
    "oversold_reversal": oversold_reversal_agent,
}
SHORTLIST_MIN_SCORE, SHORTLIST_MIN_AGENTS = 40, 2


def main():
    universe = load_universe()
    company_map = load_company_map()
    print(f"Building feature frame through {SIGNAL_DATE}...", flush=True)
    df = build_stock_frame(universe, start="2026-04-01", end=SIGNAL_DATE)
    by_sym, lut = frame_lookup(df)
    regime_map = build_regime_map(df)
    freshness_map = build_freshness_map(regime_map)

    regime = regime_map.get(SIGNAL_DATE, {}).get("regime", "UNKNOWN")
    day_in_regime = freshness_map.get(SIGNAL_DATE, 1)
    mult, hard_cut = freshness_multiplier(regime, day_in_regime)
    print(f"\n{SIGNAL_DATE} regime={regime}  day_in_regime={day_in_regime}  "
          f"freshness_mult={mult}  hard_cut={hard_cut}")
    if hard_cut:
        print("STALE TRENDING_UP (day 7+) - V8 says STAND DOWN, no trades today.")
        return

    day_returns = {}
    candidates = []
    for sym, series in by_sym.items():
        if not series or series[-1]["time"].strftime("%Y-%m-%d") != SIGNAL_DATE:
            continue
        r = series[-1]
        j = len(series) - 1
        if r.get("atr14") is None:
            continue
        prev_c = r.get("prev_close")
        if prev_c:
            day_returns.setdefault(SIGNAL_DATE, []).append((sym, (r["close"] - prev_c) / prev_c * 100))
        tech = {name: fn(r) for name, fn in TECH_AGENTS.items()}
        n_fired = sum(1 for s, _ in tech.values() if s > 0)
        max_score = max((s for s, _ in tech.values()), default=0)
        if max_score >= SHORTLIST_MIN_SCORE or n_fired >= SHORTLIST_MIN_AGENTS:
            rejected, reject_reasons = hard_reject_v7(r)
            if rejected:
                continue
            candidates.append((sym, j, tech, r))

    print(f"Candidates after V7 hard reject: {len(candidates)}", flush=True)
    sector_move_map = build_sector_move_map(day_returns, company_map)

    scored = []
    for sym, j, tech, r in candidates:
        earn = earnings_calendar_agent(sym, SIGNAL_DATE)
        news = news_catalyst_agent_v2(sym, SIGNAL_DATE)
        sect = sector_rotation_agent(sym, SIGNAL_DATE, sector_move_map)
        agent_results = {**tech, "earnings_calendar": earn, "news_catalyst_v2": news, "sector_rotation": sect}
        composite, n_fired, reasons = combine_v7(agent_results, regime)
        composite *= mult
        scored.append((sym, round(composite, 1), n_fired, reasons, r["close"], r.get("atr14")))

    ranked = sorted(scored, key=lambda x: -x[1])[:N]
    print(f"\n=== V8 TOP-{N} PICKS (signal day {SIGNAL_DATE}, regime={regime}) ===")
    print("These are scored off Monday's close (already passed). Backtest validated")
    print("SAME-DAY-close entry; live equivalent now is TUESDAY-OPEN entry - flagged,")
    print("not backtested at this exact timing, expect some slippage vs backtest WR.\n")
    for i, (sym, comp, n_fired, reasons, close, atr) in enumerate(ranked, 1):
        stop = close - 1.5 * atr if atr else None
        print(f"{i}. {sym:12s} composite={comp:5.1f}  n_agents={n_fired}  "
              f"mon_close={close:9.2f}  suggested_stop={stop:9.2f}" if stop else
              f"{i}. {sym:12s} composite={comp:5.1f}  n_agents={n_fired}  mon_close={close:9.2f}")
        print(f"     reasons: {reasons}")


if __name__ == "__main__":
    main()
