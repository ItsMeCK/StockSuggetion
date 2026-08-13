"""
Tests whether the Pring exhaustion/key-reversal/Pinocchio REJECT filter
actually improves PRECISION (not just recall) on the same May-June data.

Reports, before and after applying the reject filter:
  - shortlist size, precision, recall
This is the real test: does rejecting false-signal bars raise precision
while keeping recall reasonably intact, or does it just shrink everything
proportionally (no real discriminative value)?
"""
import os
import csv
from mesh_v1.features import build_stock_frame, load_universe, frame_lookup
from mesh_v1.technical_agents import (
    trend_continuation_agent, volume_thrust_agent, relative_strength_agent, oversold_reversal_agent,
)
from mesh_v1.exhaustion_reject_agent import exhaustion_reject

os.environ["TRADING_MODE"] = "HISTORICAL"
TECH_AGENTS = {
    "t": trend_continuation_agent, "v": volume_thrust_agent,
    "r": relative_strength_agent, "o": oversold_reversal_agent,
}
WIN_START, WIN_END = "2026-05-01", "2026-06-30"


def main():
    universe = load_universe()
    df = build_stock_frame(universe, start="2026-04-01", end="2026-06-30")
    by_sym, lut = frame_lookup(df)

    shortlisted = set()
    shortlisted_after_reject = set()
    reject_reasons_hit = {"key_reversal": 0, "exhaustion": 0, "pinocchio": 0}
    for sym, series in by_sym.items():
        for r in series:
            d = r["time"].strftime("%Y-%m-%d")
            if not (WIN_START <= d <= WIN_END) or r.get("atr14") is None:
                continue
            tech = {n: fn(r) for n, fn in TECH_AGENTS.items()}
            if not any(s > 0 for s, _ in tech.values()):
                continue
            shortlisted.add((sym, d))
            rejected, reasons = exhaustion_reject(r)
            if rejected:
                for k, v in reasons.items():
                    if v:
                        reject_reasons_hit[k] += 1
            else:
                shortlisted_after_reject.add((sym, d))

    rows = list(csv.DictReader(open("mover_categorization_with_capture.csv")))
    mover_keys = set((row["symbol"], row["start"]) for row in rows)

    def report(label, sl):
        cap = len(sl & mover_keys)
        precision = 100 * cap / len(sl) if sl else 0
        recall = 100 * cap / len(mover_keys)
        print(f"{label}: shortlist={len(sl):6d}  real_movers_in_it={cap:5d}  "
              f"PRECISION={precision:5.1f}%  RECALL={recall:5.1f}%")

    print("=" * 80)
    report("BEFORE reject filter", shortlisted)
    report("AFTER  reject filter", shortlisted_after_reject)
    print(f"\nRejected {len(shortlisted) - len(shortlisted_after_reject)} of {len(shortlisted)} "
          f"candidates ({100*(len(shortlisted)-len(shortlisted_after_reject))/len(shortlisted):.1f}%)")
    print(f"Reject reason breakdown (non-exclusive): {reject_reasons_hit}")

    # How many REAL movers did we lose by rejecting? (false negatives introduced)
    lost_movers = (shortlisted & mover_keys) - (shortlisted_after_reject & mover_keys)
    print(f"\nReal movers REMOVED by the reject filter (cost): {len(lost_movers)} "
          f"of {len(shortlisted & mover_keys)}")


if __name__ == "__main__":
    main()
