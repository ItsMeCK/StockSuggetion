"""
V8: regime-FRESHNESS gate. Confirmed directly on our own data (2026-07-06):
the current TRENDING_UP regime (started 2026-06-17) is exactly where the
9-trade, 22.2% WR loss cluster (2026-06-22 to 2026-06-30) sits - those are
day 4-9 of an aging uptrend. This mirrors the ALREADY-LOCKED baseline finding
from 2026-07-05 (June 15 Day-1 WR 63.2% vs June 22+ stale WR 29.3%): a
trending regime's edge decays fast as it ages. The mesh currently treats
every day of TRENDING_UP/TRENDING_DOWN identically - this fixes that.

build_freshness_map: for each date, count consecutive trading days the
CURRENT regime label has held (day_in_regime=1 on a fresh turn).

freshness_multiplier: ASYMMETRIC by design, and this asymmetry is itself a
confirmed finding, not an assumption. Tested applying the same day-7+ hard
cut to BOTH TRENDING_UP and TRENDING_DOWN: the aging-TRENDING_DOWN trades
(day 7-10, e.g. 2026-05-19..21 and 2026-06-08..11) turned out to be 77.1% WR
/ +Rs23,871 - excellent, not stale. Aging TRENDING_UP (2026-06-22..30, day
4-9) was the opposite: 22.2% WR / -Rs5,799. This makes trading sense: in
TRENDING_DOWN the mesh leans on oversold_reversal_agent (1.4x weight) -
contrarian long entries that get MORE washed-out and reliable the longer a
decline runs, not less (Weinstein/Pring: deeper capitulation = better
reversal setup). In TRENDING_UP the mesh leans on trend_continuation /
relative_strength - chasing a rally that is long-only-only-exit; the longer
it runs uncorrected, the closer to its own exhaustion. So: decay/hard-cut
applies ONLY to an aging TRENDING_UP regime. TRENDING_DOWN and CHOPPY are
left alone here (CHOPPY already regime-penalized separately).
"""


def build_freshness_map(regime_map):
    dates = sorted(regime_map.keys())
    out = {}
    prev_regime = None
    streak_start_idx = 0
    for i, d in enumerate(dates):
        reg = regime_map[d]["regime"]
        if reg != prev_regime:
            streak_start_idx = i
            prev_regime = reg
        out[d] = (i - streak_start_idx) + 1
    return out


def freshness_multiplier(regime, day_in_regime):
    if regime != "TRENDING_UP":
        return 1.0, False
    if day_in_regime <= 3:
        return 1.0, False
    if day_in_regime <= 6:
        return 0.55, False
    return 0.0, True  # hard cut: aging long-only chase, proven negative edge
