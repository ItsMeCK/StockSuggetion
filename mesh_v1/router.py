"""
Dynamic Router — combines all detector agents into one conviction score per
(symbol, date), with REGIME-CONDITIONAL weighting (Shannon: trend-following
signals are unreliable in chop; news/event catalysts are regime-agnostic —
validated this session: fresh-turn WR 56-63% vs stale-chop WR 29%), and a
Pring "weight of evidence" bonus for multiple independent confirmations.

Selects the single best candidate per day (the 1-slot constraint).
"""
from collections import defaultdict

BASE_WEIGHTS = {
    "trend_continuation": 1.0,
    "volume_thrust": 1.0,
    "relative_strength": 0.8,
    "oversold_reversal": 0.6,
    "earnings_calendar": 0.9,
    "news_catalyst_v2": 0.7,
    "sector_rotation": 0.5,
}

REGIME_MULTIPLIERS = {
    "TRENDING_UP": {"trend_continuation": 1.3, "relative_strength": 1.2},
    "CHOPPY": {"trend_continuation": 0.5, "volume_thrust": 1.1, "earnings_calendar": 1.3,
              "news_catalyst_v2": 1.3, "sector_rotation": 1.2},
    "TRENDING_DOWN": {"trend_continuation": 0.3, "relative_strength": 0.6, "oversold_reversal": 1.4},
    "UNKNOWN": {},
}
# Long-only equity: an overall caution penalty when the broad tape itself is falling.
REGIME_GLOBAL_PENALTY = {"TRENDING_UP": 1.0, "CHOPPY": 0.9, "TRENDING_DOWN": 0.65, "UNKNOWN": 0.85}


def weights_for_regime(regime):
    w = dict(BASE_WEIGHTS)
    for k, mult in REGIME_MULTIPLIERS.get(regime, {}).items():
        w[k] = w[k] * mult
    return w


def combine(agent_results, regime):
    """agent_results: {agent_name: (score_0_100, reason)}. Returns
    (composite_0_100, n_fired, reasons_dict)."""
    w = weights_for_regime(regime)
    total_w = sum(w.values())
    weighted_sum = sum(w[a] * s for a, (s, _) in agent_results.items() if a in w)
    n_fired = sum(1 for s, _ in agent_results.values() if s > 0)

    composite = weighted_sum / total_w if total_w else 0
    # Pring weight-of-evidence bonus for multi-agent confirmation
    if n_fired >= 3:
        composite *= 1.15
    elif n_fired >= 2:
        composite *= 1.05
    composite *= REGIME_GLOBAL_PENALTY.get(regime, 0.85)
    composite = min(composite, 100)

    reasons = {a: r for a, (s, r) in agent_results.items() if s > 0}
    return round(composite, 1), n_fired, reasons


def select_daily_top(all_scores_by_day, top_n=1):
    """all_scores_by_day: {date: [(symbol, composite, n_fired, reasons), ...]}.
    Returns {date: [top_n picks]} sorted by composite desc."""
    out = {}
    for date, rows in all_scores_by_day.items():
        ranked = sorted(rows, key=lambda x: -x[1])
        out[date] = ranked[:top_n]
    return out
