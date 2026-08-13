"""
V7 quality gate - built directly from the loss autopsy (mesh_v1/loser_autopsy.py),
not from book theory. Three concrete, data-backed fixes:

1. near_20d_high was REWARDED (+15pts) in volume_thrust_agent but the autopsy
   showed it's the single biggest loss driver (44.5% WR vs 66.4% WR when NOT
   near the 20d high, on the same mesh/day/regime). Invert it to a penalty.
2. Hard reject on excessive volume ratio (>4x = climax/blow-off, not healthy
   accumulation - losers averaged 3.49x vs winners' 2.70x) and excessive bar
   width vs ATR (>=2.2x = already an exhaustion-scale bar; losers averaged
   1.80x vs winners' 1.51x).
3. sector_rotation contributing was NEGATIVE alpha (44.3% WR vs 51.3% pop
   avg) - downweight sharply rather than counting it as confirming evidence.
"""


def g(r, k):
    return r.get(k)


def near_high_penalty(r):
    """Replaces volume_thrust_agent's old +15 'near 20d high' bonus. Returns a
    multiplier: buying right at/near resistance is the trap the autopsy found,
    so penalize proximity to the prior 20d high instead of rewarding it."""
    close = r["close"]
    prior_high20 = g(r, "prior_high20")
    if not prior_high20:
        return 1.0
    prox = close / prior_high20  # 1.0 = sitting right at the old high
    if prox >= 0.98:
        return 0.55   # right at/above resistance - the losing setup
    if prox >= 0.93:
        return 0.85
    return 1.1        # genuine room to run - the winning setup


def volume_thrust_agent_v7(r):
    """Same as technical_agents.volume_thrust_agent but with near_20d_high
    flipped from a bonus to a penalty (see near_high_penalty)."""
    close, vol = r["close"], r["volume"]
    va = g(r, "vol_avg20") or vol
    hi10, lo10 = g(r, "hi10"), g(r, "lo10")
    prior_high20 = g(r, "prior_high20")
    score, reasons = 0, []
    if va and vol >= 2.0 * va:
        score += 45; reasons.append("2x+ volume thrust")
    elif va and vol >= 1.5 * va:
        score += 25; reasons.append("1.5x volume")
    tight = hi10 and lo10 and lo10 > 0 and (hi10 / lo10 - 1) < 0.10
    if prior_high20 and close > prior_high20 and tight and va and vol >= 1.5 * va:
        score += 40; reasons.append("decisive base breakout (Pring)")
    score = min(score, 100) * near_high_penalty(r)
    if near_high_penalty(r) < 1.0:
        reasons.append("PENALTY: near/at prior 20d high (resistance)")
    return round(score, 1), (", ".join(reasons) or "no volume signature")


def hard_reject_v7(r):
    """Hard reject: excessive volume ratio (climax, not accumulation) or
    excessive bar width vs ATR (already an exhaustion-scale bar)."""
    va = g(r, "vol_avg20")
    vol_ratio = r["volume"] / va if va else None
    atr = g(r, "atr14")
    rng = r["high"] - r["low"]
    bar_width_atr = rng / atr if atr else None
    reasons = []
    if vol_ratio is not None and vol_ratio > 4.0:
        reasons.append(f"climax volume ({vol_ratio:.1f}x)")
    if bar_width_atr is not None and bar_width_atr >= 2.2:
        reasons.append(f"exhaustion-scale bar ({bar_width_atr:.1f}x ATR)")
    return bool(reasons), reasons
