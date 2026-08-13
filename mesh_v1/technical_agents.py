"""
The 4 technical detector agents. Each takes a feature row (from
mesh_v1.features.build_stock_frame) and returns (score 0-100, reason string).
Thresholds mirror the validated archetypes from mover_pattern_classifier.py /
mover_categorization_final.csv (base_breakout, volume_thrust, ma_stack_rising,
pullback_in_uptrend, near_20d_high, hh_hl_uptrend, rs_beats_nifty,
strong_close, deep_oversold_bounce) — this is the OPERATIONALIZATION of the
categorization table, not a reimplementation of the old agents/ pipeline.
"""


def g(r, k):
    return r.get(k)


def extension_penalty(r):
    """Shannon/Pring risk principle: don't chase, buy fresh. Penalizes stocks
    already far above their 20-SMA (the day's most 'obvious' movers, likely
    already extended/consensus) and gives a small bonus to fresh ones near
    support. Returns a multiplier in [0.35, 1.15]. Fixes the router picking
    the most-extended 'everyone already sees it' name every day."""
    close, sma20 = r["close"], g(r, "sma20")
    if not sma20:
        return 1.0
    ext = (close - sma20) / sma20 * 100
    if ext <= 3:
        return 1.15   # fresh, still near support - the setup with room to run
    if ext <= 8:
        return 1.0
    if ext <= 15:
        return 0.7
    if ext <= 25:
        return 0.5
    return 0.35       # very extended - likely today's "obvious" late chase


def trend_continuation_agent(r):
    """Shannon: stacked rising EMAs + HH/HL structure. Coverage 46%+ in our table.
    Extension-penalized: reward the FRESH breakout near support, not the
    already-extended, most-obvious mover of the day."""
    ema10, ema20, ema50 = g(r, "ema10"), g(r, "ema20"), g(r, "ema50")
    ema20_5ago = g(r, "ema20_5ago")
    close, close10ago, low, lo5 = r["close"], g(r, "close_10ago"), r["low"], g(r, "lo5")
    if not all(x is not None for x in (ema10, ema20, ema50, ema20_5ago)):
        return 0, "insufficient history"
    stack = ema10 > ema20 > ema50
    rising = ema20 > ema20_5ago
    hh_hl = bool(close10ago and close > close10ago and lo5 and low > lo5)
    above_20sma = g(r, "sma20") and g(r, "sma20_5ago") and r["close"] > r["sma20"] and r["sma20"] > r["sma20_5ago"]
    score = 0
    reasons = []
    if stack and rising:
        score += 55; reasons.append("stacked rising EMAs")
    if hh_hl:
        score += 25; reasons.append("higher-high/higher-low")
    if above_20sma:
        score += 20; reasons.append("above rising 20-SMA")
    score = min(score, 100) * extension_penalty(r)
    return round(score, 1), (", ".join(reasons) or "no trend structure")


def volume_thrust_agent(r):
    """Pring: volume-confirmed breakout (base_breakout, volume_thrust_2x, near_20d_high)."""
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
    if prior_high20 and close >= 0.985 * prior_high20:
        score += 15; reasons.append("near 20d high")
    return min(score, 100), ", ".join(reasons) or "no volume signature"


def relative_strength_agent(r):
    """rs_beats_nifty + momentum_roc10 + strong_close. Extension-penalized -
    same chase-avoidance principle as trend_continuation_agent."""
    roc10, nif_roc10 = g(r, "roc10"), g(r, "nif_roc10")
    high, low, close, opn = r["high"], r["low"], r["close"], r["open"]
    score, reasons = 0, []
    if roc10 is not None and nif_roc10 is not None and roc10 > nif_roc10:
        score += 35; reasons.append("beating NIFTY (RS)")
    if roc10 is not None and roc10 > 5:
        score += 25; reasons.append("roc10>5%")
    rng = high - low
    if rng > 0 and (close - low) / rng >= 0.7:
        score += 25; reasons.append("strong close (top of range)")
    if close >= opn:
        score += 15; reasons.append("green candle")
    score = min(score, 100) * extension_penalty(r)
    return round(score, 1), (", ".join(reasons) or "no relative strength edge")


def oversold_reversal_agent(r):
    """Weinstein/Pring: deep pullback below 100-SMA then bounce attempt."""
    close, opn = r["close"], r["open"]
    sma100 = g(r, "sma100")
    if not sma100:
        return 0, "insufficient history"
    ext100 = (close - sma100) / sma100 * 100
    if ext100 > -15:
        return 0, "not deeply oversold"
    score, reasons = 40, [f"deeply oversold ({ext100:.0f}% below 100sma)"]
    if close > opn:
        score += 30; reasons.append("green reversal candle")
    va = g(r, "vol_avg20") or r["volume"]
    if va and r["volume"] >= 1.3 * va:
        score += 30; reasons.append("volume pickup on bounce")
    return min(score, 100), ", ".join(reasons)
