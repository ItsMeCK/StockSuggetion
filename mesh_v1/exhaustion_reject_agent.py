"""
Pring Chapter 15 - Key Reversal, Exhaustion, and Pinocchio Bars - implemented
as a HARD REJECT (veto), not an additive score. This is the missing piece:
every other agent in this mesh scores "evidence FOR" a move; none of them can
say "no, reject this" the way Pring's false-signal detection is designed to.
That gap is exactly why precision was only 5.5% despite 79.8% recall.

KEY REVERSAL (bar-level exhaustion at a top): gaps up strongly in the
direction of the trend, trades in an unusually WIDE range, but closes back
near/below the PREVIOUS close - the whole gap got erased intraday. "If good
news cannot push prices higher, what will?" (Pring, p.271)

EXHAUSTION BAR (more extreme): opens with an even larger gap, opens in the
UPPER half of its own range (starts strong), but closes in the LOWER half
AND below the open (reverses hard within the same bar).

PINOCCHIO BAR (false breakout): the HIGH temporarily clears a resistance
level (prior 20-day high) intraday, but the CLOSE fails to hold above it -
"the bigger the nose, the bigger the lie." This is the single most
actionable, mechanical false-breakout check Pring gives.

Any ONE of these firing on the signal day = REJECT the candidate outright,
regardless of how many other agents scored it positively.
"""


def g(r, k):
    return r.get(k)


def exhaustion_reject(r):
    """Returns (rejected: bool, reasons: dict) - a HARD VETO, not a score."""
    o, h, l, c = r["open"], r["high"], r["low"], r["close"]
    rng = h - l
    atr = g(r, "atr14") or rng
    prior_high20 = g(r, "prior_high20")
    gap_pct = g(r, "gap_pct") or 0
    prev_close = g(r, "prev_close")

    wide_bar = bool(atr and rng >= 1.8 * atr)

    # KEY REVERSAL: strong gap up + unusually wide range + closed back near/below
    # yesterday's close despite the gap - the gap got fully erased intraday.
    key_reversal = bool(wide_bar and gap_pct > 1.5 and prev_close and c <= prev_close * 1.005)

    # EXHAUSTION BAR: bigger gap, opened in the UPPER half (strong start), closed
    # in the LOWER half AND below the open (hard intraday reversal).
    open_in_upper_half = rng > 0 and (o - l) / rng >= 0.6
    close_in_lower_half_below_open = rng > 0 and (c - l) / rng <= 0.4 and c < o
    exhaustion = bool(wide_bar and gap_pct > 2.0 and open_in_upper_half and close_in_lower_half_below_open)

    # PINOCCHIO: high broke above the prior 20-day high, but close failed to
    # hold above it. FIXED (v2): the naive version (any tiny poke+fail) fired
    # MORE on real movers (67.3%) than non-movers (53.2%) - it was just
    # detecting routine daily noise around a resistance test, not Pring's
    # actual signature. Real Pinocchios require a MEANINGFUL excursion (not a
    # 0.1% poke) and a SUBSTANTIAL giveback (closes back most of the way down,
    # not just barely under the line) - same "wide bar" qualifier as the other two.
    excursion = (h - prior_high20) if prior_high20 else 0
    giveback_frac = (h - c) / excursion if excursion > 0 else 0
    meaningful_excursion = bool(atr and excursion >= 0.3 * atr)
    pinocchio = bool(prior_high20 and h > prior_high20 and c <= prior_high20
                     and meaningful_excursion and giveback_frac >= 0.7 and wide_bar)

    rejected = key_reversal or exhaustion or pinocchio
    return rejected, {"key_reversal": key_reversal, "exhaustion": exhaustion, "pinocchio": pinocchio}
