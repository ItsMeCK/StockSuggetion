# Institutional Study: Why This System Is Not Reaching a Stable High Win Rate

*A diagnosis written before making further changes. Every claim is grounded in
this system's own backtest data (see scripts/institutional_forensics.py output,
2026-07-05), not theory alone.*

---

## Executive summary

The system does not have a "tuning" problem. It has **four structural problems**,
and three of them mean the *target itself* ("70% win rate on options") is
mis-specified for the instrument being traded. The honest findings:

1. **We cannot actually evaluate the options strategy at all** — every options
   number is a Black-Scholes *proxy* off the underlying's daily close, with no
   real option premium, bid-ask, IV surface, or liquidity. This is fiction-grade
   data for options.
2. **70% win rate is the wrong objective for long call options.** Long options
   are a convex, positively-skewed instrument. Our own data shows the correct
   signature of one: 63.6% WR *but* avg win +99% vs avg loss −48%, profit factor
   3.63, expectancy +45.6%/trade. Forcing a higher win rate means cutting the fat
   right tail that is the *only* reason long options make money.
3. **Our entry timing is structurally wrong for long premium.** 73% of our option
   entries happen when volatility is already *expanding* (the blow-off), only 27%
   in the *coil*. Long options want to be bought in the coil (cheap vol, before
   expansion) so both delta and vega pay you.
4. **The strategy is mostly beta, not alpha.** June (breadth 30%→78%, raging bull)
   → 70% WR. May (choppy/corrective) → 42% WR. The "edge" is largely long
   exposure to a rising tape.

---

## Finding 1 — The options backtest is not measuring options

Everything we've reported (73.7%, 78.9%, 70.6%, 41.9%) is computed from
`bs_call_price()` using a *static* implied vol estimated as realized-vol × 1.15,
priced off the daily close of the *underlying*. A real institutional desk would
reject this outright, because it omits every dominant driver of actual option
P&L:

- **IV dynamics / vega & IV-crush.** We hold sigma constant for the whole trade.
  In reality IV expands into a breakout and mean-reverts after — so even a call
  whose underlying keeps rising can *lose* as IV bleeds out. Our model is blind
  to this.
- **Bid-ask spread.** NSE *single-stock* options — especially the mid-cap
  momentum names our screener favours (CARTRADE, AEGISLOG, IFCI) — routinely
  carry 5–15% spreads. We enter at ask, exit at bid. A 10–20% round-trip friction
  is not modeled at all; we transact at theoretical mid.
- **Liquidity / fills.** Thin OI in weekly/monthly contracts means the
  theoretical fill is unattainable in size.
- **Expiry structure.** We assume a flat 30-day expiry decaying daily. Real
  entries land at specific distances from monthly/weekly expiry; theta near
  expiry is brutal and completely changes the trade.

**Consequence:** the +45.6% expectancy could survive real frictions, or could be
entirely eaten by them. *We do not currently have the data to know.* This is
Priority #1 to fix, and nothing about the options book should be trusted or
deployed until it is.

---

## Finding 2 — For long calls, win rate is the wrong metric (the CFA point)

Long options are **convex and positively skewed by construction**. The correct
way to run a long-premium book is: *many small losses, a few enormous winners.*
Our own closed-trade data is a textbook example:

| Metric | Value |
|---|---|
| Win rate | 63.6% |
| Avg win | **+99.0%** |
| Avg loss | −47.8% |
| Win/loss magnitude | 2.07× |
| Profit factor | **3.63** |
| Expectancy / trade | **+45.6%** |
| Biggest winner / loser | +332% / −62% |

A win rate of 40–50% with this payoff shape is *excellent* and highly profitable.
Chasing 70% forces early profit-taking (a 1.5× target beats a 2.0× target on WR
but our data shows 2.0× has ~3× the average P&L) — i.e. **you'd be capping the
fat tail, which is the entire source of edge.**

The instrument that naturally produces a 70%+ win rate is the **opposite
structure**: *short* premium / defined-risk credit spreads (short puts, put
credit spreads, bull call spreads). Those win often-and-small, lose rarely-and-
big. **You cannot have both a high win rate and the convex upside from the same
long-call structure.** The objective must be split by instrument (see roadmap).

---

## Finding 3 — We buy the blow-off, not the coil

Signal-day volatility state of our 67 entries:

- **73% entered while ATR-3 > ATR-20** — volatility already *expanding*, the
  worst possible entry for long premium (you're buying elevated IV relative to
  the name's own baseline).
- Only **27% entered in a contracted/coil state** — the regime where long
  options are cheap and a subsequent expansion pays you on delta *and* vega.

Our signal is "already broke the 2-day high on volume" — by definition it fires
*after* the initial pop, when IV has expanded and part of the move is spent.
For **equity** (no vega) buying the breakout is fine. For **options** it is
backwards: the edge is the *volatility vacuum* (Bollinger/ATR squeeze, low IV
percentile) *before* the break. The system already gestured at this ("Stealth
Accumulation", the scratch `volatility_vacuum` files) but the live options path
does the opposite.

---

## Finding 4 — The equity exit rule is theater

65 of 67 equity exits were `TIME_STOP_2D`. The advertised +10% target / −5% stop
**almost never triggers in a 2-day window** — we are really running "hold exactly
2 days and take whatever" (mean +0.83%, stdev 2.46%). Two consequences:

- The 10/5 rule is decorative; real behaviour is a 2-day hold.
- 2 days is arbitrary and far too short. Momentum autocorrelation (Jegadeesh–
  Titman) operates over *weeks*. We systematically exit winners early. A
  trailing stop (10-EMA / chandelier-ATR) would capture the persistence the
  fixed 2-day stop throws away — and equity is exactly where a high, *honest*
  win rate is reachable because there's no theta/vega fighting you.

---

## Finding 5 — Beta, not alpha; and the sample is too small to tell

- **Regime dependence:** June (rising breadth) 70% WR vs May (corrective) 42%.
  The strategy largely expresses long exposure to a rising market. A breadth
  filter doesn't *create* alpha — it *times beta*, which is itself hard and
  semi-circular.
- **Sample size:** ~40 trading days, 15–30 option trades per window.
  Distinguishing a true 55% from a true 70% win rate needs ~100+ trades. Every
  "breakthrough" that died out-of-sample this session (equity-continuation 74%→52%,
  RS-rank lab 74%→real-screener 52%, same-day-entry June 70%→May 42%) is the
  direct symptom of fitting noise. An institutional backtest demands **3–5 years**
  spanning bull/bear/sideways/crash.

---

## What an institutional desk would actually do (the roadmap)

**A. Get real options data (Priority 1, non-negotiable).** Pull strike-level
historical option OHLC + OI from Kite (`historical_data` on option instrument
tokens). Rebuild the option simulator on real premiums with modeled bid-ask.
Until this exists, treat all options numbers as unverified.

**B. Split the objective by instrument — stop asking one structure for both:**
- **Equity book →** pursue the honest high win rate (target 60–70%): momentum
  persistence with a trailing exit, not a 2-day time stop. This is where a high
  win rate is real.
- **Options book →** treat as a convex, *low-win-rate / high-expectancy* vehicle.
  Measure by **profit factor & expectancy, not win rate.** If the user genuinely
  wants a 70% *win rate* on the derivatives book, that requires **defined-risk
  credit spreads / short premium**, not long calls.

**C. Fix the options entry thesis:** buy *volatility contraction* (ATR-3 < ATR-20,
Bollinger bandwidth at multi-month low, low IV percentile) *before* the breakout,
so you're long cheap vol. Reverses Finding 3.

**D. Fix equity exits:** replace the 2-day time stop with a trailing/chandelier
stop to capture multi-week momentum. Reverses Finding 4.

**E. Position sizing:** with a convex payoff, sizing dominates win rate. The
fractional-Kelly machinery that was stripped out matters more than the win-rate
number.

**F. Reset expectations honestly:** a momentum desk running long single-stock
options in India would be *thrilled* with 45% WR at profit factor 1.8+. The "70%
win rate" goal belongs to the **equity / credit-spread** book, not the long-call
book.

---

## Bottom line

The question "how do we get long calls to 70% win rate" has no honest answer —
it fights the instrument. The right questions are:
1. Can we get **real options data** so any options number means something?
2. On **equity**, can a trailing-momentum exit reach a genuine 65–70% win rate?
3. On **derivatives**, do we want **expectancy** (long vol on the coil) or
   **win rate** (short-premium credit spreads)? — pick the structure that matches
   the metric, don't force one structure to produce the other's signature.
