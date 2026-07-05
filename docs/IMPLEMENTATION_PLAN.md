# Implementation Plan — Path to 70% WR / ₹2L+ per ₹1L monthly (single NFO slot)

**Standing constraints (from Chandrakant, permanent):**
- Long NFO options only (CE + PE). NO selling/spreads — margin too heavy.
- Single NFO slot (one position at a time), ~₹1L working capital.
- Exit rule: hard stop -50%; trailing stop arms at 2x, 10% giveback, unlimited upside; 3-day max hold.
- Entry timing: same-day, at/near the ignition candle's close ("not before, not after").
- Target: ≥70% win rate AND ≥₹2L profit per ₹1L deployed monthly. Proof bar: May+June combined > ₹2L on single slot.
- Every strategy change gets a version here. Failed versions STAY in this file with the failure reason.

---

## Register of everything tried (as of 2026-07-05)

### Validated (in production)
| # | Idea | Result | Where |
|---|------|--------|-------|
| V1 | Same-day-close entry beats next-day-open | 78.9% vs 73.7% WR paired test, n=19 | conviction_router_agent.py |
| V2 | Full agent gauntlet must NOT be bypassed | raw pool 52.4% WR vs gauntlet 73.7% WR (proven twice) | graph/builder.py comment |
| V3 | Breadth Thrust regime widener (<45%→+15pts/3d) | n=181, PF 1.31, +7.1%/trade | agents/breadth_thrust_agent.py |
| V4 | Pring bearish divergence + vol≥1.5x, macro_regime==BEARISH only | n=49, WR 53.1%, PF 1.88, +13.9%; holds in BOTH May 10-31 (PF 2.34) and Jun 1-10 (PF 1.21) clusters | agents/pring_divergence_agent.py |
| V5 | New exit rule (2x-arm trail, unlimited upside) + 3-day max hold | 19 June trades: WR 52.6%, PF 1.71, +6.37%/trade | backtest_3pm_options_vs_equity.py |

### Failed / rejected (do NOT retry without new data)
| # | Idea | Why it failed |
|---|------|---------------|
| F1 | Equity-continuation route (74.4% claim) | Was a screener reimplementation artifact; real screener → 52.4% |
| F2 | Gauntlet bypass for more candidates | Made results worse both times tried |
| F3 | Credit spreads (70-83% WR, validated!) | User rejected: margin too heavy. Kept as reference only |
| F4 | Panic-dip detector | Best config PF 1.04 @ n=23 — not trustworthy |
| F5 | Quiet-compression 4th momentum pattern | May -11.5% exp / June +11.7% — regime-dependent curve fit |
| F6 | News catalyst keywords | 20% movers vs 15% control — statistically nothing |
| F7 | Same-day 10AM intraday breakout | All lift came from May (control had 0 fires); June lift 1.07x |
| F8 | News sentiment (LLM) as independent signal | +0.05% vs -0.11% 3-day fwd return — noise; news is priced same-day |
| F9 | Stage-4 breakdown mirror for PE | 51% WR / +14.2% only in May, n=51 — superseded by V4 (Pring divergence) |

### Data/infra bugs found & fixed (this session)
- 919 duplicate corrupted rows in daily_ohlcv (2026-05-07 mass event + 3-symbol collision) — deleted, ingestion hardened with EQ filter.
- NIFTY 50 ingestion silently dead since 2026-05-07 → macro_regime stale for ALL June backtests. Backfilled; INDEX_SYMBOLS added to ingestion.
- DB timezone UTC vs IST: every time::date label was one day behind the real trading day. Fixed via ALTER DATABASE (user approved).

---

## Version 1 (2026-07-05) — status: IN PROGRESS

**Newly discovered inputs driving this version:**

1. **AMBUJACEM Thursday miss (found 2026-07-05):** Thu Jul 2 had the full ignition signature (+2.41%, 2x vol, 2-day-high break, cr=0.75). Critic approved. The **fundamental-audit LLM vetoed it ("Grade C, AVOID")**, then approved the SAME stock next day. The one-day-late entry cost Friday's +3.37% underlying move. → The audit gate is non-deterministic day-over-day and is killing valid ignition entries.
2. **Real option data unlock:** currently-listed July/Aug/Sep expiry NFO contracts have full daily history (premium + OI + volume) back to their listing (~May 27 for July expiry). June is therefore backtestable with REAL premiums and REAL OI — no more Black-Scholes proxy for June. OI imbalance (CE-vs-PE OI skew, per stock, per day) becomes a testable feature.

**Steps:**
- [ ] S1. Quantify audit-veto damage: re-run June fast-backtest recording every critic-approved-but-audit-vetoed OPTIONS_IGNITION signal; simulate their option P&L. If the vetoed set performs ≥ the approved set, the audit gate loses veto power on the ignition route (demote to warning flag).
- [ ] S2. Real-premium June re-validation: re-price all June ignition trades with actual July-expiry contract prices (entry = signal-day close of the real option; exits per V5 rule on the real premium series). Compare to BS-proxy results — this is the honest baseline.
- [ ] S3. OI-imbalance feature: for each June signal day, compute stock-level CE/PE OI ratio + day-over-day OI change from the July-expiry chain. Test whether OI skew separates winners from losers on the ignition set (and on the wider critic-approved set).
- [ ] S4. Single-slot simulation: sequential 1-slot portfolio sim over May+June (May uses BS proxy where real data absent, flagged as such), ₹1L capital, real position sizing by lot. Report the honest total P&L vs the ₹2L bar.
- [ ] S5. Whatever S1-S4 reveal → tighten/loosen → wire into production agents. New findings become V1 outcomes; failures spawn Version 2 below.

**Outcome:** (pending)

---
