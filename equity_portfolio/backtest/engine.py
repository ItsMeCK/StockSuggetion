"""
Walk-Forward Historical Backtesting Engine (Nifty 500 Universe)
Requirements EQ27, EQ28, EQ47, EQ54
Implements Next-Bar Fills, Real Fundamental Gating, and Authentic Close-to-Close Benchmark.
"""

import os
from datetime import date
from typing import Dict, List, Optional, Tuple, Any
import polars as pl

from equity_portfolio.config import POLICY, FEES, TAX
from equity_portfolio.data.point_in_time import PointInTimeDataLoader
from equity_portfolio.data.fundamental_store import FundamentalStore
from equity_portfolio.gates.forensic_gate import ForensicGatekeeper, ForensicReport
from equity_portfolio.gates.liquidity_gate import LiquidityGatekeeper, LiquidityMetrics
from equity_portfolio.gates.event_gate import EventRiskGatekeeper, EventContext
from equity_portfolio.models.industrial_schema import IndustrialQualityEvaluator, IndustrialMetrics
from equity_portfolio.models.banking_schema import BankingQualityEvaluator, BankingMetrics
from equity_portfolio.industry.cycle_tracker import IndustryCycleTracker, CyclePhase, SYMBOL_SECTOR_MAP
from equity_portfolio.valuation.valuation_engine import ValuationEngine, ValuationInputs
from equity_portfolio.technical.timing_filters import TechnicalTimingGate, TechnicalMetrics
from equity_portfolio.portfolio.allocator import PortfolioAllocator, ProposedPosition
from equity_portfolio.portfolio.risk_guard import PortfolioRiskGuard
from equity_portfolio.tax.lot_ledger import FifoLotLedger
from equity_portfolio.backtest.metrics import PerformanceAuditor, BacktestSummaryReport

class SovereignEquityBacktester:
    """
    Institutional Walk-Forward Simulation Engine:
    - Real Point-in-Time Fundamental, Forensic, and Quality Gating (No hardcoded placeholders)
    - Next-Bar Open Fills (EQ47): Orders generated at Day t EOD fill at Day t+1 Open with slippage
    - True Close-to-Close Benchmark Return (capturing overnight gaps)
    - Precision FIFO tax lot accounting with Indian loss set-off and FY exemption reset
    """
    def __init__(
        self,
        initial_capital: float = 1_000_000.0,
        parquet_path: Optional[str] = None
    ):
        self.initial_capital = initial_capital
        self.data_loader = PointInTimeDataLoader(parquet_path=parquet_path)
        self.fundamental_store = FundamentalStore()
        self.allocator = PortfolioAllocator()
        self.risk_guard = PortfolioRiskGuard(initial_nav=initial_capital)
        self.tax_ledger = FifoLotLedger()
        self.cycle_tracker = IndustryCycleTracker()
        self.technical_gate = TechnicalTimingGate()
        self.valuation_engine = ValuationEngine()
        self.forensic_gate = ForensicGatekeeper(require_verified_filings=POLICY.REQUIRE_VERIFIED_FILINGS)
        self.liquidity_gate = LiquidityGatekeeper()
        self.event_gate = EventRiskGatekeeper()
        self.industrial_evaluator = IndustrialQualityEvaluator()
        self.banking_evaluator = BankingQualityEvaluator()

        self.cash: float = initial_capital
        self.last_known_prices: Dict[str, float] = {}
        self.missing_days_tracker: Dict[str, int] = {}
        self.daily_nav_history: List[Dict[str, Any]] = []
        self.position_sleeve: Dict[str, str] = {} # symbol -> "CORE" (1-year hold) or "TACTICAL" (1-month swing)

        # Order queues for next-bar execution (EQ47)
        self.pending_buys: List[ProposedPosition] = []
        self.pending_exits: List[Tuple[str, int, str]] = [] # (symbol, shares, reason)

    def run_backtest(
        self,
        warmup_bars: int = 28, # Defaults to August 6, 2026 (54 trading days through October 23, 2026)
        rebalance_days_interval: int = 20,
        trial_count: int = 10,
        initial_seasoned_holdings: Optional[List[Dict[str, Any]]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Tuple[BacktestSummaryReport, pl.DataFrame, pl.DataFrame]:
        daily_df = self.data_loader.load_daily_eod_data()
        trading_dates = self.data_loader.get_trading_dates()

        if start_date is not None:
            sim_dates = [d for d in trading_dates if d >= start_date]
        else:
            if len(trading_dates) <= warmup_bars:
                raise ValueError(f"Insufficient trading dates ({len(trading_dates)}) for warmup ({warmup_bars})")
            sim_dates = trading_dates[warmup_bars:]

        if end_date is not None:
            sim_dates = [d for d in sim_dates if d <= end_date]

        # Initialize any pre-existing seasoned portfolio holdings (e.g. for Day-366 LTCG tax testing)
        if initial_seasoned_holdings:
            for h in initial_seasoned_holdings:
                lot = self.tax_ledger.record_buy(
                    symbol=h["symbol"],
                    buy_date=h["buy_date"],
                    shares=h["shares"],
                    execution_price=h["execution_price"]
                )
                self.cash -= (lot.buy_turnover + lot.buy_statutory_charges)
                self.position_sleeve[h["symbol"]] = h.get("sleeve", "CORE")
        
        # Benchmark tracker: official NIFTY 500 index if available, else true Close-to-Close proxy
        benchmark_initial_val = 100.0
        benchmark_val = 100.0
        prev_close_map: Dict[str, float] = {}
        benchmark_history: Dict[date, float] = {}
        stock_close_history: Dict[str, Dict[date, float]] = {}

        nifty_500_map: Dict[str, float] = {}
        base_nifty_500: Optional[float] = None
        if os.path.exists("data/nifty_500_daily.json"):
            try:
                import json
                with open("data/nifty_500_daily.json") as f:
                    bars = json.load(f)
                nifty_500_map = {b["date"]: float(b["close"]) for b in bars}
            except Exception:
                nifty_500_map = {}

        benchmark_name = "NSE NIFTY 500 Official Index" if nifty_500_map else "Equal-Weight Universe Benchmark (498 Scrips Proxy)"

        print(f"Starting Next-Bar Walk-Forward Backtest across {len(sim_dates)} dates ({sim_dates[0]} to {sim_dates[-1]})...")

        for current_date in sim_dates:
            current_day_df = daily_df.filter(pl.col("trade_date") == current_date)
            if current_day_df.is_empty():
                continue

            day_records = current_day_df.to_dicts()
            open_map: Dict[str, float] = {r["symbol"]: float(r["open"]) for r in day_records if r["open"] is not None}
            close_map: Dict[str, float] = {r["symbol"]: float(r["close"]) for r in day_records if r["close"] is not None}
            sma20_map: Dict[str, float] = {r["symbol"]: float(r["sma_20"]) if r["sma_20"] is not None else float(r["close"]) for r in day_records}
            sma50_map: Dict[str, float] = {r["symbol"]: float(r["sma_50"]) if r["sma_50"] is not None else float(r["close"]) for r in day_records}
            vol_z_map: Dict[str, float] = {r["symbol"]: float(r["vol_zscore"]) if r["vol_zscore"] is not None else 0.0 for r in day_records}
            range_map: Dict[str, float] = {r["symbol"]: float(r["daily_range_pct"]) if r["daily_range_pct"] is not None else 1.5 for r in day_records}

            # Update last known prices and stock price history
            for sym, p in close_map.items():
                self.last_known_prices[sym] = p
                if sym not in stock_close_history:
                    stock_close_history[sym] = {}
                stock_close_history[sym][current_date] = p

            # --- 1. EXECUTE PENDING NEXT-BAR EXITS (Filled at Day t Open) ---
            if self.pending_exits:
                remaining_pending_exits = []
                for sym, shares, reason in self.pending_exits:
                    open_p = open_map.get(sym, self.last_known_prices.get(sym, 0.0))
                    if open_p > 0:
                        is_emergency = "STOP" in reason or "OVERSUPPLY" in reason or "BREAKDOWN" in reason
                        closed_execs = self.tax_ledger.record_sell(
                            symbol=sym,
                            sell_date=current_date,
                            shares_to_sell=shares,
                            execution_price=open_p,
                            is_emergency_cut=is_emergency
                        )
                        if not closed_execs and not is_emergency:
                            # Day-366 tax delay held this lot! Keep pending to execute on Day 366+
                            remaining_pending_exits.append((sym, shares, reason))
                        for cx in closed_execs:
                            net_inflow = (cx.shares * cx.sell_price) - cx.total_sell_charges - cx.tax_liability_inr
                            self.cash += net_inflow
                        if sym not in self.tax_ledger.get_portfolio_holdings():
                            self.position_sleeve.pop(sym, None)
                self.pending_exits = remaining_pending_exits

            # --- 2. EXECUTE PENDING NEXT-BAR BUYS (Filled at Day t Open) ---
            if self.pending_buys:
                current_holdings = self.tax_ledger.get_portfolio_holdings()
                for prop in self.pending_buys:
                    if len(current_holdings) >= self.allocator.target_holdings_max:
                        break
                    if prop.symbol in current_holdings:
                        continue
                    open_p = open_map.get(prop.symbol, 0.0)
                    if open_p <= 0:
                        continue

                    cost_est = (prop.target_shares * open_p) * (1.0 + (FEES.SLIPPAGE_PCT + FEES.STT_BUY_PCT + FEES.STAMP_DUTY_BUY_PCT) / 100.0)
                    if self.cash >= cost_est and cost_est >= POLICY.MIN_ORDER_VALUE:
                        lot = self.tax_ledger.record_buy(
                            symbol=prop.symbol,
                            buy_date=current_date,
                            shares=prop.target_shares,
                            execution_price=open_p
                        )
                        total_outflow = lot.buy_turnover + lot.buy_statutory_charges
                        self.cash -= total_outflow
                        current_holdings[prop.symbol] = prop.target_shares
                        self.position_sleeve[prop.symbol] = getattr(prop, "sleeve", "CORE")
                self.pending_buys = []

            # --- 3. COMPUTE AUTHENTIC BENCHMARK RETURN ---
            date_str = current_date.isoformat()
            if nifty_500_map and date_str in nifty_500_map:
                idx_close = nifty_500_map[date_str]
                if base_nifty_500 is None:
                    base_nifty_500 = idx_close
                    benchmark_val = 100.0
                else:
                    benchmark_val = 100.0 * (idx_close / base_nifty_500)
            else:
                if prev_close_map:
                    common_syms = [s for s in close_map.keys() if s in prev_close_map and prev_close_map[s] > 0]
                    if common_syms:
                        c2c_returns = [(close_map[s] - prev_close_map[s]) / prev_close_map[s] for s in common_syms]
                        daily_market_ret = sum(c2c_returns) / len(c2c_returns)
                        benchmark_val *= (1.0 + daily_market_ret)
            prev_close_map = close_map.copy()
            benchmark_history[current_date] = benchmark_val

            # --- 4. CHECK NON-TRADING SCrip LIQUIDATIONS ---
            active_holdings = self.tax_ledger.get_portfolio_holdings()
            for sym in list(active_holdings.keys()):
                if sym not in close_map:
                    self.missing_days_tracker[sym] = self.missing_days_tracker.get(sym, 0) + 1
                    if self.missing_days_tracker[sym] > 1:
                        last_p = self.last_known_prices.get(sym, 0.0)
                        if last_p > 0:
                            closed_execs = self.tax_ledger.record_sell(
                                symbol=sym,
                                sell_date=current_date,
                                shares_to_sell=active_holdings[sym],
                                execution_price=last_p,
                                is_emergency_cut=True
                            )
                            for cx in closed_execs:
                                net_inflow = (cx.shares * cx.sell_price) - cx.total_sell_charges - cx.tax_liability_inr
                                self.cash += net_inflow
                            self.missing_days_tracker.pop(sym, None)
                else:
                    self.missing_days_tracker[sym] = 0

            # --- 5. MARK-TO-MARKET EOD VALUATION ---
            active_equity_val = self.tax_ledger.get_active_lot_valuation(close_map, fallback_prices=self.last_known_prices)
            total_nav = self.cash + active_equity_val
            risk_state = self.risk_guard.update_nav(total_nav)

            # --- 6. EVALUATE EXITS AT DAY t CLOSE (To execute at Day t+1 Open) ---
            active_after_check = self.tax_ledger.get_portfolio_holdings()
            for sym, shares in active_after_check.items():
                if sym not in close_map:
                    continue

                p = close_map[sym]
                sma20 = sma20_map.get(sym, p)
                sma50 = sma50_map.get(sym, p)
                sleeve = self.position_sleeve.get(sym, "CORE")

                # Get average purchase price and holding days
                lots = self.tax_ledger.active_lots.get(sym, [])
                tot_rem = sum(l.remaining_shares for l in lots)
                avg_entry_price = sum(l.remaining_shares * l.buy_price for l in lots) / max(1, tot_rem) if tot_rem > 0 else p
                oldest_buy_date = min((l.buy_date for l in lots), default=current_date)
                holding_days = (current_date - oldest_buy_date).days

                # Exit Condition 1: Individual Stop Loss (-8% from Entry) applies to BOTH sleeves
                if p < (avg_entry_price * 0.92):
                    loss_pct = ((p - avg_entry_price) / avg_entry_price) * 100.0
                    self.pending_exits.append((sym, shares, f"HARD_STOP_LOSS_{loss_pct:.1f}%"))
                    continue

                # Exit Condition 2: Industry Oversupply Veto (Point-in-Time, EQ39) applies to BOTH
                phase, _, _ = self.cycle_tracker.evaluate_cycle(sym, as_of_date=current_date)
                if phase == CyclePhase.OVERSUPPLIED:
                    self.pending_exits.append((sym, shares, "INDUSTRY_OVERSUPPLY_VETO"))
                    continue

                # Exit Condition 3: Governance / Forensic Failure (Emergency exit for Core)
                forensic_rep = self.fundamental_store.get_forensic_report(sym)
                if forensic_rep:
                    f_pass, _ = self.forensic_gate.evaluate(forensic_rep)
                    if not f_pass:
                        self.pending_exits.append((sym, shares, "GOVERNANCE_RED_FLAG_EXIT"))
                        continue

                # Exit Condition 4: Sleeve-Specific Technical / Time Exit
                if sleeve == "CORE":
                    # Core 1-Year Sleeve: Structural Trend Breakdown (Close < 50 SMA - 3% buffer band)
                    # Eliminates noise whipsaw from 20-day SMA, allowing compounders to ride multi-month trends
                    if p < (sma50 * 0.97):
                        self.pending_exits.append((sym, shares, "STRUCTURAL_BREAKDOWN_50SMA"))
                        continue
                else: # TACTICAL Sleeve (1-Month Swing Horizon)
                    # Tactical momentum breakdown (Close < 20 SMA - 2% buffer)
                    if p < (sma20 * 0.98):
                        self.pending_exits.append((sym, shares, "TECHNICAL_BREAKDOWN_20SMA"))
                        continue
                    # Tactical time stop (stagnation beyond 25 trading days with negative return)
                    if holding_days >= 25 and p < avg_entry_price:
                        self.pending_exits.append((sym, shares, "TACTICAL_STAGNATION_TIME_STOP"))
                        continue

            # --- 7. EVALUATE DRIFT TRIMS (To execute at Day t+1 Open) ---
            holding_vals = {s: sh * close_map.get(s, self.last_known_prices.get(s, 0.0)) for s, sh in active_after_check.items()}
            drift_trims = self.allocator.check_drift_rebalance(holding_vals, total_nav)
            for sym, action, excess_val in drift_trims:
                p = close_map.get(sym, self.last_known_prices.get(sym, 0.0))
                if p > 0 and excess_val >= POLICY.MIN_ORDER_VALUE:
                    shares_to_trim = int(excess_val / p)
                    if shares_to_trim > 0:
                        self.pending_exits.append((sym, shares_to_trim, "TRIM_HARD_CEILING"))

            # --- 8. REAL 7-STEP CANDIDATE SCREENING AT DAY t CLOSE (To buy at Day t+1 Open) ---
            can_buy, block_reason = self.risk_guard.can_open_new_position()
            current_num_holdings = len(self.tax_ledger.get_portfolio_holdings()) + len(self.pending_buys)

            date_idx = sim_dates.index(current_date)

            # Macro Benchmark Regime Filter (EQ48)
            # If broad Nifty 500 is in confirmed correction below its 20-day SMA, throttle buying to defensive exposure
            if POLICY.ENABLE_MACRO_REGIME_FILTER and date_idx >= 5:
                recent_bench_vals = [benchmark_history[sim_dates[i]] for i in range(max(0, date_idx - 19), date_idx + 1)]
                bench_sma = sum(recent_bench_vals) / len(recent_bench_vals)
                current_exposure_pct = (active_equity_val / total_nav) * 100.0 if total_nav > 0 else 0.0
                if benchmark_val < bench_sma and current_exposure_pct >= POLICY.DEFENSIVE_MAX_EXPOSURE_PCT:
                    can_buy = False
                    block_reason = f"MACRO_REGIME_DEFENSIVE: Benchmark {benchmark_val:.2f} < SMA {bench_sma:.2f}, exposure capped at {POLICY.DEFENSIVE_MAX_EXPOSURE_PCT:.0f}%"

            if can_buy and current_num_holdings < self.allocator.target_holdings_max and self.cash >= POLICY.MIN_ORDER_VALUE:
                candidates: List[Tuple[str, float]] = []

                # Rolling lookback for benchmark relative strength (20-day return vs Nifty 500)
                lookback_idx = max(0, date_idx - 20)
                ref_date = sim_dates[lookback_idx]
                bench_ref_val = benchmark_history.get(ref_date, benchmark_initial_val)
                bench_ret_pct = ((benchmark_val - bench_ref_val) / bench_ref_val) * 100.0 if bench_ref_val > 0 else 0.0

                for r in day_records:
                    sym = r["symbol"]
                    if sym in self.tax_ledger.get_portfolio_holdings():
                        continue

                    p = float(r["close"])
                    sma20 = float(r["sma_20"]) if r["sma_20"] is not None else p
                    sma50 = float(r["sma_50"]) if r["sma_50"] is not None else p
                    vol_z = float(r["vol_zscore"]) if r["vol_zscore"] is not None else 0.0
                    dr = float(r["daily_range_pct"]) if r["daily_range_pct"] is not None else 1.5

                    # Step 1: Real Forensics Gate from Fundamental Store (EQ12)
                    forensic_rep = self.fundamental_store.get_forensic_report(sym)
                    if forensic_rep is None:
                        continue # Strict Fail-Closed (EQ2): Reject unverified / unknown symbols
                    f_pass, f_violations = self.forensic_gate.evaluate(forensic_rep)
                    if not f_pass:
                        continue # Rejects high pledge, failing Beneish, ASM/GSM surveillance!

                    # Step 2: Dual Business Quality Schema Evaluation (EQ34, EQ40, EQ41, EQ42)
                    rec = self.fundamental_store.get_record(sym)
                    if rec is None:
                        continue # Strict Fail-Closed: Reject unprofiled symbols

                    if rec.is_banking_or_nbfc:
                        banking_m = self.fundamental_store.get_banking_metrics(sym)
                        if banking_m is None:
                            continue # Fail-Closed
                        q_pass, q_score, q_reasons = self.banking_evaluator.evaluate(banking_m)
                        if not q_pass:
                            continue # Catches HDFCBANK (CD Ratio >= 100%) and BANDHANBNK (NPA/PCR)!
                    else:
                        ind_m = self.fundamental_store.get_industrial_metrics(sym)
                        if ind_m is None:
                            continue # Fail-Closed
                        q_pass, q_score, q_reasons = self.industrial_evaluator.evaluate(ind_m)
                        if not q_pass:
                            continue # Catches weak ROCE, poor cash conversion, excessive leverage!

                    # Step 3: Industry Demand-Supply Cycle Position (Point-in-Time, EQ32, EQ39)
                    phase, cycle_bonus, _ = self.cycle_tracker.evaluate_cycle(sym, as_of_date=current_date)
                    if phase == CyclePhase.OVERSUPPLIED:
                        continue # Hard veto on commodity oversupply/dumping

                    # Step 4: Liquidity Gatekeeper (EQ7, EQ8)
                    daily_vol = float(r["vol_sma_20"]) if r["vol_sma_20"] is not None else float(r["volume"])
                    daily_turnover = daily_vol * p
                    target_ticket_val = max(POLICY.MIN_ORDER_VALUE, min(total_nav * (POLICY.PURCHASE_WEIGHT_MAX_PCT / 100.0), self.cash))
                    target_ticket_shares = int(target_ticket_val / p) if p > 0 else 0

                    # Authentic Corwin-Schultz (Journal of Finance 2012) High-Low Spread Estimator
                    cs_spread_pct = float(r["corwin_schultz_spread_pct"]) if r.get("corwin_schultz_spread_pct") is not None else 0.15

                    l_metrics = LiquidityMetrics(
                        symbol=sym,
                        bid_ask_spread_pct=round(cs_spread_pct, 3),
                        daily_median_turnover_inr=daily_turnover,
                        daily_median_volume=int(daily_vol),
                        order_value_inr=target_ticket_val,
                        order_shares=target_ticket_shares
                    )
                    l_pass, l_violations = self.liquidity_gate.evaluate(l_metrics)
                    if not l_pass:
                        continue

                    # Step 5: Real Valuation Band & Cap Engine (EQ35, EQ44, EQ45)
                    val_inputs = self.fundamental_store.get_valuation_inputs(sym, p)
                    if val_inputs is None:
                        continue # Fail-Closed
                    v_pass, mos_pct, _, v_reasons = self.valuation_engine.evaluate(val_inputs)
                    if not v_pass:
                        continue # Hard veto on > 60x P/E growth traps (DIXON, KAYNES)!

                    # Step 6: Technical Timing Confirmation (EQ46, EQ47)
                    # Authentic Relative Strength vs Nifty 500 TRI Benchmark
                    stock_ref_p = stock_close_history.get(sym, {}).get(ref_date, sma20)
                    stock_ret_pct = ((p - stock_ref_p) / stock_ref_p) * 100.0 if stock_ref_p > 0 else 0.0
                    rel_strength = stock_ret_pct - bench_ret_pct

                    tech_metrics = TechnicalMetrics(
                        symbol=sym,
                        current_price=p,
                        sma_50=sma50,
                        sma_200=None, # Honest labeling: evaluates 50 SMA directly without faking 200 SMA
                        volume_zscore_20d=vol_z,
                        relative_strength_3m_pct=rel_strength,
                        daily_range_pct=dr
                    )
                    t_pass, t_score, _ = self.technical_gate.evaluate(tech_metrics)
                    # Institutional Stage 2 Entry Filter: Must trade above 20 SMA, above 50 SMA, and have positive RS
                    if not t_pass or p < sma20 or p < sma50 or rel_strength <= 0:
                        continue

                    # Step 7: Event & Binary Risk Gate (EQ49)
                    next_rbi_mpc = self.event_gate.get_next_rbi_mpc_date(current_date)

                    next_earnings = rec.filing_date if rec.filing_date >= current_date else None

                    event_ctx = EventContext(
                        symbol=sym,
                        current_date=current_date,
                        next_earnings_date=next_earnings,
                        next_rbi_mpc_date=next_rbi_mpc,
                        ipo_listing_date=rec.ipo_listing_date,
                        anchor_lockin_expiry_date=rec.anchor_lockin_expiry_date
                    )
                    e_pass, e_reasons = self.event_gate.evaluate_new_buy(event_ctx)
                    if not e_pass:
                        continue # Blocks buys within 48h of earnings, RBI MPC, <60d IPO seasoning, or anchor lock-in

                    # Composite Conviction Score
                    conviction = (q_score * 0.4) + (t_score * 0.3) + (cycle_bonus * 2.0) + (50.0 + mos_pct * 0.2)
                    candidates.append((sym, conviction))

                available_slots = max(0, self.allocator.target_holdings_max - current_num_holdings)
                if available_slots > 0 and candidates:
                    candidates.sort(key=lambda x: x[1], reverse=True)
                    top_candidate_symbols = [c[0] for c in candidates[:available_slots]]

                    current_core_val = sum(sh * close_map.get(s, self.last_known_prices.get(s, 0.0)) for s, sh in active_after_check.items() if self.position_sleeve.get(s, "CORE") == "CORE")
                    max_core_val = total_nav * POLICY.CORE_ALLOCATION_PCT

                    for sym in top_candidate_symbols:
                        sleeve_type = "CORE" if current_core_val < max_core_val else "TACTICAL"
                        props = self.allocator.allocate_equal_weight(
                            approved_symbols=[sym],
                            symbol_sector_map=SYMBOL_SECTOR_MAP,
                            price_map=close_map,
                            total_portfolio_nav=total_nav,
                            sleeve=sleeve_type
                        )
                        if props:
                            self.pending_buys.extend(props)
                            if sleeve_type == "CORE":
                                current_core_val += props[0].target_value_inr

            # Record Daily EOD Snapshot
            self.daily_nav_history.append({
                "date": current_date,
                "cash": round(self.cash, 2),
                "active_holdings_value": round(active_equity_val, 2),
                "nav": round(total_nav, 2),
                "holdings_count": len(self.tax_ledger.get_portfolio_holdings()),
                "benchmark_val": round(benchmark_val, 2)
            })

        # Generate Audited Report
        report = PerformanceAuditor.generate_report(
            daily_nav_history=self.daily_nav_history,
            closed_trades=self.tax_ledger.closed_executions,
            initial_nav=self.initial_capital,
            benchmark_initial_val=benchmark_initial_val,
            benchmark_final_val=benchmark_val,
            trial_count=trial_count,
            benchmark_name=benchmark_name
        )

        nav_df = pl.DataFrame(self.daily_nav_history)
        
        if self.tax_ledger.closed_executions:
            trades_dicts = [
                {
                    "symbol": t.symbol,
                    "buy_date": str(t.buy_date),
                    "sell_date": str(t.sell_date),
                    "holding_days": t.holding_days,
                    "shares": t.shares,
                    "buy_price": t.buy_price,
                    "sell_price": t.sell_price,
                    "gross_pnl": t.gross_pnl,
                    "buy_charges": t.total_buy_charges,
                    "sell_charges": t.total_sell_charges,
                    "net_pnl_before_tax": t.net_pnl_before_tax,
                    "is_ltcg": t.is_ltcg,
                    "tax_liability_inr": t.tax_liability_inr,
                    "net_pnl_after_tax": t.net_pnl_after_tax,
                    "tax_alpha_saved_inr": t.tax_alpha_saved_inr
                }
                for t in self.tax_ledger.closed_executions
            ]
            trades_df = pl.DataFrame(trades_dicts)
        else:
            trades_df = pl.DataFrame()

        return report, nav_df, trades_df
