"""
FIFO Tax Lot Ledger & Day-366 LTCG Optimizer
Requirements EQ9, EQ26
"""

import uuid
from dataclasses import dataclass, field
from datetime import date
from typing import List, Dict, Optional, Tuple
from equity_portfolio.config import FEES, TAX, POLICY

@dataclass
class TaxLot:
    lot_id: str
    symbol: str
    buy_date: date
    shares: int
    buy_price: float
    buy_turnover: float
    buy_statutory_charges: float
    remaining_shares: int

@dataclass
class ClosedLotExecution:
    symbol: str
    buy_date: date
    sell_date: date
    holding_days: int
    shares: int
    buy_price: float
    sell_price: float
    gross_pnl: float
    total_buy_charges: float
    total_sell_charges: float
    net_pnl_before_tax: float
    is_ltcg: bool
    tax_rate_pct: float
    tax_liability_inr: float
    net_pnl_after_tax: float
    tax_alpha_saved_inr: float # Alpha saved if LTCG vs STCG

class FifoLotLedger:
    """
    Precision FIFO tax lot tracker with Day-366 LTCG optimization.
    Computes exact Indian statutory charges (STT, Stamp, NSE, SEBI, GST, CDSL DP)
    and post-2024 capital gains taxes (20% STCG vs 12.5% LTCG).
    """
    def __init__(
        self,
        stt_rate: float = FEES.STT_BUY_PCT / 100.0,
        stamp_duty_rate: float = FEES.STAMP_DUTY_BUY_PCT / 100.0,
        nse_charge_rate: float = FEES.NSE_TURNOVER_PCT / 100.0,
        sebi_rate: float = FEES.SEBI_FEE_PCT / 100.0,
        gst_rate: float = FEES.GST_PCT / 100.0,
        cdsl_dp_charge: float = FEES.CDSL_DP_CHARGE,
        slippage_rate: float = FEES.SLIPPAGE_PCT / 100.0,
        stcg_rate: float = TAX.STCG_RATE,
        ltcg_rate: float = TAX.LTCG_RATE,
        ltcg_exemption_annual: float = TAX.LTCG_EXEMPTION,
        brokerage_per_order: float = FEES.BROKERAGE_PER_ORDER_INR
    ):
        self.stt_rate = stt_rate
        self.stamp_duty_rate = stamp_duty_rate
        self.nse_charge_rate = nse_charge_rate
        self.sebi_rate = sebi_rate
        self.gst_rate = gst_rate
        self.cdsl_dp_charge = cdsl_dp_charge
        self.slippage_rate = slippage_rate
        self.stcg_rate = stcg_rate
        self.ltcg_rate = ltcg_rate
        self.ltcg_exemption_annual = ltcg_exemption_annual
        self.brokerage_per_order = brokerage_per_order
        
        self.active_lots: Dict[str, List[TaxLot]] = {} # symbol -> list of active TaxLot (FIFO ordered)
        self.closed_executions: List[ClosedLotExecution] = []
        self.cumulative_ltcg_gain_fy: float = 0.0
        self.cumulative_stcg_gain_fy: float = 0.0
        self.current_fy: Optional[int] = None

    def _check_and_reset_fy(self, current_date: date):
        """Resets annual LTCG exemption (Rs 1.25L) and capital loss ledgers on April 1."""
        fy = current_date.year if current_date.month >= 4 else current_date.year - 1
        if self.current_fy != fy:
            self.current_fy = fy
            self.cumulative_ltcg_gain_fy = 0.0
            self.cumulative_stcg_gain_fy = 0.0

    def calculate_buy_charges(self, turnover: float) -> float:
        stt = turnover * self.stt_rate
        stamp = turnover * self.stamp_duty_rate
        nse = turnover * self.nse_charge_rate
        sebi = turnover * self.sebi_rate
        brokerage = self.brokerage_per_order
        gst = (nse + sebi + brokerage) * self.gst_rate
        return round(stt + stamp + nse + sebi + brokerage + gst, 2)

    def calculate_sell_charges(self, turnover: float) -> float:
        stt = turnover * self.stt_rate
        nse = turnover * self.nse_charge_rate
        sebi = turnover * self.sebi_rate
        brokerage = self.brokerage_per_order
        gst = (nse + sebi + brokerage) * self.gst_rate
        # Flat CDSL DP charge applies to every scrip sale
        return round(stt + nse + sebi + brokerage + gst + self.cdsl_dp_charge, 2)

    def record_buy(self, symbol: str, buy_date: date, shares: int, execution_price: float) -> TaxLot:
        effective_price = execution_price * (1.0 + self.slippage_rate) # Slipped entry
        turnover = round(shares * effective_price, 2)
        charges = self.calculate_buy_charges(turnover)

        lot = TaxLot(
            lot_id=str(uuid.uuid4())[:8],
            symbol=symbol,
            buy_date=buy_date,
            shares=shares,
            buy_price=round(effective_price, 2),
            buy_turnover=turnover,
            buy_statutory_charges=charges,
            remaining_shares=shares
        )
        if symbol not in self.active_lots:
            self.active_lots[symbol] = []
        self.active_lots[symbol].append(lot)
        return lot

    def should_delay_for_day_366(self, lot: TaxLot, current_date: date, is_emergency_cut: bool = False) -> bool:
        """
        The Day-366 Rule (EQ9):
        If non-emergency sell is requested between Day 350 and Day 364,
        defer execution to Day 366 to transition from 20% STCG to 12.5% LTCG.
        """
        if is_emergency_cut:
            return False # Emergency risk/fraud cuts never wait

        holding_days = (current_date - lot.buy_date).days
        if 350 <= holding_days <= 364:
            return True # Hold for Day 366
        return False

    def record_sell(
        self,
        symbol: str,
        sell_date: date,
        shares_to_sell: int,
        execution_price: float,
        is_emergency_cut: bool = False
    ) -> List[ClosedLotExecution]:
        self._check_and_reset_fy(sell_date)
        effective_price = execution_price * (1.0 - self.slippage_rate) # Slipped exit
        executions: List[ClosedLotExecution] = []
        if symbol not in self.active_lots or not self.active_lots[symbol]:
            return executions

        remaining_needed = shares_to_sell
        lots = self.active_lots[symbol]

        while remaining_needed > 0 and lots:
            current_lot = lots[0]

            # Check Day-366 delay condition
            if self.should_delay_for_day_366(current_lot, sell_date, is_emergency_cut):
                break

            matched_shares = min(remaining_needed, current_lot.remaining_shares)
            holding_days = (sell_date - current_lot.buy_date).days

            buy_portion_turnover = matched_shares * current_lot.buy_price
            buy_portion_charges = (matched_shares / current_lot.shares) * current_lot.buy_statutory_charges
            sell_turnover = round(matched_shares * effective_price, 2)
            sell_charges = self.calculate_sell_charges(sell_turnover)

            gross_pnl = sell_turnover - buy_portion_turnover
            net_before_tax = gross_pnl - buy_portion_charges - sell_charges

            # Tax Calculation: Post-2024 Budget Rates with Loss Set-Off
            is_ltcg = holding_days > 365
            tax_rate = self.ltcg_rate if is_ltcg else self.stcg_rate
            tax_liability = 0.0
            tax_alpha_saved = 0.0

            if is_ltcg:
                if net_before_tax > 0:
                    # Apply annual Rs 1.25L exemption (resets each FY)
                    taxable_ltcg = max(0.0, net_before_tax - max(0.0, self.ltcg_exemption_annual - self.cumulative_ltcg_gain_fy))
                    self.cumulative_ltcg_gain_fy += net_before_tax
                    tax_liability = round(taxable_ltcg * self.ltcg_rate, 2)
                    potential_stcg_tax = round(net_before_tax * self.stcg_rate, 2)
                    tax_alpha_saved = round(potential_stcg_tax - tax_liability, 2)
                else:
                    self.cumulative_ltcg_gain_fy += net_before_tax
            else:
                # Short-Term Capital Gains with Loss Set-Off (Section 70)
                if net_before_tax < 0:
                    self.cumulative_stcg_gain_fy += net_before_tax # Accumulate loss shield
                    tax_liability = 0.0
                else:
                    if self.cumulative_stcg_gain_fy < 0:
                        net_taxable_stcg = max(0.0, net_before_tax + self.cumulative_stcg_gain_fy)
                        self.cumulative_stcg_gain_fy += net_before_tax
                    else:
                        net_taxable_stcg = net_before_tax
                        self.cumulative_stcg_gain_fy += net_before_tax
                    tax_liability = round(net_taxable_stcg * self.stcg_rate, 2)

            net_pnl_after_tax = round(net_before_tax - tax_liability, 2)

            exec_record = ClosedLotExecution(
                symbol=symbol,
                buy_date=current_lot.buy_date,
                sell_date=sell_date,
                holding_days=holding_days,
                shares=matched_shares,
                buy_price=current_lot.buy_price,
                sell_price=round(effective_price, 2),
                gross_pnl=round(gross_pnl, 2),
                total_buy_charges=round(buy_portion_charges, 2),
                total_sell_charges=round(sell_charges, 2),
                net_pnl_before_tax=round(net_before_tax, 2),
                is_ltcg=is_ltcg,
                tax_rate_pct=round(tax_rate * 100.0, 1),
                tax_liability_inr=tax_liability,
                net_pnl_after_tax=net_pnl_after_tax,
                tax_alpha_saved_inr=tax_alpha_saved
            )
            executions.append(exec_record)
            self.closed_executions.append(exec_record)

            current_lot.remaining_shares -= matched_shares
            remaining_needed -= matched_shares

            if current_lot.remaining_shares == 0:
                lots.pop(0)

        return executions

    def get_portfolio_holdings(self) -> Dict[str, int]:
        """Returns map of symbol -> total remaining active shares."""
        holdings = {}
        for sym, lot_list in self.active_lots.items():
            tot_shares = sum(l.remaining_shares for l in lot_list)
            if tot_shares > 0:
                holdings[sym] = tot_shares
        return holdings

    def get_active_lot_valuation(
        self,
        current_prices: Dict[str, float],
        fallback_prices: Optional[Dict[str, float]] = None
    ) -> float:
        fallback_prices = fallback_prices or {}
        total_val = 0.0
        for sym, lot_list in self.active_lots.items():
            p = current_prices.get(sym)
            if p is None or p <= 0:
                p = fallback_prices.get(sym, 0.0)
            for l in lot_list:
                total_val += l.remaining_shares * p
        return round(total_val, 2)
