"""
Audited Backtest Metrics & Performance Analytics
Requirements EQ27, EQ28, EQ54
"""

import math
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import polars as pl
import numpy as np
from scipy import stats

@dataclass
class BacktestSummaryReport:
    start_date: str
    end_date: str
    trading_days: int
    initial_nav: float
    final_nav: float
    gross_pnl: float
    net_pnl_after_tax: float
    total_statutory_charges: float
    total_tax_paid: float
    tax_alpha_saved: float
    strategy_return_pct: float
    benchmark_name: str
    benchmark_return_pct: float
    alpha_over_benchmark_pct: float
    average_invested_exposure_pct: float
    exposure_adjusted_benchmark_return_pct: float
    exposure_adjusted_alpha_pct: float
    cash_cushion_contribution_pct: float
    max_drawdown_pct: float
    max_drawdown_inr: float
    sharpe_ratio: float
    deflated_sharpe_ratio: float
    trial_count: int
    total_trades_closed: int
    win_rate_pct: float
    profit_factor: float
    avg_holding_days: float
    portfolio_turnover_pct: float

class PerformanceAuditor:
    """
    Computes institutional performance, risk, and tax analytics.
    Deflated Sharpe Ratio (Bailey & Lopez de Prado 2014) corrects for multiple testing.
    """
    @staticmethod
    def compute_deflated_sharpe(
        sharpe: float,
        num_returns: int,
        skewness: float = 0.0,
        kurtosis: float = 3.0,
        num_trials: int = 10
    ) -> float:
        """
        Computes Deflated Sharpe Ratio (DSR, Bailey & Lopez de Prado 2014) to penalize
        backtest selection bias and multiple testing across candidate trials.

        Note: When annualized Sharpe ratio is <= 0 (strategy incurred net loss),
        DSR is mathematically 0.0 (probability of true statistical alpha <= 0%).
        """
        if num_returns <= 2 or sharpe <= 0:
            return 0.0

        # Euler-Mascheroni constant
        gamma = 0.5772156649
        e_max = (1.0 - gamma) * stats.norm.ppf(1.0 - 1.0 / max(2, num_trials)) + gamma * stats.norm.ppf(1.0 - 1.0 / (max(2, num_trials) * math.e))
        variance_sharpe = (1.0 + 0.5 * (sharpe ** 2) - skewness * sharpe + ((kurtosis - 3.0) / 4.0) * (sharpe ** 2)) / max(1, num_returns - 1)
        
        std_sharpe = math.sqrt(max(1e-6, variance_sharpe))
        z = (sharpe - e_max) / std_sharpe
        dsr = float(stats.norm.cdf(z))
        return round(dsr, 4)

    @classmethod
    def generate_report(
        cls,
        daily_nav_history: List[Dict[str, Any]],
        closed_trades: List[Any],
        initial_nav: float,
        benchmark_initial_val: float,
        benchmark_final_val: float,
        trial_count: int = 10,
        benchmark_name: str = "Equal-Weight Universe Benchmark (498 Scrips Proxy)"
    ) -> BacktestSummaryReport:
        if not daily_nav_history:
            raise ValueError("daily_nav_history cannot be empty")

        df_nav = pl.DataFrame(daily_nav_history)
        start_date = str(df_nav["date"][0])
        end_date = str(df_nav["date"][-1])
        trading_days = len(df_nav)

        final_nav = float(df_nav["nav"][-1])
        gross_pnl = sum(t.gross_pnl for t in closed_trades) if closed_trades else 0.0
        total_charges = sum(t.total_buy_charges + t.total_sell_charges for t in closed_trades) if closed_trades else 0.0
        total_tax = sum(t.tax_liability_inr for t in closed_trades) if closed_trades else 0.0
        tax_alpha = sum(t.tax_alpha_saved_inr for t in closed_trades) if closed_trades else 0.0
        net_pnl = final_nav - initial_nav

        strategy_ret = ((final_nav - initial_nav) / initial_nav) * 100.0
        benchmark_ret = ((benchmark_final_val - benchmark_initial_val) / benchmark_initial_val) * 100.0 if benchmark_initial_val > 0 else 0.0
        alpha = strategy_ret - benchmark_ret

        nav_series = df_nav["nav"].to_numpy()

        # Exposure & Cash Cushion Analytics (Claude CFA Audit Requirement)
        active_vals = df_nav["active_holdings_value"].to_numpy()
        daily_exposures = (active_vals / nav_series) * 100.0
        avg_exposure_pct = float(np.mean(daily_exposures)) if len(daily_exposures) > 0 else 0.0

        # Exposure-Adjusted Benchmark Return = (Avg Exposure / 100) * Benchmark Return
        exp_adj_bench_ret = round((avg_exposure_pct / 100.0) * benchmark_ret, 2)

        # Exposure-Adjusted Alpha = Strategy Return - Exposure-Adjusted Benchmark Return
        exp_adj_alpha = round(strategy_ret - exp_adj_bench_ret, 2)

        # Cash cushion contribution to raw alpha
        cash_cushion = round(alpha - exp_adj_alpha, 2)

        # Drawdown calculation
        running_max = np.maximum.accumulate(nav_series)
        drawdown_inr = running_max - nav_series
        drawdown_pct = (drawdown_inr / running_max) * 100.0
        max_dd_pct = float(np.max(drawdown_pct)) if len(drawdown_pct) > 0 else 0.0
        max_dd_inr = float(np.max(drawdown_inr)) if len(drawdown_inr) > 0 else 0.0

        # Daily returns & Sharpe
        daily_returns = np.diff(nav_series) / nav_series[:-1]
        mean_ret = np.mean(daily_returns) if len(daily_returns) > 0 else 0.0
        std_ret = np.std(daily_returns, ddof=1) if len(daily_returns) > 1 else 1e-6
        annualized_sharpe = float((mean_ret / std_ret) * math.sqrt(252)) if std_ret > 0 else 0.0

        # Deflated Sharpe
        skew = float(stats.skew(daily_returns)) if len(daily_returns) > 2 else 0.0
        kurt = float(stats.kurtosis(daily_returns, fisher=False)) if len(daily_returns) > 2 else 3.0
        dsr = cls.compute_deflated_sharpe(annualized_sharpe, len(daily_returns), skew, kurt, num_trials=trial_count)

        # Trade analytics
        total_closed = len(closed_trades)
        winning_trades = [t for t in closed_trades if t.net_pnl_after_tax > 0]
        losing_trades = [t for t in closed_trades if t.net_pnl_after_tax < 0]
        win_rate = (len(winning_trades) / total_closed * 100.0) if total_closed > 0 else 0.0

        tot_win_inr = sum(t.net_pnl_after_tax for t in winning_trades)
        tot_loss_inr = abs(sum(t.net_pnl_after_tax for t in losing_trades))
        profit_factor = round(tot_win_inr / tot_loss_inr, 2) if tot_loss_inr > 0 else (99.0 if tot_win_inr > 0 else 0.0)

        avg_hold = float(np.mean([t.holding_days for t in closed_trades])) if closed_trades else 0.0
        tot_turnover = sum(t.shares * t.sell_price for t in closed_trades) if closed_trades else 0.0
        turnover_pct = round((tot_turnover / initial_nav) * 100.0, 1)

        return BacktestSummaryReport(
            start_date=start_date,
            end_date=end_date,
            trading_days=trading_days,
            initial_nav=round(initial_nav, 2),
            final_nav=round(final_nav, 2),
            gross_pnl=round(gross_pnl, 2),
            net_pnl_after_tax=round(net_pnl, 2),
            total_statutory_charges=round(total_charges, 2),
            total_tax_paid=round(total_tax, 2),
            tax_alpha_saved=round(tax_alpha, 2),
            strategy_return_pct=round(strategy_ret, 2),
            benchmark_name=benchmark_name,
            benchmark_return_pct=round(benchmark_ret, 2),
            alpha_over_benchmark_pct=round(alpha, 2),
            average_invested_exposure_pct=round(avg_exposure_pct, 1),
            exposure_adjusted_benchmark_return_pct=exp_adj_bench_ret,
            exposure_adjusted_alpha_pct=exp_adj_alpha,
            cash_cushion_contribution_pct=cash_cushion,
            max_drawdown_pct=round(max_dd_pct, 2),
            max_drawdown_inr=round(max_dd_inr, 2),
            sharpe_ratio=round(annualized_sharpe, 2),
            deflated_sharpe_ratio=dsr,
            trial_count=trial_count,
            total_trades_closed=total_closed,
            win_rate_pct=round(win_rate, 1),
            profit_factor=profit_factor,
            avg_holding_days=round(avg_hold, 1),
            portfolio_turnover_pct=turnover_pct
        )
