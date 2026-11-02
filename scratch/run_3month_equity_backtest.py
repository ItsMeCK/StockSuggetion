"""
Script to execute the Sovereign Equity Delivery Walk-Forward Backtest
Fixed Period: August 6, 2026 to October 23, 2026 (54 Trading Days).
Benchmark: Official NSE NIFTY 500 Index.
Initial Capital: Rs 10,00,000 (100% Clean Cash, Zero Injected Holdings).
Deducts all statutory charges (STT, Stamp, NSE, SEBI, GST, CDSL DP), slippage,
and post-2024 capital gains taxes (20% STCG vs 12.5% LTCG).
"""

import json
import os
from datetime import date
import polars as pl
from equity_portfolio.backtest.engine import SovereignEquityBacktester

def main():
    print("=" * 80)
    print("SOVEREIGN EQUITY DELIVERY ENGINE: 54-DAY WALK-FORWARD HISTORICAL BACKTEST")
    print("FIXED WINDOW: August 6, 2026 to October 23, 2026 (54 Trading Days)")
    print("INITIAL CAPITAL: Rs 10,00,000.00 Clean Cash (No Injected Lots)")
    print("BENCHMARK: Official NSE NIFTY 500 Index")
    print("=" * 80)

    backtester = SovereignEquityBacktester(
        initial_capital=1_000_000.0, # Rs 10,00,000 Initial Equity
        parquet_path="data/equity_universe_daily.parquet"
    )

    # 100% Clean Cash: No synthetic seasoned holdings injected
    report, nav_df, trades_df = backtester.run_backtest(
        warmup_bars=28, # Index 28 corresponds to 2026-08-06
        rebalance_days_interval=20,
        trial_count=10,
        initial_seasoned_holdings=None,
        start_date=date(2026, 8, 6),
        end_date=date(2026, 10, 23)
    )

    # Export daily NAV history
    nav_csv_path = "data/equity_backtest_daily_nav.csv"
    nav_df.write_csv(nav_csv_path)
    print(f"\n[OK] Saved Daily NAV history to {nav_csv_path}")

    # Export closed trades audit
    trades_csv_path = "data/equity_backtest_closed_trades.csv"
    if not trades_df.is_empty():
        trades_df.write_csv(trades_csv_path)
        print(f"[OK] Saved Closed Trades audit to {trades_csv_path}")
    else:
        print("[INFO] No closed trades during this window (all positions held in delivery)")

    # Export summary report to JSON
    summary_path = "data/equity_backtest_summary.json"
    with open(summary_path, "w") as f:
        json.dump(report.__dict__, f, indent=2)
    print(f"[OK] Saved Performance Summary to {summary_path}")

    print("\n" + "=" * 80)
    print("AUDITED PERFORMANCE & RISK SUMMARY (NET OF COSTS & TAXES)")
    print("=" * 80)
    print(f"Simulation Period:            {report.start_date} to {report.end_date} ({report.trading_days} Trading Days)")
    print(f"Initial Capital:              Rs {report.initial_nav:,.2f}")
    print(f"Final Portfolio NAV:          Rs {report.final_nav:,.2f}")
    print(f"Gross PnL:                    Rs {report.gross_pnl:,.2f}")
    print(f"Total Statutory Charges:      Rs {report.total_statutory_charges:,.2f} (STT, Stamp, NSE, GST, DP Fee)")
    print(f"Total Tax Liability:          Rs {report.total_tax_paid:,.2f} (Post-2024 STCG 20% / LTCG 12.5%)")
    print(f"Tax Alpha Saved (Day-366):    Rs {report.tax_alpha_saved:,.2f} (Verified in unit test suite)")
    print(f"Net Realized PnL (After Tax): Rs {report.net_pnl_after_tax:,.2f}")
    print("-" * 80)
    print(f"Strategy Net Return:          {report.strategy_return_pct:+.2f}%")
    print(f"Benchmark ({report.benchmark_name}): {report.benchmark_return_pct:+.2f}%")
    print(f"Raw Alpha Over Benchmark:     {report.alpha_over_benchmark_pct:+.2f}%")
    print("-" * 80)
    print(f"Average Invested Exposure:    {report.average_invested_exposure_pct:.1f}%")
    print(f"Exposure-Adjusted Bench Return:{report.exposure_adjusted_benchmark_return_pct:+.2f}%")
    print(f"Exposure-Adjusted Stock Alpha: {report.exposure_adjusted_alpha_pct:+.2f}%")
    print(f"Cash Cushion Contribution:    {report.cash_cushion_contribution_pct:+.2f}%")
    print("-" * 80)
    print(f"Maximum Drawdown (%):         {report.max_drawdown_pct:.2f}%")
    print(f"Maximum Drawdown (INR):       Rs {report.max_drawdown_inr:,.2f}")
    print(f"Annualized Sharpe Ratio:      {report.sharpe_ratio:.2f}")
    print(f"Deflated Sharpe Ratio (DSR):  {report.deflated_sharpe_ratio:.4f} (Trials: {report.trial_count})")
    print("-" * 80)
    print(f"Closed Positions:             {report.total_trades_closed}")
    print(f"Win Rate:                     {report.win_rate_pct:.1f}%")
    print(f"Profit Factor:                {report.profit_factor:.2f}")
    print(f"Average Holding Period:       {report.avg_holding_days:.1f} days")
    print(f"Portfolio Annualized Turnover:{report.portfolio_turnover_pct:.1f}%")
    print("=" * 80)

if __name__ == "__main__":
    main()
