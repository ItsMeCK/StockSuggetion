import os
import logging
import polars as pl
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SovereignBacktestWrapper:
    """
    Standalone simulator that evaluates institutional rules over historical timeframes.
    """
    def __init__(self):
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = os.getenv("DB_PORT", "5432")
        self.user = os.getenv("POSTGRES_USER", "quant")
        self.password = os.getenv("POSTGRES_PASSWORD", "quantpassword")
        self.db_name = os.getenv("POSTGRES_DB", "market_data")
        
    def execute_simulation(self, lookback_days: int = 30):
        logging.info(f"Starting {lookback_days}-day institutional state simulation.")
        
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.db_name
            )
            
            # Data Coverage Diagnostic
            diag_query = "SELECT MIN(time) as start_date, MAX(time) as end_date, COUNT(*) as total_rows FROM daily_ohlcv"
            df_diag = pl.read_database(diag_query, conn)
            row = df_diag.to_dicts()[0]
            logging.info(f"DATABASE COVERAGE DIAGNOSTIC:")
            logging.info(f" - Earliest Date: {row['start_date']}")
            logging.info(f" - Latest Date: {row['end_date']}")
            logging.info(f" - Total EOD Rows: {row['total_rows']}")
            
            query = "SELECT time, symbol, close, volume FROM daily_ohlcv ORDER BY symbol, time ASC"
            full_df = pl.read_database(query, conn)
            
            # Get distinct dates
            date_query = "SELECT DISTINCT time FROM daily_ohlcv ORDER BY time DESC LIMIT %s"
            df_dates = pl.read_database(date_query, conn, execute_options={"parameters": (lookback_days,)})
            target_dates = df_dates["time"].to_list()[::-1]
            
            backtest_results = {}
            
            for current_date in target_dates:
                # Filter data up to current simulation date
                df_slice = full_df.filter(pl.col("time") <= current_date)
                
                # Apply Stage 2 filters
                df_slice = df_slice.with_columns([
                    pl.col("close").rolling_mean(window_size=50).over("symbol").alias("sma_50"),
                    pl.col("close").rolling_mean(window_size=200).over("symbol").alias("sma_200")
                ])
                df_slice = df_slice.with_columns([
                    (pl.col("sma_50") - pl.col("sma_50").shift(10).over("symbol")).alias("sma_50_slope_10d"),
                    (((pl.col("close") - pl.col("sma_50")) / pl.col("sma_50")) * 100).alias("extension_pct"),
                    (pl.col("close") * 0.95).alias("mock_avwap")
                ])
                
                # Take the latest row per symbol on or before current_date
                latest_slice = df_slice.group_by("symbol").tail(1)
                
                # Filter candidates
                passing_df = latest_slice.filter(
                    (pl.col("close") > pl.col("sma_50")) &
                    (pl.col("sma_50") > pl.col("sma_200")) &
                    (pl.col("sma_50_slope_10d") > 0) &
                    (pl.col("sma_50_slope_10d") <= 150.0) &
                    (pl.col("close") > pl.col("mock_avwap")) &
                    (pl.col("extension_pct") <= 12.0)
                )
                
                approved_symbols = passing_df["symbol"].to_list()
                
                # Basic exclusion filter
                approved_symbols = [s for s in approved_symbols if not any(x in s for x in ["NIFTY", "BOND", "INV", "ETF", "B22", "BEES"])]
                
                # Track picks for P&L calculation
                for s in approved_symbols[:3]:
                    if s not in backtest_results:
                        # Get entry price on this date
                        entry_price = latest_slice.filter(pl.col("symbol") == s)["close"].to_list()[0]
                        backtest_results[s] = {"entry_date": current_date, "entry_price": entry_price}
                        
            logging.info("Historical state iterations evaluated completely. Calculating P&L...")
            
            # Calculate final P&L relative to the latest overall date
            latest_overall_date = target_dates[-1]
            latest_prices_df = full_df.filter(pl.col("time") == latest_overall_date)
            
            total_pnl_pct = 0.0
            trade_count = 0
            
            for sym, trade_data in backtest_results.items():
                # Find latest price for this symbol
                exit_row = latest_prices_df.filter(pl.col("symbol") == sym)
                if not exit_row.is_empty():
                    exit_price = exit_row["close"].to_list()[0]
                    entry_price = trade_data["entry_price"]
                    pnl_pct = ((exit_price - entry_price) / entry_price) * 100
                    trade_count += 1
                    total_pnl_pct += pnl_pct
                    logging.info(f"Trade: {sym} | Entry: ₹{entry_price:.2f} | Exit: ₹{exit_price:.2f} | P&L: {pnl_pct:+.2f}%")
                else:
                    logging.warning(f"Symbol {sym} not found on exit date {latest_overall_date}")
                    
            if trade_count > 0:
                avg_pnl = total_pnl_pct / trade_count
                logging.info(f"==================================================")
                logging.info(f"SIMULATION COMPLETE | Total Trades: {trade_count} | Average P&L: {avg_pnl:+.2f}%")
                logging.info(f"==================================================")
            else:
                logging.error("No valid historical trades calculated safely.")

            conn.close()
            
        except Exception as e:
            logging.error(f"Wrapper Execution Error: {e}")

if __name__ == "__main__":
    simulator = SovereignBacktestWrapper()
    simulator.execute_simulation(30)
