import os
import json
import logging
import polars as pl
from agents.llm_ranking_agent import LLMRankingAgent

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class IntradayScreener:
    def __init__(self):
        self.parquet_path = "data/intraday_ohlcv.parquet"
        self.llm_agent = LLMRankingAgent()

    def run_screener(self):
        if not os.path.exists(self.parquet_path):
            logging.error(f"Data file {self.parquet_path} not found. Run ingestion first.")
            return

        logging.info("Loading Intraday Parquet Data...")
        df = pl.read_parquet(self.parquet_path)
        
        # Sort chronologically
        df = df.sort(["symbol", "time"])
        
        # Calculate Technicals on the 1-Hour Chart (20-period rolling = 20 trading hours)
        df = df.with_columns([
            pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
            pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
            pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
        ])
        
        # Calculate Coiled Spring metrics
        df = df.with_columns([
            ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
            (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
        ])
        
        # Isolate the very latest (most recent) hourly candle for each stock
        latest_candles = df.group_by("symbol").tail(1)
        
        logging.info(f"Scanning the most recent hourly candle for {len(latest_candles)} symbols...")
        
        # Filter for Coiled Spring Breakouts
        breakouts = latest_candles.filter(
            (pl.col("bbw") < 0.22) & 
            (pl.col("vol_surge") > 2.5) & 
            (pl.col("close") > pl.col("sma_20"))
        )
        
        valid_symbols = [row['symbol'] for row in breakouts.iter_rows(named=True)]
        logging.info(f"🎯 Math Engine found {len(valid_symbols)} Intraday Breakouts: {valid_symbols}")
        
        if len(valid_symbols) == 0:
            logging.info("No trades triggered the Math Engine this hour.")
            return
            
        if len(valid_symbols) > 2:
            # Pass to LLM Ranking Agent to enforce the Capital Constraint (Max 2 Trades)
            logging.info("Sending setups to LLM Ranking Agent for fundamental verification...")
            top_trades = self.llm_agent.rank_trades(valid_symbols, max_picks=2)
            
            print("\n==============================================")
            print("🚀 LIVE INTRADAY EXECUTION SIGNALS 🚀")
            print("==============================================")
            for i, trade in enumerate(top_trades, 1):
                print(f"Rank {i}: {trade['symbol']} | Score: {trade['conviction_score']}")
                print(f"Catalyst: {trade['catalyst_summary']}\n")
        else:
            # If 2 or fewer, just execute them!
            print("\n==============================================")
            print("🚀 LIVE INTRADAY EXECUTION SIGNALS 🚀")
            print("==============================================")
            for i, symbol in enumerate(valid_symbols, 1):
                print(f"Execute: {symbol} (No LLM filtering needed, within limit)")
                
if __name__ == "__main__":
    screener = IntradayScreener()
    screener.run_screener()
