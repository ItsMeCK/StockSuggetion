import logging
import os
import polars as pl
import psycopg2
from typing import Dict, Any

from core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MomentumAdaptationAgent:
    """
    Evaluates 'flagged' high-momentum candidates that failed the primary screener's strict extension limits.
    Uses Martin Pring's principles to distinguish dangerous parabolic chases from legitimate explosive Stage 2 ignitions.
    """
    def __init__(self):
        pass

    def fetch_recent_data(self, symbol: str, target_date: str = None) -> pl.DataFrame:
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", "5432"),
                user=os.getenv("POSTGRES_USER", "quant"),
                password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
                database=os.getenv("POSTGRES_DB", "market_data")
            )
            
            date_filter = f"AND time <= '{target_date} 23:59:59+00'" if target_date else ""
            
            query = f"""
                SELECT time, close, volume, high, low 
                FROM daily_ohlcv 
                WHERE symbol = '{symbol}' {date_filter}
                ORDER BY time ASC
            """
            df = pl.read_database(query, conn)
            conn.close()
            return df
        except Exception as e:
            logging.error(f"MomentumAgent failed to fetch data for {symbol}: {e}")
            return pl.DataFrame()

    def evaluate_momentum(self, symbol: str, regime: str, target_date: str = None) -> bool:
        """
        Returns True if the stock should be 'rescued' and passed to the Librarian.
        """
        df = self.fetch_recent_data(symbol, target_date)
        if len(df) < 20:
            return False

        # Calculate ROC and RSI
        df = df.with_columns([
            (((pl.col("close") - pl.col("close").shift(10)) / pl.col("close").shift(10)) * 100).alias("roc_10"),
            (pl.col("close") - pl.col("close").shift(1)).alias("diff")
        ]).with_columns([
            pl.when(pl.col("diff") > 0).then(pl.col("diff")).otherwise(0).alias("gain"),
            pl.when(pl.col("diff") < 0).then(pl.col("diff").abs()).otherwise(0).alias("loss")
        ]).with_columns([
            pl.col("gain").rolling_mean(window_size=14).alias("avg_gain"),
            pl.col("loss").rolling_mean(window_size=14).alias("avg_loss")
        ]).with_columns([
            (100 - (100 / (1 + (pl.col("avg_gain") / pl.col("avg_loss"))))).alias("rsi_14")
        ])

        latest = df.tail(1).to_dicts()[0]
        roc_10 = latest.get("roc_10", 0)
        rsi_14 = latest.get("rsi_14", 50)
        turnover = latest.get("close", 0) * latest.get("volume", 0)

        # Dynamic Thresholds based on Regime & Turnover (Titan Bypass)
        is_titan = turnover >= 5_000_000_000  # 500 Crores
        
        if is_titan:
            min_roc = 5.0  # Aggressively relaxed for Titans
            min_rsi = 60.0
            logging.info(f"⚡ TITAN DETECTED: {symbol} (Turnover: ₹{turnover/1e7:.1f} Cr). Applying aggressive momentum bypass.")
        else:
            min_roc = 10.0 if regime == "BULLISH" else 15.0
            min_rsi = 65.0 if regime == "BULLISH" else 70.0

        if roc_10 > min_roc and rsi_14 > min_rsi:
            logging.info(f"🚀 RESCUED: {symbol} (ROC: {roc_10:.1f}%, RSI: {rsi_14:.1f}) overrode extension limits in {regime} regime.")
            return True
            
        logging.info(f"🛑 REJECTED CHASE: {symbol} (ROC: {roc_10:.1f}%, RSI: {rsi_14:.1f}) failed Pring momentum validation.")
        return False

def run_momentum_adaptation_node(state: SovereignState) -> Dict[str, Any]:
    """
    LangGraph Node integration.
    """
    agent = MomentumAdaptationAgent()
    flagged = state.get("flagged_momentum_candidates", [])
    regime = state.get("macro_regime", "NEUTRAL")
    target_date = state.get("target_date")
    
    rescued_candidates = []
    
    for symbol in flagged:
        if agent.evaluate_momentum(symbol, regime, target_date):
            rescued_candidates.append(symbol)
            
    return {
        "candidates": rescued_candidates # This will be appended to the state.candidates via add_lists reducer
    }
