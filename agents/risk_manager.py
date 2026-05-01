import os
import logging
import psycopg2
import polars as pl
from typing import Dict, Any

from midnight_sovereign.core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RiskManagerAgent:
    """
    The 'Tactical Sizer'. Calculates position size based on Volatility (ATR) 
    and the final Cognitive Confidence Score.
    """
    def __init__(self, total_capital: float = 1000000.0):
        self.total_capital = total_capital
        self.risk_per_trade_pct = 0.005 # Risk 0.5% of total capital per trade

    def calculate_optimal_size(self, symbol: str, entry_price: float, confidence_score: float, target_date: str) -> Dict[str, Any]:
        """
        Calculates quantity and allocation % using ATR-based volatility sizing.
        """
        atr = 0.0
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", "5432"),
                user=os.getenv("POSTGRES_USER", "quant"),
                password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
                dbname=os.getenv("POSTGRES_DB", "market_data")
            )
            cur = conn.cursor()
            query = """
                SELECT high, low, close 
                FROM daily_ohlcv 
                WHERE symbol = %s AND time <= %s
                ORDER BY time DESC LIMIT 15
            """
            cur.execute(query, (symbol, target_date))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            if len(rows) >= 2:
                df = pl.DataFrame(rows, schema=["high", "low", "close"])
                df = df.with_columns([
                    (pl.col("high") - pl.col("low")).alias("tr1"),
                    (pl.col("high") - pl.col("close").shift(1)).abs().alias("tr2"),
                    (pl.col("low") - pl.col("close").shift(1)).abs().alias("tr3")
                ])
                df = df.with_columns([pl.max_horizontal("tr1", "tr2", "tr3").alias("true_range")])
                atr = df["true_range"].mean()
        except Exception as e:
            logging.error(f"ATR Query Failed for {symbol}: {e}")

        # Fallback if ATR is too low or missing
        if atr is None or atr <= 0:
            atr = entry_price * 0.03 # Assume 3% daily movement as floor
            
        # --- THE SIZING LOGIC (95% PROFIT BASELINE) ---
        stop_distance = max(1.5 * atr, entry_price * 0.02) # Min 2% stop
        risk_amt = self.total_capital * self.risk_per_trade_pct # e.g. 5000
        
        # Quantity = Risk / Stop
        quantity = int(risk_amt / stop_distance)
        
        # Conviction Multiplier
        multiplier = 1.5 if confidence_score >= 85 else (1.0 if confidence_score >= 70 else 0.8)
        final_quantity = max(1, int(quantity * multiplier))
        
        allocation_pct = (final_quantity * entry_price) / self.total_capital * 100
        
        # Cap at 10%
        if allocation_pct > 10.0:
            final_quantity = max(1, int((self.total_capital * 0.10) / entry_price))
            allocation_pct = (final_quantity * entry_price) / self.total_capital * 100

        return {
            "quantity": final_quantity,
            "allocation_pct": round(allocation_pct, 2),
            "entry_price": entry_price,
            "stop_loss": round(entry_price - stop_distance, 2),
            "atr": round(atr, 2),
            "multiplier": multiplier
        }

def run_risk_manager_agent(state: SovereignState) -> Dict[str, Any]:
    """
    LangGraph Node to assign sizing to all approved trades.
    """
    logging.info("Executing Risk Manager Agent...")
    
    critic_approvals = state.get("critic_approvals", [])
    agent_scores = state.get("agent_scores", {})
    entry_trigger_results = state.get("entry_trigger_results", {})
    target_date = state.get("target_date")
    
    sizer = RiskManagerAgent(total_capital=1000000.0)
    approved_allocations = {}
    
    for symbol in critic_approvals:
        scores = agent_scores.get(symbol, {})
        total_score = (scores.get("entry", 0) * 0.4) + (scores.get("vision", 0) * 0.4) + (scores.get("dtw", 0) * 0.2)
        
        # Try to get price from state, fallback to DB
        entry_price = entry_trigger_results.get(symbol, {}).get("entry_price", 0.0)
        if entry_price == 0.0:
            try:
                conn = psycopg2.connect(
                    host=os.getenv("DB_HOST", "localhost"),
                    port=os.getenv("DB_PORT", "5432"),
                    user=os.getenv("POSTGRES_USER", "quant"),
                    password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
                    dbname=os.getenv("POSTGRES_DB", "market_data")
                )
                cur = conn.cursor()
                cur.execute("SELECT close FROM daily_ohlcv WHERE symbol = %s AND time <= %s ORDER BY time DESC LIMIT 1", (symbol, target_date))
                row = cur.fetchone()
                if row: entry_price = float(row[0])
                cur.close()
                conn.close()
            except: pass

        if entry_price > 0:
            risk_params = sizer.calculate_optimal_size(symbol, entry_price, total_score, target_date)
            approved_allocations[symbol] = risk_params
            logging.info(f"RISK CALC SUCCESS: {symbol} | Qty: {risk_params['quantity']} | Price: {entry_price}")

    return {"approved_allocations": approved_allocations}
