import os
import logging
import psycopg2
import numpy as np
from typing import Dict, Any
from core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class WatcherAgent:
    """
    Monitors the Incubator thread states dynamically using real metrics.
    """
    def analyze_incubation(self, symbol: str) -> str:
        logging.info(f"Watcher Agent: Querying TimescaleDB metrics for {symbol}...")
        
        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('TIMESCALE_PORT', '5432'),
                user=os.getenv('TIMESCALE_USER', 'quant'),
                password=os.getenv('TIMESCALE_PASSWORD', 'quantpassword'),
                database=os.getenv('TIMESCALE_DB', 'market_data')
            )
            cur = conn.cursor()
            query = """
                SELECT high, low, close 
                FROM daily_ohlcv 
                WHERE symbol = %s 
                ORDER BY time DESC 
                LIMIT 20
            """
            cur.execute(query, (symbol,))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            if len(rows) < 14:
                return f"Insufficient data for {symbol} (found {len(rows)} rows). Incubating normally."

            # Calculate ATR
            true_ranges = []
            for i in range(len(rows) - 1):
                high = float(rows[i][0])
                low = float(rows[i][1])
                prev_close = float(rows[i+1][2])
                
                tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
                true_ranges.append(tr)
                
            atr_current = np.mean(true_ranges[:5])
            atr_historical = np.mean(true_ranges[5:14])
            
            if atr_current < atr_historical:
                return f"Volatility contraction detected for {symbol}. Current ATR ({atr_current:.2f}) is shrinking vs historical ({atr_historical:.2f}). Setup coiling."
            else:
                return f"Incubating {symbol}. ATR expanding ({atr_current:.2f} vs {atr_historical:.2f})."
                
        except Exception as e:
            logging.error(f"Watcher Agent error for {symbol}: {e}")
            return f"Incubating {symbol} (Error fetching real metrics)."

def run_watcher_agent(state: SovereignState) -> Dict[str, Any]:
    incubator_list = state.get("incubator", [])
    if not incubator_list:
        # Use candidates if incubator is empty
        incubator_list = state.get("candidates", [])
        
    watcher = WatcherAgent()
    incubator_notes = {}
    
    for symbol in incubator_list:
        note = watcher.analyze_incubation(symbol)
        incubator_notes[symbol] = note
        
    return {"incubator_notes": incubator_notes}
