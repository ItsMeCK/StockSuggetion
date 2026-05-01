import os
import logging
import psycopg2
import polars as pl
from typing import Dict, Any
from core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SectorContextAgent:
    """
    Analyzes Sector Relative Strength (RS).
    If a stock's parent index is also breaking out, it provides a confidence booster.
    """
    def __init__(self):
        self.db_host = os.getenv("DB_HOST", "localhost")
        self.db_port = os.getenv("DB_PORT", "5432")
        self.db_user = os.getenv("POSTGRES_USER", "quant")
        self.db_password = os.getenv("POSTGRES_PASSWORD", "quantpassword")
        self.db_name = os.getenv("POSTGRES_DB", "market_data")
        
        # Mock mapping of sectors. In production, this would be dynamic.
        self.sector_map = {
            "BHEL": "NIFTY_PSE",
            "BEL": "NIFTY_PSE",
            "RELIANCE": "NIFTY_ENERGY",
            "INFY": "NIFTY_IT",
            "TCS": "NIFTY_IT",
            "HDFCBANK": "NIFTY_BANK",
            "APOLLO": "NIFTY_AUTO"
        }

    def analyze_sector_strength(self, tickers: list) -> Dict[str, float]:
        logging.info(f"Sector Agent: Analyzing Relative Strength for {tickers}...")
        sector_scores = {}
        
        try:
            conn = psycopg2.connect(
                host=self.db_host, port=self.db_port, user=self.db_user, password=self.db_password, dbname=self.db_name
            )
            
            for ticker in tickers:
                index_symbol = self.sector_map.get(ticker, "NIFTY_50") # Default to Nifty 50
                
                # Fetch last 10 days of index data
                query = "SELECT time, close FROM daily_ohlcv WHERE symbol = %s ORDER BY time DESC LIMIT 10"
                cur = conn.cursor()
                cur.execute(query, (index_symbol,))
                rows = cur.fetchall()
                cur.close()
                
                if not rows or len(rows) < 5:
                    sector_scores[ticker] = 50.0 # Neutral if no data
                    continue
                
                # Calculate Index RS: (Current Close / 10-day ago Close)
                latest_close = float(rows[0][1])
                old_close = float(rows[-1][1])
                rs_performance = (latest_close / old_close)
                
                # Score 0-100 based on performance. > 1.0 (Rising) = High score.
                # If Index is up 2% in 10 days, give a high score.
                score = min(100.0, max(0.0, (rs_performance - 0.95) / 0.1 * 100))
                sector_scores[ticker] = float(score)
                
                logging.info(f"Sector RS for {ticker} (via {index_symbol}): {score:.1f}")
                
            conn.close()
        except Exception as e:
            logging.error(f"Sector Agent error: {e}")
            for ticker in tickers:
                sector_scores[ticker] = 50.0
                
        return sector_scores

def run_sector_agent(state: SovereignState) -> Dict[str, Any]:
    candidates = state.get("candidates", [])
    if not candidates:
        return {"sector_scores": {}}
        
    agent = SectorContextAgent()
    sector_scores = agent.analyze_sector_strength(candidates)
    
    # Update global agent_scores
    agent_scores = state.get("agent_scores", {})
    for ticker, score in sector_scores.items():
        if ticker not in agent_scores:
            agent_scores[ticker] = {}
        agent_scores[ticker]["sector"] = score
        
    return {"sector_scores": sector_scores, "agent_scores": agent_scores}
