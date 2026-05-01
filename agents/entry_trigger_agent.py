import os
import logging
import psycopg2
import polars as pl
from typing import Dict, Any

from midnight_sovereign.core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EntryTriggerAgent:
    """
    Gear 2: The 'At-Bat' (The Momentum Entry Trigger)
    Monitors Gear 1 candidates and validates if setup is ready for activation.
    """
    def __init__(self):
        self.db_host = os.getenv("DB_HOST", "localhost")
        self.db_port = os.getenv("DB_PORT", "5432")
        self.db_user = os.getenv("POSTGRES_USER", "quant")
        self.db_password = os.getenv("POSTGRES_PASSWORD", "quantpassword")
        self.db_name = os.getenv("POSTGRES_DB", "market_data")

    def check_momentum_triggers(self, tickers: list) -> Dict[str, Any]:
        logging.info(f"Gear 2 At-Bat: Checking momentum ignition rules for {tickers}...")
        results = {}
        
        if not tickers:
            return results
            
        try:
            conn = psycopg2.connect(
                host=self.db_host, port=self.db_port, user=self.db_user, password=self.db_password, dbname=self.db_name
            )
            
            # Fetch OHLCV to calculate Volume Thrust & 10 SMA
            query = """
                SELECT time, symbol, close, volume 
                FROM daily_ohlcv 
                WHERE symbol IN %s 
                ORDER BY symbol, time ASC
            """
            cur = conn.cursor()
            # Handling tuple formatting for single item lists
            if len(tickers) == 1:
                cur.execute("SELECT time, symbol, close, volume FROM daily_ohlcv WHERE symbol = %s ORDER BY time ASC", (tickers[0],))
            else:
                cur.execute(query, (tuple(tickers),))
            
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            if not rows:
                logging.warning("No OHLCV data retrieved for Gear 2 trigger assessment.")
                for ticker in tickers:
                    results[ticker] = {"approved": False, "reason": "Missing historical data"}
                return results
                
            df = pl.DataFrame(rows, schema=["time", "symbol", "close", "volume"])
            
            # Calculate indicators
            df = df.with_columns([
                pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
                pl.col("volume").rolling_std(window_size=20).over("symbol").alias("vol_20d_std"),
                pl.col("close").rolling_mean(window_size=10).over("symbol").alias("sma_10")
            ])
            
            df = df.with_columns([
                ((pl.col("volume") - pl.col("vol_avg_20")) / pl.col("vol_20d_std")).alias("vol_z_score")
            ])
            
            latest_df = df.group_by("symbol").tail(1)
            
            for ticker in tickers:
                ticker_row = latest_df.filter(pl.col("symbol") == ticker)
                if ticker_row.is_empty():
                    results[ticker] = {"approved": False, "reason": "No latest candle data"}
                    continue
                    
                row = ticker_row.to_dicts()[0]
                close = row["close"]
                volume = row["volume"]
                vol_avg_20 = row["vol_avg_20"] if row["vol_avg_20"] else 1.0
                sma_10 = row["sma_10"] if row["sma_10"] else close
                vol_z_score = row["vol_z_score"] if row["vol_z_score"] is not None else 0.0
                
                # 1. Volume Z-Score Trigger (> 1.5)
                vol_thrust = vol_z_score > 1.5
                
                # 2. 10-SMA Floor
                sma_floor = close >= sma_10
                
                # 3. Momentum Ignition: Is price breaking the high of the last 2 days?
                cur_idx = df.filter(pl.col("symbol") == ticker).height - 1
                prev_highs = df.filter(pl.col("symbol") == ticker).slice(cur_idx-2, 2)["close"].max()
                momentum_ignition = close >= prev_highs
                
                approved = vol_thrust and sma_floor and momentum_ignition
                
                rejections = []
                if not vol_thrust: rejections.append(f"Dynamic RVOL Fail (Z-Score: {vol_z_score:.2f} < 1.5)")
                if not sma_floor: rejections.append(f"Below 10-SMA Floor ({close} < {sma_10:.2f})")
                if not momentum_ignition: rejections.append(f"No Momentum Ignition (Price {close} < 2-day high {prev_highs:.2f})")
                
                results[ticker] = {
                    "approved": approved,
                    "vol_thrust": vol_thrust,
                    "sma_floor": sma_floor,
                    "momentum_ignition": momentum_ignition,
                    "vol_z_score": float(vol_z_score),
                    "rejection_reason": ", ".join(rejections) if rejections else "None"
                }
                logging.info(f"Gear 2 Assessment for {ticker}: Approved={approved} | Z-Score={vol_z_score:.2f} | {results[ticker]['rejection_reason']}")
                
        except Exception as e:
            logging.error(f"Gear 2 Processing Error: {e}")
            for ticker in tickers:
                results[ticker] = {"approved": False, "reason": f"Execution error: {e}"}
                
        return results

def run_entry_trigger_agent(state: SovereignState) -> Dict[str, Any]:
    candidates = state.get("candidates", [])
    agent = EntryTriggerAgent()
    results = agent.check_momentum_triggers(candidates)
    
    incubator = []
    breakouts = []
    for ticker, res in results.items():
        if res.get("approved"):
            breakouts.append(ticker)
        else:
            incubator.append(ticker)
            
    return {
        "entry_trigger_results": results,
        "incubator": incubator,
        "breakouts": breakouts
    }
