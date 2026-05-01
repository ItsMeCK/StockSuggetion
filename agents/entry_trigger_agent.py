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
            
            # Fetch OHLCV to calculate Volume Thrust & VSA
            query = """
                SELECT time, symbol, open, high, low, close, volume 
                FROM daily_ohlcv 
                WHERE symbol IN %s 
                ORDER BY symbol, time ASC
            """
            cur = conn.cursor()
            # Handling tuple formatting for single item lists
            if len(tickers) == 1:
                cur.execute("SELECT time, symbol, open, high, low, close, volume FROM daily_ohlcv WHERE symbol = %s ORDER BY time ASC", (tickers[0],))
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
                
            df = pl.DataFrame(rows, schema=["time", "symbol", "open", "high", "low", "close", "volume"])
            
            # Calculate indicators
            df = df.with_columns([
                pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
                pl.col("volume").rolling_std(window_size=20).over("symbol").alias("vol_20d_std"),
                pl.col("close").rolling_mean(window_size=10).over("symbol").alias("sma_10")
            ])
            
            df = df.with_columns([
                ((pl.col("volume") - pl.col("vol_avg_20")) / pl.col("vol_20d_std")).alias("vol_z_score"),
                (pl.col("high") - pl.col("low")).alias("price_spread"),
                ((pl.col("high") - pl.col("low")) / pl.col("close") * 100).alias("spread_pct")
            ])
            
            # Calculate average spread for VSA
            df = df.with_columns([
                pl.col("spread_pct").rolling_mean(window_size=20).over("symbol").alias("avg_spread_20")
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
                spread_pct = row["spread_pct"]
                avg_spread_20 = row["avg_spread_20"] if row["avg_spread_20"] else 1.0
                
                # 1. Volume Z-Score Trigger (> 1.5)
                vol_thrust = vol_z_score > 1.5
                
                # 2. 10-SMA Floor
                sma_floor = close >= sma_10
                
                # 3. VSA (Volume Spread Analysis)
                # Effort vs Result: High volume must produce Wide Spread
                # If Volume is high but spread is narrow (< avg_spread), it is 'Churn' (Distribution)
                is_churn = vol_z_score > 1.5 and spread_pct < (avg_spread_20 * 0.8)
                vsa_score_mod = -20.0 if is_churn else (15.0 if (vol_z_score > 1.0 and spread_pct > avg_spread_20) else 0.0)
                
                # 3. Momentum Ignition: Is price breaking the high of the last 2 days?
                cur_idx = df.filter(pl.col("symbol") == ticker).height - 1
                prev_highs = df.filter(pl.col("symbol") == ticker).slice(cur_idx-2, 2)["close"].max()
                momentum_ignition = close >= prev_highs
                
                # --- FUZZY SCORING LOGIC (The 80% Rule) ---
                score = 0.0
                
                # 1. Volume Intensity (Max 40 pts)
                # Z-score of 2.0 = 40 pts, 1.0 = 20 pts
                vol_score = min(40.0, max(0.0, (vol_z_score / 2.0) * 40.0))
                score += vol_score
                
                # 2. SMA Floor (Max 30 pts)
                sma_score = 30.0 if close >= sma_10 else (15.0 if close >= (sma_10 * 0.99) else 0.0)
                score += sma_score
                
                # 3. Momentum Proximity (Max 30 pts)
                # "Fuzzy": 90% score if at the high, even if not crossed
                dist_from_high = (prev_highs - close) / close if close > 0 else 0.0
                if dist_from_high <= 0:
                    mom_score = 30.0 # Decisive Breakout
                elif dist_from_high <= 0.005:
                    mom_score = 27.0 # "At the high" (90% of 30)
                elif dist_from_high <= 0.02:
                    mom_score = 15.0 # Close proximity
                else:
                    mom_score = 0.0
                score += mom_score
                
                # 4. Multi-Timeframe Alignment (MTA) Bonus (+10 pts)
                # We mock the 65-min check here. In production, this queries Zerodha.
                mta_aligned = True # Mocking aggressive 65-min momentum
                if mta_aligned:
                    score = min(100.0, score + 10.0)
                
                # 5. VSA Modifier (Max 15 pts or -20 pts)
                score += vsa_score_mod

                results[ticker] = {
                    "approved": True if score >= 80 else False, # New threshold hint
                    "entry_score": float(score),
                    "entry_price": float(close),
                    "vol_thrust": vol_thrust,
                    "sma_floor": sma_floor,
                    "momentum_ignition": momentum_ignition,
                    "mta_aligned": mta_aligned,
                    "vol_z_score": float(vol_z_score),
                    "rejection_reason": "Low Confidence" if score < 60 else "None"
                }
                
                logging.info(f"Gear 2 Cognitive Scoring for {ticker}: Score={score:.1f}/100 | Vol:{vol_score:.1f} SMA:{sma_score:.1f} Mom:{mom_score:.1f} MTA:{'+10' if mta_aligned else '0'}")
                
        except Exception as e:
            logging.error(f"Gear 2 Processing Error: {e}")
            for ticker in tickers:
                results[ticker] = {"approved": False, "reason": f"Execution error: {e}"}
                
        return results

def run_entry_trigger_agent(state: SovereignState) -> Dict[str, Any]:
    candidates = state.get("candidates", [])
    agent = EntryTriggerAgent()
    results = agent.check_momentum_triggers(candidates)
    
    # Update global agent_scores in state
    agent_scores = state.get("agent_scores", {})
    for ticker, data in results.items():
        if ticker not in agent_scores:
            agent_scores[ticker] = {}
        agent_scores[ticker]["entry"] = data.get("entry_score", 0.0)

    return {
        "entry_trigger_results": results,
        "agent_scores": agent_scores
    }
