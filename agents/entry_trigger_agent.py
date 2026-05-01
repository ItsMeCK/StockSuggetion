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
                raw_df = self.fetch_ticker_data(ticker)
                if raw_df is None or raw_df.is_empty():
                    results[ticker] = {"approved": False, "reason": "No ticker data"}
                    continue
                
                # Enrich with metrics
                ticker_df = self.screener.apply_avwap_filter(raw_df)
                ticker_df = self.screener.apply_stage_2_filter(ticker_df)
                
                ticker_row = ticker_df.tail(1)
                if ticker_row.is_empty():
                    results[ticker] = {"approved": False, "reason": "No latest candle data"}
                    continue

                # --- PARAMETER EXTRACTION ---
                row = ticker_row.to_dicts()[0]
                close = row["close"]
                volume = row["volume"]
                vol_avg_20 = row["vol_avg_20"] if row["vol_avg_20"] else 1.0
                sma_10 = row["sma_10"] if row["sma_10"] else close
                sma_20 = row["sma_20"] if row["sma_20"] else close
                sma_50 = row["sma_50"] if row["sma_50"] else close
                vol_z_score = row["vol_z_score"] if row["vol_z_score"] is not None else 0.0
                spread_pct = row["spread_pct"]
                avg_spread_20 = row["avg_spread_20"] if row["avg_spread_20"] else 1.0
                
                # Effort vs Result (VSA)
                is_churn = vol_z_score > 1.5 and spread_pct < (avg_spread_20 * 0.8)
                vsa_score_mod = -20.0 if is_churn else (15.0 if (vol_z_score > 1.0 and spread_pct > avg_spread_20) else 0.0)
                
                # 3. Momentum Ignition: Is price breaking the high of the last 2 days?
                cur_idx = ticker_df.height - 1
                prev_highs = ticker_df.slice(cur_idx-2, 2)["close"].max()
                momentum_ignition = close >= prev_highs
                
                # Relaxed for consensus-based approval
                approved = vol_thrust or momentum_ignition
                
                rejections = []
                if not vol_thrust: rejections.append(f"Dynamic RVOL Fail (Z-Score: {vol_z_score:.2f} < 1.5)")
                if not sma_floor: rejections.append(f"Below 10-SMA Floor ({close} < {sma_10:.2f})")
                if not momentum_ignition: rejections.append(f"No Momentum Ignition (Price {close} < 2-day high {prev_highs:.2f})")
                
                # --- SCORING LOGIC ---
                score = 0.0
                
                # 1. Volume Intensity (Max 40 pts)
                # Z-score of 2.0 = 40 pts, 0.0 = 0 pts
                vol_score = min(40.0, max(0.0, (vol_z_score / 2.0) * 40.0))
                score += vol_score
                
                # 2. SMA Support (Max 30 pts)
                # 30 pts if above 10-SMA, 20 pts if above 50-SMA (Institutional Floor)
                if close >= sma_10:
                    sma_score = 30.0
                elif close >= sma_50:
                    sma_score = 20.0
                else:
                    sma_score = 0.0
                score += sma_score
                
                # 3. Momentum Proximity (Max 30 pts)
                # If price is at or above 2-day high = 30 pts. 
                # If within 2% of high = 15 pts.
                dist_from_high = (prev_highs - close) / close if close > 0 else 1.0
                if dist_from_high <= 0:
                    mom_score = 30.0
                elif dist_from_high <= 0.02:
                    mom_score = 15.0
                else:
                    mom_score = 0.0
                score += mom_score
                
                # 4. VSA Modifier (Max 15 pts or -20 pts)
                score += vsa_score_mod

                # --- PIVOT POINT ENTRY (95% PROFIT BASELINE) ---
                results[ticker] = {
                    "approved": True if score >= 60 else False,
                    "entry_score": float(score),
                    "entry_price": float(close),
                    "vol_thrust": vol_thrust,
                    "sma_floor": sma_floor,
                    "momentum_ignition": momentum_ignition,
                    "vol_z_score": float(vol_z_score),
                    "rejection_reason": ", ".join(rejections) if rejections else "None"
                }
                
                logging.info(f"Gear 2 Scoring for {ticker}: Score={score:.1f}/100 | Vol:{vol_score:.1f} SMA:{sma_score:.1f} Mom:{mom_score:.1f}")
                
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
