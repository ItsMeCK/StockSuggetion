import os
import psycopg2
import polars as pl
import logging
import concurrent.futures
from typing import List, Dict, Any
from agents.news_catalyst_agent import NewsCatalystAgent

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EventCatalystScreener:
    """
    Runs before the main pipeline. 
    Finds stocks with massive institutional turnover but tight consolidation (The Coil),
    then audits their news to see if there is an upcoming or fresh catalyst to front-run.
    """
    def __init__(self):
        self.news_agent = NewsCatalystAgent()

    def get_quiet_accumulation_stocks(self, target_date: str = None, top_n: int = 20) -> List[str]:
        """
        Query DB for Top N highest turnover stocks that are currently coiled (ATR squeeze, flat momentum).
        """
        logging.info("Catalyst Screener: Finding quietly accumulating high-turnover stocks...")
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", "5432"),
                user=os.getenv("POSTGRES_USER", "quant"),
                password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
                database=os.getenv("POSTGRES_DB", "market_data")
            )
            
            date_filter = f"time < '{target_date} 00:00:00+00' AND" if target_date else ""
            
            # Use CTEs to get the last 20 days of data for each stock, calculate ATR, Turnover, and ROC
            # For simplicity, we just pull the raw data for the last 25 days and compute in Polars
            query = f"""
                SELECT time, symbol, open, high, low, close, volume 
                FROM daily_ohlcv 
                WHERE {date_filter}
                symbol ~ '^[A-Z0-9]+$' 
                AND LENGTH(symbol) <= 10
                AND symbol NOT ILIKE '%%NIFTY%%'
                AND symbol NOT ILIKE '%%INDEX%%'
                AND symbol NOT ILIKE '%%GS%%'
                AND symbol NOT ILIKE '%%BOND%%'
                AND symbol NOT ILIKE '%%MOMENT%%'
            """
            df = pl.read_database(query, conn)
            conn.close()
            
            if len(df) == 0:
                return []
                
            # Compute required metrics
            df = df.sort(["symbol", "time"])
            
            df = df.with_columns([
                (((pl.col("close") - pl.col("close").shift(1).over("symbol")) / pl.col("close").shift(1).over("symbol")) * 100).alias("roc_1"),
                (pl.col("close") * pl.col("volume")).alias("turnover"),
                pl.max_horizontal([
                    (pl.col("high") - pl.col("low")),
                    (pl.col("high") - pl.col("close").shift(1).over("symbol")).abs(),
                    (pl.col("low") - pl.col("close").shift(1).over("symbol")).abs()
                ]).alias("true_range")
            ])
            
            df = df.with_columns([
                pl.col("true_range").rolling_mean(window_size=3).over("symbol").alias("atr_3"),
                pl.col("true_range").rolling_mean(window_size=20).over("symbol").alias("atr_20")
            ])
            
            # Take latest row
            latest_df = df.group_by("symbol").tail(1)
            
            # Filter for Quiet Accumulation
            # Turnover > 10 Crores (100_000_000)
            # ROC < 4.0% (Hasn't exploded yet today)
            # ROC > -2.0% (Not dumping)
            # ATR 3 <= ATR 20 (Volatility contraction)
            quiet_df = latest_df.filter(
                (pl.col("turnover") > 100_000_000) &
                (pl.col("roc_1") < 4.0) &
                (pl.col("roc_1") > -2.0) &
                (pl.col("atr_3") <= (pl.col("atr_20") * 1.2)) # Slight tolerance for coil
            )
            
            # Sort by Turnover descending and take Top N
            top_stocks = quiet_df.sort("turnover", descending=True).head(top_n)
            candidates = top_stocks["symbol"].to_list()
            
            logging.info(f"Catalyst Screener: Found {len(candidates)} quietly accumulating stocks. Top 5: {candidates[:5]}")
            return candidates

        except Exception as e:
            logging.error(f"Catalyst Screener DB error: {e}")
            return []

    def evaluate_catalysts(self, symbols: List[str], target_date: str = None) -> List[str]:
        """
        Runs NewsCatalystAgent on the symbols concurrently.
        Returns those with a FRESH_REACTION or BOARD_MEETING/EARNINGS_BEAT catalyst.
        """
        injected_catalysts = []
        logging.info(f"Catalyst Screener: Evaluating {len(symbols)} stocks for imminent news catalysts...")
        
        def _check_stock(symbol):
            res = self.news_agent.evaluate_news_catalysts(symbol, target_date)
            return symbol, res

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(_check_stock, sym) for sym in symbols]
            for future in concurrent.futures.as_completed(futures):
                sym, result = future.result()
                if result.get("catalyst_detected"):
                    cat_type = result.get("catalyst_type")
                    status = result.get("priced_in_status")
                    score = result.get("score_boost", 0.0)
                    
                    if status == "FRESH_REACTION" and score >= 15.0:
                        logging.info(f"🚀 CATALYST INCUBATOR FOUND: {sym} | {cat_type} | {result.get('summary')}")
                        injected_catalysts.append(sym)
                    elif cat_type == "BOARD_MEETING" and status != "CONSUMED":
                        # Board meetings are upcoming, so they are pure front-running plays
                        logging.info(f"📅 UPCOMING CATALYST FOUND: {sym} | BOARD MEETING | {result.get('summary')}")
                        injected_catalysts.append(sym)

        return injected_catalysts

    def run(self, target_date: str = None) -> List[str]:
        candidates = self.get_quiet_accumulation_stocks(target_date)
        if not candidates:
            return []
        
        injected_symbols = self.evaluate_catalysts(candidates, target_date)
        return injected_symbols

if __name__ == "__main__":
    screener = EventCatalystScreener()
    res = screener.run()
    print("Injected Catalyst Stocks:", res)
