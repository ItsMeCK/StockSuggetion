import polars as pl
import logging
from datetime import datetime
from typing import List, Dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SovereignScreener:
    """
    The Deterministic Polars Pipeline.
    Aggressively filters the Nifty 500 using cheap, fast math down to a highly probable handful.
    """
    def __init__(self):
        # In production, we connect to TimescaleDB and read directly into Polars using ConnectorX
        pass

    def fetch_active_trades(self) -> List[str]:
        """
        Queries the trade_events ledger for symbols that are currently ACTIVE or AMO_PLACED.
        """
        import os
        import psycopg2
        
        logging.info("Querying ledger for active/pending trades to exclude...")
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", "5432"),
                user=os.getenv("POSTGRES_USER", "quant"),
                password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
                database=os.getenv("POSTGRES_DB", "market_data")
            )
            cur = conn.cursor()
            
            # Subquery to get the latest status for each trade_id
            query = """
                WITH latest_status AS (
                    SELECT ticker, status, 
                           ROW_NUMBER() OVER(PARTITION BY trade_id ORDER BY system_time DESC) as rn
                    FROM trade_events
                )
                SELECT DISTINCT ticker 
                FROM latest_status 
                WHERE rn = 1 AND status IN ('ACTIVE', 'AMO_PLACED');
            """
            cur.execute(query)
            active_symbols = [row[0] for row in cur.fetchall()]
            cur.close()
            conn.close()
            logging.info(f"Excluding {len(active_symbols)} symbols currently in active/pending trades: {active_symbols}")
            return active_symbols
        except Exception as e:
            logging.error(f"Failed to fetch active trades: {e}")
            return []

    def fetch_market_data(self) -> pl.DataFrame:
        """
        Fetches the real Nifty 500 dataset from TimescaleDB.
        """
        import os
        import psycopg2
        
        logging.info("Fetching real OHLCV data from TimescaleDB into Polars...")
        
        # Connection params from .env
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            user=os.getenv("POSTGRES_USER", "quant"),
            password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
            database=os.getenv("POSTGRES_DB", "market_data")
        )
        
        # 1. Load Data from TimescaleDB
        query = """
            SELECT time, symbol, open, high, low, close, volume 
            FROM daily_ohlcv 
            WHERE symbol ~ '^[A-Z0-9]+$' 
              AND LENGTH(symbol) <= 10
              AND symbol NOT ILIKE '%%NIFTY%%'
              AND symbol NOT ILIKE '%%INDEX%%'
              AND symbol NOT ILIKE '%%GS%%'
              AND symbol NOT ILIKE '%%BOND%%'
              AND symbol NOT ILIKE '%%MOMENT%%'
            ORDER BY time ASC
        """
        
        # Read into Polars
        df = pl.read_database(query, conn)
        conn.close()
        
        logging.info(f"Successfully loaded {len(df)} rows from TimescaleDB.")
        return df

    def apply_stage_2_filter(self, df: pl.DataFrame) -> pl.DataFrame:
        logging.info("Calculating Stage 2 & Momentum metrics...")
        
        # Calculate SMAs grouped by symbol
        df = df.with_columns([
            pl.col("close").rolling_mean(window_size=10).over("symbol").alias("sma_10"),
            pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
            pl.col("close").rolling_mean(window_size=50).over("symbol").alias("sma_50"),
            pl.col("close").rolling_mean(window_size=200).over("symbol").alias("sma_200"),
            pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20")
        ])
        
        # Calculate 10-day momentum (slope) of the 50 SMA
        df = df.with_columns([
            (pl.col("sma_50") - pl.col("sma_50").shift(10).over("symbol")).alias("sma_50_slope_10d")
        ])
        
        # --- STAGE 2 & TRANSITION FOCUS ---
        # Traditional Stage 2: 50 SMA > 200 SMA
        # Transition: 50 SMA is SLOPING UP significantly + Volume Thrust
        is_stage_2 = (pl.col("sma_50") > pl.col("sma_200"))
        is_transition = (pl.col("sma_50_slope_10d") > 0) & (pl.col("volume") > (pl.col("vol_avg_20") * 1.5))
        
        df = df.with_columns([
            # Mandatory: Price > 50 SMA for any long
            (pl.col("close") > pl.col("sma_50")).alias("price_above_50"),
            # Macro Stage: Either Stage 2 or explosive Transition
            (is_stage_2 | is_transition).alias("in_macro_regime")
        ])
        
        # --- PRIMARY FILTERS ---
        # 1. Macro Regime Gate
        df = df.with_columns([
            (pl.col("price_above_50") & pl.col("in_macro_regime")).alias("passed_regime")
        ])
        
        # 2. Momentum Ignition Gate (Relaxed)
        df = df.with_columns([
            ((pl.col("close") > pl.col("sma_10")) & (pl.col("close") > pl.col("mock_avwap"))).alias("passed_momentum")
        ])
        
        # Calculate extension from 50 SMA
        df = df.with_columns([
            (((pl.col("close") - pl.col("sma_50")) / pl.col("sma_50")) * 100).alias("extension_pct")
        ])
        
        # Calculate True Range & ATR
        df = df.with_columns([
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
        
        return df

    def apply_avwap_filter(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Applies the Anchored VWAP filter.
        Adds columns without dropping rows yet.
        """
        logging.info("Calculating AVWAP metrics...")
        df = df.with_columns([
            (pl.col("close") * 0.95).alias("mock_avwap")
        ])
        return df

    def run_pipeline(self, target_date: str = None) -> List[str]:
        df = self.fetch_market_data()
        
        # If target_date is provided, slice data up to that date
        if target_date:
            logging.info(f"Slicing historical data up to {target_date}")
            # Ensure target_date is a datetime for comparison
            from datetime import timezone
            target_dt = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            df = df.filter(pl.col("time") <= target_dt)
        
        # 1. Drop non-equities first from main dataset (expanded regex)
        # Filters for alphanumeric NSE symbols, blocks noise, and enforces length
        df = df.filter(
            (pl.col("symbol").str.contains("^[A-Z0-9]+$")) & 
            (pl.col("symbol").str.len_chars() < 15) &
            (~pl.col("symbol").str.contains("VIX|BOND|GS|INDEX|ETF|BEES|NIFTY"))
        )
        
        # 2. Calculate metrics
        df = self.apply_avwap_filter(df)
        df_metrics = self.apply_stage_2_filter(df)
        
        # 3. Take the latest row for each stock (relative to the target_date or current)
        latest_df = df_metrics.group_by("symbol").tail(1)
        
        # 4. Create a robust diagnostic dataframe for the user
        active_trades = self.fetch_active_trades()
        diagnostics = latest_df.with_columns([
            (pl.col("sma_200").is_null()).alias("REJECT_reason_no_200_data"),
            (pl.col("close") <= pl.col("sma_50")).alias("REJECT_reason_below_50_SMA"),
            (pl.col("sma_50") <= pl.col("sma_200")).alias("REJECT_reason_50_below_200_SMA"),
            (pl.col("sma_50_slope_10d") <= 0).alias("REJECT_reason_negative_momentum"),
            (pl.col("extension_pct") > 12.0).alias("REJECT_reason_over_extended"),
            (pl.col("sma_50_slope_10d") > 150.0).alias("REJECT_reason_velocity_breach"),
            (pl.col("sma_10") <= pl.col("sma_20")).alias("REJECT_reason_velocity_cross_fail"),
            (pl.col("volume") < (1.5 * pl.col("vol_avg_20"))).alias("REJECT_reason_volume_thrust_fail"),
            (pl.col("atr_3") > pl.col("atr_20")).alias("REJECT_reason_atr_squeeze_fail"),
            (pl.col("symbol").is_in(active_trades)).alias("EXCLUDED_active_trade"),
            # Shannon Stage 1 -> 2 Transition Marker
            ((pl.col("close") > pl.col("sma_10")) & 
             (pl.col("close") > pl.col("sma_20")) & 
             (pl.col("volume") > (2.0 * pl.col("vol_avg_20")))).alias("IS_stage_transition")
        ])
        
        # Save diagnostics to CSV for user inspection
        diag_path = "screening_diagnostics.csv"
        diagnostics.write_csv(diag_path)
        logging.info(f"Diagnostic report saved to: {diag_path}")
        
        # 5a. Filter for established Stage 2 participation
        stage_2_df = latest_df.filter(
            (pl.col("close") > pl.col("sma_50")) &
            (pl.col("sma_50_slope_10d") > -2.0) & # Allow flat/slight drift
            (pl.col("extension_pct") <= 18.0) & # Catch more momentum
            (pl.col("sma_10") > pl.col("sma_20")) &
            (pl.col("volume") >= (1.2 * pl.col("vol_avg_20"))) & # Relaxed Volume
            (pl.col("atr_3") <= (pl.col("atr_20") * 1.1)) # Slight wiggle in volatility
        )

        # 5b. Filter for Shannon Stage Transition (Institutional Bottoming)
        transition_df = latest_df.filter(
            (pl.col("close") > pl.col("sma_10")) &
            (pl.col("volume") >= (2.0 * pl.col("vol_avg_20"))) & # Relaxed Ignition
            (pl.col("close") > pl.col("sma_50")) &
            (pl.col("extension_pct") <= 12.0)
        )

        final_df = pl.concat([stage_2_df, transition_df]).unique(subset=["symbol"])
        
        # 6. Global Exclusion: Remove symbols already in a trade
        active_trades = self.fetch_active_trades()
        if active_trades:
            final_df = final_df.filter(~pl.col("symbol").is_in(active_trades))
        
        approved_symbols = final_df["symbol"].unique().to_list()
        base_scores = {}
        
        if approved_symbols:
            ranked_candidates = []
            for sym in approved_symbols:
                sym_df = df_metrics.filter(pl.col("symbol") == sym).sort("time")
                if len(sym_df) < 20:
                    continue
                
                latest_row = sym_df.tail(1).to_dicts()[0]
                row_20d = sym_df.tail(20).head(1).to_dicts()[0]
                
                # 1. Proximity
                ext_pct = latest_row["extension_pct"]
                
                # 2. 20d Return
                close_latest = latest_row["close"]
                close_20d = row_20d["close"]
                return_20d = ((close_latest - close_20d) / close_20d) if close_20d else 0.0
                
                # 3. Volume Ratio
                sym_15d = sym_df.tail(16)
                sym_15d = sym_15d.with_columns([
                    (pl.col("close") > pl.col("close").shift(1)).alias("is_green")
                ]).tail(15)
                
                green_vol = sym_15d.filter(pl.col("is_green") == True)["volume"].sum()
                red_vol = sym_15d.filter(pl.col("is_green") == False)["volume"].sum()
                vol_ratio = (green_vol / red_vol) if red_vol > 0 else (green_vol if green_vol else 1.0)
                
                ranked_candidates.append({
                    "symbol": sym,
                    "ext_pct": ext_pct,
                    "return_20d": return_20d,
                    "vol_ratio": vol_ratio
                })
                
            if ranked_candidates:
                # Rank Proximity (Ascending extension_pct)
                ranked_candidates.sort(key=lambda x: x["ext_pct"])
                n = len(ranked_candidates)
                for i, item in enumerate(ranked_candidates):
                    item["prox_score"] = 20 * (1 - i / (n - 1)) if n > 1 else 20
                    
                # Rank RS Score (Descending return_20d)
                ranked_candidates.sort(key=lambda x: x["return_20d"], reverse=True)
                for i, item in enumerate(ranked_candidates):
                    item["rs_score"] = 20 * (1 - i / (n - 1)) if n > 1 else 20
                    
                # Rank Volume Ratio (Descending vol_ratio)
                ranked_candidates.sort(key=lambda x: x["vol_ratio"], reverse=True)
                for i, item in enumerate(ranked_candidates):
                    item["vol_score"] = 20 * (1 - i / (n - 1)) if n > 1 else 20
                    
                for item in ranked_candidates:
                    base_scores[item["symbol"]] = item["prox_score"] + item["rs_score"] + item["vol_score"]
                    
                # Sort by Base Score descending
                ranked_candidates.sort(key=lambda x: base_scores[x["symbol"]], reverse=True)
                approved_symbols = [x["symbol"] for x in ranked_candidates]
        
        logging.info(f"Screener Complete. Final High-Probability Candidates: {approved_symbols}")
        return approved_symbols, base_scores

def run_screener_node(state: dict) -> dict:
    """
    LangGraph Node integration.
    """
    screener = SovereignScreener()
    candidates, base_scores = screener.run_pipeline()
    return {"candidates": candidates, "base_scores": base_scores}

if __name__ == "__main__":
    screener = SovereignScreener()
    screener.run_pipeline()
