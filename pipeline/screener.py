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
            # Dynamic SMA-200: Use 200 if possible, otherwise use 100 or 50
            pl.col("close").rolling_mean(window_size=200).over("symbol").alias("sma_200"),
            pl.col("close").rolling_mean(window_size=100).over("symbol").alias("sma_100"),
            pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20")
        ])
        
        # Fill null SMA-200 with SMA-100 for younger stocks
        df = df.with_columns([
            pl.col("sma_200").fill_null(pl.col("sma_100")).fill_null(pl.col("sma_50"))
        ])
        
        # Calculate 10-day momentum (slope) of the 50 SMA
        df = df.with_columns([
            (pl.col("sma_50") - pl.col("sma_50").shift(10).over("symbol")).alias("sma_50_slope_10d")
        ])
        
        # Calculate extension from 50 SMA
        df = df.with_columns([
            (((pl.col("close") - pl.col("sma_50")) / pl.col("sma_50")) * 100).alias("extension_pct")
        ])

        # Pring Optimization: Rate of Change (ROC)
        df = df.with_columns([
            (((pl.col("close") - pl.col("close").shift(1).over("symbol")) / pl.col("close").shift(1).over("symbol")) * 100).alias("roc_1"),
            (((pl.col("close") - pl.col("close").shift(10).over("symbol")) / pl.col("close").shift(10).over("symbol")) * 100).alias("roc_10"),
            (((pl.col("close") - pl.col("close").shift(20).over("symbol")) / pl.col("close").shift(20).over("symbol")) * 100).alias("roc_20"),
            (((pl.col("volume") - pl.col("volume").rolling_mean(window_size=10).over("symbol")) / pl.col("volume").rolling_mean(window_size=10).over("symbol")) * 100).alias("vol_roc_10")
        ])

        # Pring Optimization: RSI & Money Flow Index (MFI)
        # Simplified RSI (Relative Strength Index)
        df = df.with_columns([
            (pl.col("close") - pl.col("close").shift(1).over("symbol")).alias("diff")
        ]).with_columns([
            pl.when(pl.col("diff") > 0).then(pl.col("diff")).otherwise(0).alias("gain"),
            pl.when(pl.col("diff") < 0).then(pl.col("diff").abs()).otherwise(0).alias("loss")
        ]).with_columns([
            pl.col("gain").rolling_mean(window_size=14).over("symbol").alias("avg_gain"),
            pl.col("loss").rolling_mean(window_size=14).over("symbol").alias("avg_loss")
        ]).with_columns([
            (100 - (100 / (1 + (pl.col("avg_gain") / pl.col("avg_loss"))))).alias("rsi_14")
        ])

        # Money Flow Index (MFI)
        df = df.with_columns([
            ((pl.col("high") + pl.col("low") + pl.col("close")) / 3).alias("typical_price")
        ]).with_columns([
            (pl.col("typical_price") * pl.col("volume")).alias("money_flow")
        ]).with_columns([
            (pl.col("typical_price") > pl.col("typical_price").shift(1).over("symbol")).alias("is_pos_mf")
        ]).with_columns([
            pl.when(pl.col("is_pos_mf")).then(pl.col("money_flow")).otherwise(0).alias("pos_mf"),
            pl.when(pl.col("is_pos_mf").not_()).then(pl.col("money_flow")).otherwise(0).alias("neg_mf")
        ]).with_columns([
            pl.col("pos_mf").rolling_sum(window_size=14).over("symbol").alias("pos_mf_14"),
            pl.col("neg_mf").rolling_sum(window_size=14).over("symbol").alias("neg_mf_14")
        ]).with_columns([
            (100 - (100 / (1 + (pl.col("pos_mf_14") / pl.col("neg_mf_14"))))).alias("mfi_14")
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

    def calculate_market_regime(self, target_date: str = None) -> Dict[str, Any]:
        """
        Analyzes the broader market (NIFTY 50) to determine the Macro Regime.
        Used to block trades during massive global sell-offs (war, inflation spikes).
        """
        import os, psycopg2
        logging.info("Calculating Market Regime via NIFTY 50...")
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", "5432"),
                user=os.getenv("POSTGRES_USER", "quant"),
                password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
                database=os.getenv("POSTGRES_DB", "market_data")
            )
            # Fetch Nifty data. Symbol is 'NIFTY 50'
            query = "SELECT time, close FROM daily_ohlcv WHERE symbol = 'NIFTY 50' ORDER BY time ASC"
            df_nifty = pl.read_database(query, conn)
            conn.close()
            
            if target_date:
                from datetime import timezone
                target_dt = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                df_nifty = df_nifty.filter(pl.col("time") <= target_dt)
            
            if len(df_nifty) < 20:
                return {"regime": "NEUTRAL", "nifty_close": 0, "nifty_sma_20": 0}
            
            # Simplified Regime Filter: Price vs 20-SMA
            df_nifty = df_nifty.with_columns([
                pl.col("close").rolling_mean(window_size=20).alias("nifty_sma_20")
            ])
            
            latest = df_nifty.tail(1).to_dicts()[0]
            close = latest["close"]
            sma_20 = latest["nifty_sma_20"]
            
            # Regime Status: 
            # BEARISH: Price < SMA-20
            # BULLISH: Price > SMA-20 * 1.01
            if close < sma_20:
                regime = "BEARISH"
            elif close > (sma_20 * 1.01):
                regime = "BULLISH"
            else:
                regime = "NEUTRAL"
                
            logging.info(f"Market Regime: {regime} (Nifty: {close:.0f} | SMA-20: {sma_20:.0f})")
            return {"regime": regime, "nifty_close": close, "nifty_sma_20": sma_20}
        except Exception as e:
            logging.error(f"Market Regime calculation failed: {e}")
            return {"regime": "NEUTRAL", "nifty_close": 0, "nifty_sma_20": 0}

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
            logging.info(f"Slicing historical data up to end of {target_date}")
            # Ensure target_date is a datetime for comparison, set to end of day to include EOD
            from datetime import datetime, timezone, timedelta
            target_dt = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
            df = df.filter(pl.col("time") < target_dt)
        
        # 1. Drop non-equities first from main dataset (expanded regex)
        # Filters for alphanumeric NSE symbols, blocks noise, and enforces length
        df = df.filter(
            (pl.col("symbol").str.contains("^[A-Z0-9]+$")) & 
            (pl.col("symbol").str.len_chars() < 15) &
            (~pl.col("symbol").str.contains("VIX|BOND|GS|INDEX|ETF|BEES|NIFTY"))
        )
        
        # 2. Calculate metrics
        df = self.apply_stage_2_filter(df)
        df_metrics = self.apply_avwap_filter(df)
        
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
             (pl.col("volume") >= (2.0 * pl.col("vol_avg_20"))) &
             (pl.col("close") >= (pl.col("sma_50") * 0.98)) &
             (pl.col("extension_pct") <= 12.0)).alias("IS_stage_transition")
        ])
        
        # Save diagnostics to CSV for user inspection
        diag_path = "screening_diagnostics.csv"
        diagnostics.write_csv(diag_path)
        logging.info(f"Diagnostic report saved to: {diag_path}")
        
        # Determine if we are in the relaxed window (Feb 15 - May 1) to capture leaders
        # Forced to TRUE to ensure we catch explosive momentum leaders as requested
        relaxed_window = True 
        if target_date:
            try:
                from datetime import datetime
                td = datetime.strptime(target_date, "%Y-%m-%d")
                if datetime(2026, 2, 15) <= td <= datetime(2026, 5, 1):
                    relaxed_window = True
            except Exception:
                pass
        
        # Adjust thresholds based on the window (Institutional Grade)
        vol_mult_stage2 = 1.2 if relaxed_window else 1.3
        vol_mult_transition = 1.5 if relaxed_window else 1.8
        ext_limit = 15.0 if relaxed_window else 12.0
        
        # 5a. Filter for established Stage 2 participation
        # Pring Rule: Dynamic Extension + Institutional Dry-up Support
        stage_2_df = latest_df.filter(
            (pl.col("close") > pl.col("sma_50")) &
            (
                (pl.col("sma_50") >= pl.col("sma_200")) | # Established Stage 2
                (pl.col("volume") > 2.0 * pl.col("vol_avg_20")) # Institutional Rebirth (Stage 1 -> 2)
            ) &
            (pl.col("sma_50_slope_10d") > -50) & # Allow slight negative slope if recovering fast
            (
                (pl.col("extension_pct") <= 5.0) | 
                ((pl.col("extension_pct") <= 20.0) & (pl.col("roc_10") > pl.col("roc_20")) & (pl.col("volume") > 1.5 * pl.col("vol_avg_20")))
            ) & 
            (pl.col("sma_10") > pl.col("sma_20")) &
            (
                (pl.col("volume") >= (vol_mult_stage2 * pl.col("vol_avg_20"))) | # Thrust
                (pl.col("volume") <= (0.8 * pl.col("vol_avg_20"))) # Institutional Dry-up
            )
        )
        
        # Apply ATR squeeze only if NOT in relaxed window
        if not relaxed_window:
            stage_2_df = stage_2_df.filter(pl.col("atr_3") <= pl.col("atr_20"))

        # 5b. Filter for Shannon Stage Transition / Incubator (The Coil)
        # Pring Rule: Look for Volume Dry-up (< 0.8x) OR early Stage 2 thrust
        transition_df = latest_df.filter(
            (pl.col("close") > pl.col("sma_10")) &
            (pl.col("close") > pl.col("sma_20")) &
            (pl.col("close") >= (pl.col("sma_50") * 0.98)) & 
            (
                (pl.col("extension_pct") <= 5.0) | 
                ((pl.col("extension_pct") <= 12.0) & (pl.col("roc_10") > pl.col("roc_20")))
            ) &
            (
                (pl.col("volume") <= (0.8 * pl.col("vol_avg_20"))) | # The Coil (Dry-up)
                (pl.col("volume") >= (vol_mult_transition * pl.col("vol_avg_20"))) # The Launch (Thrust)
            )
        )

        # Separate symbols
        stage_2_symbols = stage_2_df["symbol"].unique().to_list()
        transition_symbols = transition_df["symbol"].unique().to_list()
        
        # Tag the main dataframe for the Librarian
        latest_df = latest_df.with_columns([
            pl.col("symbol").is_in(stage_2_symbols).alias("is_stage_2"),
            pl.col("symbol").is_in(transition_symbols).alias("is_incubator")
        ])

        # Candidates are established Stage 2 stocks
        approved_symbols = stage_2_symbols
        
        # Incubator contains Shannon Transition stocks that aren't yet in Stage 2 candidates
        incubator_symbols = [s for s in transition_symbols if s not in approved_symbols]
        
        # 6. Global Exclusion: Remove symbols already in a trade
        # CRITICAL: Skip production ledger check during backtests to avoid poisoning results
        if not target_date:
            active_trades = self.fetch_active_trades()
            if active_trades:
                approved_symbols = [s for s in approved_symbols if s not in active_trades]
                incubator_symbols = [s for s in incubator_symbols if s not in active_trades]
        
        base_scores = {}
        
        if approved_symbols:
            ranked_candidates = []
            for sym in approved_symbols:
                sym_df = df.filter(pl.col("symbol") == sym).sort("time")
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
        
        # 7. Final Audit List: Merge established Stage 2 and Shannon Transitions
        all_audit_candidates = list(set(approved_symbols + incubator_symbols))
        
        # Market Regime Check
        macro_regime = self.calculate_market_regime(target_date)
        
        logging.info(f"Screener Complete. Final Audit List: {all_audit_candidates}")
        return all_audit_candidates, incubator_symbols, base_scores, macro_regime

def run_screener_node(state: dict) -> dict:
    """
    LangGraph Node integration.
    """
    screener = SovereignScreener()
    candidates, incubator, base_scores, macro_regime = screener.run_pipeline()
    return {
        "candidates": candidates, 
        "incubator": incubator, 
        "base_scores": base_scores,
        "macro_regime": macro_regime["regime"]
    }

if __name__ == "__main__":
    screener = SovereignScreener()
    screener.run_pipeline()
