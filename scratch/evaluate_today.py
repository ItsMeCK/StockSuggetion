import polars as pl
from datetime import datetime
import os
from agents.llm_ranking_agent import LLMRankingAgent
from core.sector_mapping import get_sector_for_symbol

def evaluate_today():
    print("Loading data...")
    df = pl.read_parquet("data/intraday_ohlcv.parquet")
    
    # Calculate all the new math engine variables
    df = df.with_columns([
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
        pl.col("volume").rolling_sum(window_size=3).over("symbol").alias("vol_sum_3"),
        pl.col("time").dt.date().alias("date"),
        (pl.col("close") * pl.col("volume")).alias("pv")
    ])
    
    df = df.with_columns([
        pl.col("volume").cum_sum().over(["symbol", "date"]).alias("cum_vol"),
        pl.col("pv").cum_sum().over(["symbol", "date"]).alias("cum_pv")
    ])
    
    df = df.with_columns([
        ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
        (pl.col("vol_sum_3") / (3 * pl.col("vol_avg_20"))).alias("vol_surge_3h"),
        (pl.col("cum_pv") / pl.col("cum_vol")).alias("vwap"),
        ((pl.col("close") - pl.col("sma_20")) / pl.col("sma_20") * 100).alias("dist_sma20"),
        ((pl.col("high") - pl.max_horizontal(pl.col("open"), pl.col("close"))) / (pl.col("high") - pl.col("low") + 0.0001) * 100).alias("upper_wick_pct")
    ])
    
    # Filter to today (Aug 17)
    today_date = datetime(2026, 8, 17).date()
    df = df.filter(pl.col("date") == today_date)
    
    # Get unique hours in the day
    unique_hours = df.select(pl.col("time")).unique().sort("time")['time'].to_list()
    
    agent = LLMRankingAgent()
    total_pnl_pct = 0.0
    total_trades = 0
    
    print("\n--- SIMULATION TRADES (Today: Aug 17) ---")
    
    for t in unique_hours:
        current_hour_df = df.filter(pl.col("time") == t)
        
        # 4. Math Engine (Dual-Track Volume + Dual-Tier Gap & Go)
        base_filter = (pl.col("bbw") < 0.22) & (pl.col("close") > pl.col("sma_20")) & (pl.col("close") > pl.col("open"))
        
        track_a = (pl.col("vol_surge") > 2.5)
        track_b = (pl.col("vol_surge_3h") > 1.5) & (pl.col("vol_surge") > 1.1) & (pl.col("close") > pl.col("vwap"))
        volume_filter = (track_a | track_b)
        
        tier_1 = (pl.col("dist_sma20") <= 3.0) & (pl.col("upper_wick_pct") < 25.0)
        tier_2 = (pl.col("dist_sma20") > 3.0) & (pl.col("dist_sma20") <= 7.5) & (pl.col("upper_wick_pct") < 12.0) & (pl.col("vol_surge") > 3.5)
        tier_filter = (tier_1 | tier_2)
        
        breakouts = current_hour_df.filter(
            base_filter & volume_filter & tier_filter
        ).sort("vol_surge", descending=True).head(4)
        
        valid_symbols = [row['symbol'] for row in breakouts.iter_rows(named=True)]
        
        if len(valid_symbols) == 0:
            continue
            
        print(f"\n[{t.strftime('%Y-%m-%d %H:%M')}] System triggered Math Setups: {valid_symbols}")
        
        llm_candidates = []
        for symbol in valid_symbols:
            row = breakouts.filter(pl.col("symbol") == symbol).row(0, named=True)
            candle_color = "GREEN" if row['close'] > row['open'] else "RED"
            vwap_status = "ABOVE_VWAP" if row['close'] > row['vwap'] else "BELOW_VWAP"
            llm_candidates.append({
                "symbol": symbol,
                "candle_color": candle_color,
                "upper_wick_pct": round(row['upper_wick_pct'], 2),
                "vwap_status": vwap_status,
                "vol_surge": round(row['vol_surge'], 2)
            })
            
        top_trades = agent.rank_trades(llm_candidates, max_picks=2)
        
        for trade in top_trades:
            sym = trade['symbol']
            score = trade['conviction_score']
            
            # Find the index of the current hour in unique_hours to get the entry price (open of the NEXT hour)
            idx = unique_hours.index(t)
            if idx + 1 < len(unique_hours):
                entry_time = unique_hours[idx + 1]
                entry_row = df.filter((pl.col("time") == entry_time) & (pl.col("symbol") == sym))
                if len(entry_row) > 0:
                    entry_price = entry_row[0, "open"]
                else:
                    entry_price = breakouts.filter(pl.col("symbol") == sym)[0, "close"] # Fallback
            else:
                entry_price = breakouts.filter(pl.col("symbol") == sym)[0, "close"] # Last candle fallback
                
            # Exit price is the close of the 14:15 or 15:15 candle (end of day)
            exit_row = df.filter((pl.col("symbol") == sym)).sort("time").tail(1)
            if len(exit_row) > 0:
                exit_price = exit_row[0, "close"]
                pnl = ((exit_price - entry_price) / entry_price) * 100
                total_pnl_pct += pnl
                total_trades += 1
                
                print(f"  -> 🤖 LLM SELECTED: {sym} (Score: {score})")
                print(f"     Catalyst: {trade['catalyst_summary']}")
                print(f"     📈 Result: Entry @ {entry_price:.2f}, Exit @ {exit_price:.2f} | PnL: {pnl:.2f}%")

    print(f"\n==============================================")
    print(f"Total Intraday Trades Taken: {total_trades}")
    print(f"Total Cumulative PnL %: {total_pnl_pct:.2f}%")
    if total_trades > 0:
        print(f"Average Return per Trade: {total_pnl_pct/total_trades:.2f}%")
    print(f"==============================================")


if __name__ == '__main__':
    evaluate_today()
