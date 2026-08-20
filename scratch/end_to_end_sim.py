import sys
import os
import polars as pl
from datetime import datetime
import asyncio

# Setup path so it can import from the workspace
sys.path.append(".")
from agents.intraday_debate.intraday_orchestrator import IntradayDebateOrchestrator

def run_end_to_end_sim():
    print("Loading Parquet data...")
    parquet_path = "data/intraday_ohlcv.parquet"
    df = pl.read_parquet(parquet_path)
    df = df.sort(["symbol", "time"])

    # Engine logic from live_hourly_job.py
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
        (pl.col("cum_pv") / pl.col("cum_vol")).alias("vwap"),
        (pl.col("close") * pl.col("volume")).rolling_mean(window_size=20*6).over("symbol").alias("atv")
    ])

    df = df.with_columns([
        ((pl.col("sma_20") + 2 * pl.col("std_20") - (pl.col("sma_20") - 2 * pl.col("std_20"))) / pl.col("sma_20")).alias("bbw")
    ])
    df = df.with_columns([
        pl.col("bbw").rolling_quantile(quantile=0.10, window_size=100).over("symbol").alias("bbw_threshold_10th")
    ])

    df = df.with_columns([
        pl.max_horizontal(
            (pl.col("high") - pl.col("low")),
            (pl.col("high") - pl.col("close").shift(1).over("symbol")).abs(),
            (pl.col("low") - pl.col("close").shift(1).over("symbol")).abs()
        ).alias("true_range")
    ])
    df = df.with_columns([
        pl.col("low").shift(1).over("symbol").alias("low_1"),
        pl.col("low").shift(2).over("symbol").alias("low_2"),
        pl.col("true_range").shift(1).over("symbol").alias("tr_1"),
        pl.col("true_range").shift(2).over("symbol").alias("tr_2"),
        ((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low") + 0.0001) * 100).alias("close_range_pct"),
        pl.col("volume").shift(1).over("symbol").alias("vol_1")
    ])

    df = df.with_columns([
        pl.col("vwap").shift(3).over(["symbol", "date"]).alias("vwap_prev3"),
        pl.col("sma_20").shift(3).over("symbol").alias("sma_20_prev3"),
    ])
    df = df.with_columns([
        (((pl.col("vwap") - pl.col("vwap_prev3")) / pl.col("vwap_prev3")) * 100).alias("vwap_slope_pct"),
        (((pl.col("sma_20") - pl.col("sma_20_prev3")) / pl.col("sma_20_prev3")) * 100).alias("sma20_slope_pct"),
    ])

    df = df.with_columns([
        (((pl.col("close") - pl.col("close").shift(3).over("symbol")) / pl.col("close").shift(3).over("symbol")) * 100).alias("ret_3h")
    ])

    df = df.with_columns([
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
        (pl.col("vol_sum_3") / (3 * pl.col("vol_avg_20"))).alias("vol_surge_3h"),
        ((pl.col("close") - pl.col("sma_20")) / pl.col("sma_20") * 100).alias("dist_sma20"),
        ((pl.col("high") - pl.max_horizontal(pl.col("open"), pl.col("close"))) / (pl.col("high") - pl.col("low") + 0.0001) * 100).alias("upper_wick_pct")
    ])

    target_date_str = sys.argv[1] if len(sys.argv) > 1 else "2026-08-18"
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    
    # Simulation loop
    unique_times = df.filter(pl.col("time").dt.date() == target_date).select("time").unique().sort("time")
    
    # We want to know the final EOD close price for P&L calculation
    eod_time = unique_times.to_series()[-1]
    eod_df = df.filter(pl.col("time") == eod_time)

    orchestrator = IntradayDebateOrchestrator()
    trades_executed = []

    # Iterate starting from 10:15 IST (04:45 UTC) to skip the 09:15 UTC (03:45 UTC)
    for row in unique_times.iter_rows():
        ts = row[0]
        if ts.hour < 4 or (ts.hour == 4 and ts.minute < 45):
            continue # Skip before 10:15 IST
            
        print(f"\n======================================")
        print(f"🕒 RUN: {ts}")
        current_hour_df = df.filter(pl.col("time") == ts)
        
        nifty_df = current_hour_df.filter(pl.col("symbol") == "NIFTY 50")
        nifty_ret_3h = 0.0
        if not nifty_df.is_empty():
            nifty_ret_3h = nifty_df.select(pl.col("ret_3h"))[0, 0]
            if nifty_ret_3h is None: nifty_ret_3h = 0.0
            
        dynamic_coil = (pl.col("bbw") < pl.col("bbw_threshold_10th")) | (pl.col("bbw") < 0.22)
        base_filter = dynamic_coil & (pl.col("close") > pl.col("sma_20")) & (pl.col("close") > pl.col("open"))
        
        ma_awareness = (pl.col("vwap_slope_pct") > 0.1) & (pl.col("sma20_slope_pct").abs() < 0.2)
        
        track_a = (pl.col("vol_surge") > 2.5) & (pl.col("upper_wick_pct") < 25.0)
        
        mega_cap_stealth = (pl.col("atv") >= 500_000_000) & (pl.col("vol_surge_3h") >= 1.60) & (pl.col("close") <= (pl.col("vwap") * 1.015))
        mid_cap_stealth = (pl.col("atv") < 500_000_000) & (pl.col("atv") >= 100_000_000) & (pl.col("vol_surge_3h") >= 1.83) & (pl.col("close") <= (pl.col("vwap") * 1.010))
        
        multi_candle_iaf = (
            (pl.col("low") >= pl.col("low_1")) & (pl.col("low_1") >= pl.col("low_2")) & 
            (pl.col("true_range") <= pl.col("tr_1")) & (pl.col("tr_1") <= pl.col("tr_2")) & 
            (pl.col("volume") >= pl.col("vol_1")) & 
            (pl.col("close_range_pct") <= 85.0)
        )
        
        track_b = (mega_cap_stealth | mid_cap_stealth) & multi_candle_iaf & ma_awareness
        
        rs_divergence = (pl.col("ret_3h") > (nifty_ret_3h + 0.5))
        
        breakouts = current_hour_df.filter(
            base_filter & (track_a | track_b) & rs_divergence
        ).sort("vol_surge", descending=True).head(4)
        
        valid_symbols = [r['symbol'] for r in breakouts.iter_rows(named=True) if r['symbol'] != "NIFTY 50"]
        
        if len(valid_symbols) > 0:
            debate_candidates = []
            for symbol in valid_symbols:
                r_data = breakouts.filter(pl.col("symbol") == symbol).row(0, named=True)
                candle_color = "GREEN" if r_data['close'] > r_data['open'] else "RED"
                vwap_status = "ABOVE_VWAP" if r_data['close'] > r_data['vwap'] else "BELOW_VWAP"
                context = f"Candle: {candle_color}, Wick: {round(r_data['upper_wick_pct'], 2)}%, VWAP: {vwap_status}, Vol Surge: {round(r_data['vol_surge'], 2)}x"
                debate_candidates.append({
                    "symbol": symbol,
                    "context": context
                })
                
            print(f"🧠 Passing {len(debate_candidates)} to Orchestrator: {[c['symbol'] for c in debate_candidates]}")
            
            # The orchestrator is synchronous but we might need event loop, handle it safely
            results = orchestrator.evaluate_all_sync(debate_candidates)
            
            top_trades = []
            for cand, res in zip(debate_candidates, results):
                top_trades.append({
                    "symbol": cand['symbol'],
                    "conviction_score": res.conviction_score,
                    "verdict": res.arbiter_verdict
                })
                
            hour_ist = ts.hour + 5 + (1 if ts.minute + 30 >= 60 else 0)
            threshold = 70 if hour_ist in [10, 11, 12] else 85
            
            for trade in top_trades:
                sym = trade['symbol']
                score = trade['conviction_score']
                
                final_score = score * 1.0 * 1.0 
                
                if final_score >= threshold:
                    print(f"✅ BUY Triggered: {sym} | Score: {final_score}")
                    
                    entry_row = current_hour_df.filter(pl.col("symbol") == sym)
                    if not entry_row.is_empty():
                        entry_price = entry_row.select(pl.col("close"))[0, 0]
                        
                        exit_row = eod_df.filter(pl.col("symbol") == sym)
                        exit_price = entry_price
                        if not exit_row.is_empty():
                            exit_price = exit_row.select(pl.col("close"))[0, 0]
                            
                        pnl_pct = ((exit_price - entry_price) / entry_price) * 100
                        
                        trades_executed.append({
                            "Time (IST)": f"{hour_ist}:15",
                            "Symbol": sym,
                            "Score": final_score,
                            "Entry Price": round(entry_price, 2),
                            "Exit Price": round(exit_price, 2),
                            "P&L (%)": round(pnl_pct, 2)
                        })
                else:
                    print(f"🛑 REJECTED by Debate: {sym} | Score: {final_score} < {threshold}")

    print("\n\n======================================")
    print("📊 FINAL P&L REPORT FOR TODAY")
    if len(trades_executed) == 0:
        print("0 Trades executed after Debate Engine vetoes.")
    else:
        total_pnl = 0.0
        for t in trades_executed:
            print(f"[{t['Time (IST)']}] {t['Symbol']} | Entry: {t['Entry Price']} -> Exit: {t['Exit Price']} | P&L: {t['P&L (%)']}% | Score: {t['Score']}")
            total_pnl += t['P&L (%)']
        print(f"\nNet Cumulative P&L: {round(total_pnl, 2)}%")

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    run_end_to_end_sim()
