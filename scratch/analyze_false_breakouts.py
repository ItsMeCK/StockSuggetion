import sys
import polars as pl
from datetime import datetime
sys.path.append(".")
from core.sector_mapping import SECTOR_MAP

def analyze_false_breakouts():
    df = pl.read_parquet("data/intraday_ohlcv.parquet")
    fno_symbols = list(SECTOR_MAP.keys())
    df = df.filter(pl.col("symbol").is_in(fno_symbols))
    df = df.sort(["symbol", "time"])

    # Calculate basic institutional features
    df = df.with_columns([
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
        pl.col("time").dt.date().alias("date"),
        (pl.col("close") * pl.col("volume")).alias("pv")
    ])
    
    df = df.with_columns([
        pl.max_horizontal(
            (pl.col("high") - pl.col("low")),
            (pl.col("high") - pl.col("close").shift(1).over("symbol")).abs(),
            (pl.col("low") - pl.col("close").shift(1).over("symbol")).abs()
        ).alias("true_range")
    ])
    
    df = df.with_columns([
        pl.col("true_range").rolling_mean(window_size=14).over("symbol").alias("atr_14")
    ])

    df = df.with_columns([
        pl.col("volume").cum_sum().over(["symbol", "date"]).alias("cum_vol"),
        pl.col("pv").cum_sum().over(["symbol", "date"]).alias("cum_pv")
    ])

    df = df.with_columns([
        (pl.col("cum_pv") / pl.col("cum_vol")).alias("vwap")
    ])

    # Distance from VWAP
    df = df.with_columns([
        ((pl.col("close") - pl.col("vwap")) / pl.col("vwap") * 100).alias("dist_vwap_pct")
    ])
    
    # Intraday Extension (distance from open)
    df = df.with_columns([
        ((pl.col("close") - pl.col("open").first()) / pl.col("open").first() * 100).over(["symbol", "date"]).alias("intraday_ext_pct")
    ])

    # Focus on Aug 20 trades
    # LICI @ 10:15, LICHSGFIN @ 11:15, NYKAA @ 11:15, PREMIERENE @ 12:15, CROMPTON @ 13:15
    target_date = pl.date(2026, 8, 20)
    
    trades = [
        {"symbol": "LICI", "time": "10:15", "result": "WIN"},
        {"symbol": "LICHSGFIN", "time": "11:15", "result": "WIN"},
        {"symbol": "NYKAA", "time": "11:15", "result": "LOSS"},
        {"symbol": "PREMIERENE", "time": "12:15", "result": "LOSS"},
        {"symbol": "CROMPTON", "time": "13:15", "result": "LOSS"}
    ]

    print("\n📊 INSTITUTIONAL FALSE BREAKOUT ANALYSIS 📊")
    for t in trades:
        # Convert hour to target UTC (e.g. 10:15 IST -> 09:15 IST -> 03:45 UTC)
        hour_ist = int(t['time'].split(':')[0])
        hour_utc = hour_ist - 6
        target_utc = datetime(2026, 8, 20, hour_utc, 45, 0)
        
        row = df.filter((pl.col("symbol") == t["symbol"]) & (pl.col("time") == target_utc))
        if not row.is_empty():
            data = row.row(0, named=True)
            print(f"\n{t['result']} | {t['symbol']} @ {t['time']}")
            print(f"  Dist from VWAP: {data['dist_vwap_pct']:.2f}%")
            print(f"  Intraday Ext:   {data['intraday_ext_pct']:.2f}%")
            print(f"  Vol/Avg Vol:    {data['volume']/data['vol_avg_20']:.2f}x")
            
            # Distance relative to ATR
            close_to_vwap_abs = abs(data['close'] - data['vwap'])
            atr_ratio = close_to_vwap_abs / data['atr_14'] if data['atr_14'] > 0 else 0
            print(f"  VWAP Ext (ATR): {atr_ratio:.2f}x ATR")

if __name__ == '__main__':
    analyze_false_breakouts()
