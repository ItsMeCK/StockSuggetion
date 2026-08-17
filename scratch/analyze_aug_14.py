import polars as pl
from datetime import datetime
import os

def check_outcomes():
    df = pl.read_parquet("data/intraday_ohlcv.parquet")
    
    # Target symbols on August 14
    targets = [
        ("ELGIEQUIP", "2026-08-14 03:45:00", 614.65),
        ("ADANIENSOL", "2026-08-14 03:45:00", 1609.2),
        ("MINDACORP", "2026-08-14 03:45:00", 752.2),
        ("MGL", "2026-08-14 03:45:00", 1153.0),
        ("TARIL", "2026-08-14 04:45:00", 302.4),
        ("VOLTAS", "2026-08-14 04:45:00", 1317.7),
        ("PTCIL", "2026-08-14 04:45:00", 18994.0),
        ("SHYAMMETL", "2026-08-14 05:45:00", 988.9),
        ("CREDITACC", "2026-08-14 05:45:00", 1559.0),
        ("MFSL", "2026-08-14 05:45:00", 1544.8),
        ("HAVELLS", "2026-08-14 06:45:00", 1297.0),
        ("SAREGAMA", "2026-08-14 07:45:00", 527.3),
        ("ONESOURCE", "2026-08-14 07:45:00", 1569.0),
        ("PETRONET", "2026-08-14 07:45:00", 281.95),
        ("UNITDSPR", "2026-08-14 07:45:00", 1539.5),
    ]
    
    print(f"{'Symbol':<12} | {'Entry Time':<16} | {'Entry Price':<11} | {'Max High':<10} | {'Max Gain%':<10} | {'EOD Price':<10} | {'EOD Gain%':<10}")
    print("-" * 85)
    
    win = 0
    loss = 0
    
    from datetime import timezone
    for symbol, time_str, entry in targets:
        dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        # Filter all candles AFTER the entry for that day
        # Time in parquet is UTC
        # Aug 14 ends at 10:00:00 UTC (15:30 IST)
        sub = df.filter(
            (pl.col("symbol") == symbol) & 
            (pl.col("time") > dt) & 
            (pl.col("time").dt.date() == dt.date())
        ).sort("time")
        
        if len(sub) == 0:
            print(f"{symbol:<12} | {dt.strftime('%H:%M UTC'):<16} | {entry:<11.2f} | {'N/A':<10} | {'N/A':<10} | {'N/A':<10} | {'N/A':<10}")
            continue
            
        max_high = sub.select(pl.max("high")).item()
        eod_close = sub.select(pl.col("close")).to_series().to_list()[-1]
        
        max_gain = ((max_high - entry) / entry) * 100
        eod_gain = ((eod_close - entry) / entry) * 100
        
        if eod_gain >= 0.0:
            win += 1
        else:
            loss += 1
            
        print(f"{symbol:<12} | {dt.strftime('%H:%M UTC'):<16} | {entry:<11.2f} | {max_high:<10.2f} | {max_gain:>8.2f}% | {eod_close:<10.2f} | {eod_gain:>8.2f}%")
        
    print(f"\nTotal Wins (Positive EOD): {win}")
    print(f"Total Losses (Negative EOD): {loss}")

if __name__ == '__main__':
    check_outcomes()
