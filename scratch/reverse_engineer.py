import psycopg2
import polars as pl

def analyze_misses():
    conn = psycopg2.connect(
        host='localhost', port='5432', user='quant', password='quantpassword', database='market_data'
    )
    
    symbols = ['SAREGAMA', 'ADANIENT', 'NLCINDIA', 'JPPOWER']
    
    for symbol in symbols:
        df = pl.read_database(f"""
            SELECT time, close, volume 
            FROM daily_ohlcv 
            WHERE symbol = '{symbol}' AND time >= '2025-01-01' 
            ORDER BY time ASC
        """, conn)
        
        # Compute indicators identically to the screener
        df = df.with_columns([
            pl.col("close").rolling_mean(window_size=10).alias("sma_10"),
            pl.col("close").rolling_mean(window_size=20).alias("sma_20"),
            pl.col("close").rolling_mean(window_size=50).alias("sma_50"),
            pl.col("close").rolling_mean(window_size=200).alias("sma_200"),
            pl.col("volume").rolling_mean(window_size=20).alias("vol_avg_20")
        ])
        
        # ATR calculation
        df = df.with_columns([
            (pl.col("close") - pl.col("close").shift(1)).abs().alias("tr")
        ]).with_columns([
            pl.col("tr").rolling_mean(window_size=3).alias("atr_3"),
            pl.col("tr").rolling_mean(window_size=20).alias("atr_20")
        ])
        
        df = df.with_columns([
            (((pl.col("close") - pl.col("sma_50")) / pl.col("sma_50")) * 100).alias("extension_pct"),
            (((pl.col("close") - pl.col("close").shift(10)) / pl.col("close").shift(10)) * 100).alias("roc_10")
        ])
        
        # Print states for May 10, 11, 12, 13
        print(f"\n--- {symbol} ---")
        for i in range(len(df)):
            if df['time'][i].strftime('%Y-%m-%d') in ['2026-05-10', '2026-05-11', '2026-05-12', '2026-05-13', '2026-05-14']:
                row = df.row(i, named=True)
                print(f"Date: {row['time'].strftime('%Y-%m-%d')}")
                sma50 = row['sma_50'] or 0
                sma200 = row['sma_200'] or 0
                ext = row['extension_pct'] or 0
                roc = row['roc_10'] or 0
                volavg = row['vol_avg_20'] or 1
                atr3 = row['atr_3'] or 0
                atr20 = row['atr_20'] or 0
                
                print(f"  Close: {row['close']} | SMA50: {sma50:.2f} | SMA200: {sma200:.2f}")
                print(f"  Extension: {ext:.2f}% | ROC10: {roc:.2f}%")
                print(f"  Vol: {row['volume']} | Vol Avg 20: {volavg:.2f} | Ratio: {(row['volume']/volavg):.2f}")
                print(f"  ATR3: {atr3:.2f} | ATR20: {atr20:.2f} | Squeeze: {atr3 <= atr20}")

    conn.close()

if __name__ == "__main__":
    analyze_misses()
