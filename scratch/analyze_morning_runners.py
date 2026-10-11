import polars as pl
from datetime import datetime

def find_morning_runners():
    df = pl.read_parquet("data/intraday_ohlcv.parquet")
    
    # Filter for today
    today = datetime(2026, 8, 24).date()
    if "date" not in df.columns:
        df = df.with_columns(pl.col("time").dt.date().alias("date"))
    df_today = df.filter(pl.col("date") == today)
    
    if df_today.height == 0:
        print("No data for today found in parquet.")
        return
        
    symbols = df_today["symbol"].unique().to_list()
    
    # Load fno universe
    try:
        import pandas as pd
        fno_df = pd.read_csv("core/fo_universe.csv")
        fno_symbols = set(fno_df['SYMBOL'].tolist())
    except:
        fno_symbols = set()
        
    runners = []
    
    for symbol in symbols:
        if fno_symbols and symbol not in fno_symbols:
            continue
            
        sym_df = df_today.filter(pl.col("symbol") == symbol).sort("time")
        if sym_df.height == 0:
            continue
            
        first_candle = sym_df[0]
        last_candle = sym_df[-1]
        
        pct_move = ((last_candle["close"][0] - first_candle["open"][0]) / first_candle["open"][0]) * 100
        
        if pct_move >= 1.5: # Moved > 1.5% in the morning
            runners.append({
                "symbol": symbol,
                "pct_move": pct_move,
                "open": first_candle["open"][0],
                "last_close": last_candle["close"][0],
                "candles": sym_df.height
            })
            
    runners = sorted(runners, key=lambda x: x["pct_move"], reverse=True)
    
    print(f"Found {len(runners)} F&O stocks that moved > 1.5% today morning:")
    for r in runners[:20]:
        print(f"{r['symbol']}: +{r['pct_move']:.2f}% (Open: {r['open']:.2f}, Close: {r['last_close']:.2f}, Candles: {r['candles']})")

if __name__ == "__main__":
    find_morning_runners()
