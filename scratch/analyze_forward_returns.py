import polars as pl
from datetime import datetime
import pytz

def analyze_forward_returns():
    print("Loading Parquet data...")
    parquet_path = "data/intraday_ohlcv.parquet"
    df = pl.read_parquet(parquet_path)
    
    # Filter to today
    df = df.filter(pl.col("time").dt.date() == pl.date(2026, 8, 18)).sort(["symbol", "time"])
    
    triggers = {
        "04:45:00": ['JYOTICNC', 'MEDANTA', 'TIINDIA', 'MRPL'],
        "05:45:00": ['DLF', 'ZFCVINDIA', 'BOSCHLTD'],
        "06:45:00": ['AFCONS', 'LTF', 'ABCAPITAL', 'ZENTEC'],
        "07:45:00": ['GLAXO', 'HEG', 'LTF', 'PCBL']
    }
    
    results = []
    
    for time_str, symbols in triggers.items():
        # Parse UTC time
        target_time = datetime.strptime(f"2026-08-18 {time_str}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
        
        # Determine IST time for display
        ist_tz = pytz.timezone('Asia/Kolkata')
        target_ist = target_time.astimezone(ist_tz)
        ist_str = target_ist.strftime("%H:%M")
        
        for sym in symbols:
            # Get the data for this symbol
            sym_df = df.filter(pl.col("symbol") == sym)
            
            # Entry candle
            entry_row = sym_df.filter(pl.col("time") == target_time)
            if entry_row.is_empty():
                continue
                
            entry_price = entry_row.select(pl.col("close"))[0, 0]
            
            # Forward data
            forward_df = sym_df.filter(pl.col("time") > target_time)
            
            if forward_df.is_empty():
                max_high = entry_price
                eod_close = entry_price
            else:
                max_high = forward_df.select(pl.max("high"))[0, 0]
                eod_close = forward_df.select(pl.last("close"))[0, 0]
                
            max_exc_pct = ((max_high - entry_price) / entry_price) * 100
            eod_pnl_pct = ((eod_close - entry_price) / entry_price) * 100
            
            results.append({
                "Time": ist_str,
                "Symbol": sym,
                "Entry": round(entry_price, 2),
                "Max High": round(max_high, 2),
                "Max Excursion %": round(max_exc_pct, 2),
                "EOD Close": round(eod_close, 2),
                "EOD P&L %": round(eod_pnl_pct, 2)
            })
            
    # Display
    print("\n--- FORWARD RETURNS ANALYSIS ---")
    res_df = pl.DataFrame(results)
    if not res_df.is_empty():
        print(res_df)
        print("\nSummary:")
        print(f"Average Max Excursion: {res_df['Max Excursion %'].mean():.2f}%")
        print(f"Average EOD P&L: {res_df['EOD P&L %'].mean():.2f}%")
        
        # How many actually broke out > 1% ?
        breakouts = res_df.filter(pl.col("Max Excursion %") >= 1.0)
        print(f"\nBreakouts (> 1% push): {len(breakouts)} out of {len(res_df)}")
        if len(breakouts) > 0:
            print(breakouts.select(["Time", "Symbol", "Max Excursion %"]))
            
        winners = res_df.filter(pl.col("EOD P&L %") > 0)
        print(f"\nEOD Winners (> 0%): {len(winners)} out of {len(res_df)}")

if __name__ == "__main__":
    analyze_forward_returns()
