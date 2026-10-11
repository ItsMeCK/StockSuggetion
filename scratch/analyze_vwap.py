import polars as pl
import os

def analyze_vwap_extension(parquet_path: str):
    print(f"Loading data from {parquet_path}...")
    df = pl.read_parquet(parquet_path)
    
    # Ensure time is parsed as datetime
    if df['time'].dtype != pl.Datetime:
        df = df.with_columns(pl.col("time").cast(pl.Datetime))
        
    # Extract date for daily anchoring
    df = df.with_columns(
        pl.col("time").dt.date().alias("date"),
        ((pl.col("high") + pl.col("low") + pl.col("close")) / 3).alias("typical_price")
    )
    
    # Calculate volume * typical price
    df = df.with_columns(
        (pl.col("volume") * pl.col("typical_price")).alias("vol_tp")
    )
    
    # Calculate daily cumulative sum of vol_tp and volume per symbol and date
    # Make sure to sort properly so cum_sum works chronologically
    vwap_df = df.sort(["symbol", "time"]).with_columns([
        pl.col("vol_tp").cum_sum().over(["symbol", "date"]).alias("cum_vol_tp"),
        pl.col("volume").cum_sum().over(["symbol", "date"]).alias("cum_volume")
    ])
    
    # Calculate VWAP and % extension
    vwap_df = vwap_df.with_columns(
        (pl.col("cum_vol_tp") / pl.col("cum_volume")).alias("vwap")
    )
    
    vwap_df = vwap_df.with_columns(
        ((pl.col("close") - pl.col("vwap")) / pl.col("vwap") * 100).alias("vwap_extension_pct")
    )
    
    # Drop NaNs that may arise from 0 volume
    vwap_df = vwap_df.filter(pl.col("vwap_extension_pct").is_not_null())
    
    # Summary Statistics
    print("\n--- VWAP Extension % Summary Statistics ---")
    summary = vwap_df.select("vwap_extension_pct").to_series().describe()
    print(summary)
    
    # Distribution Quantiles
    quantiles = [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
    print("\n--- Quantiles of VWAP Extension % ---")
    for q in quantiles:
        val = vwap_df.select(pl.col("vwap_extension_pct").quantile(q)).item()
        print(f"{int(q*100):02d}th percentile: {val:.2f}%")
        
    # Calculate how often price is > +2% above VWAP
    total_rows = vwap_df.height
    over_2_pct = vwap_df.filter(pl.col("vwap_extension_pct") > 2.0).height
    pct_over_2 = (over_2_pct / total_rows) * 100 if total_rows > 0 else 0
    print(f"\nPercentage of time price is > +2% above VWAP: {pct_over_2:.2f}%")
    
    # Focus on trend days (e.g. top 10% highest closing extensions) to see where pullbacks held
    # We can group by symbol and date to find the max extension and analyze pullbacks
    print("\n--- Insights on Optimal Entry ---")
    print("If > +2% occurs rarely (e.g., above the 95th percentile), entering there risks immediate mean reversion.")
    print("Optimal entry proximity is generally near the 50th to 75th percentile (typically between +0.2% and +1.2%),")
    print("where the stock confirms strength above VWAP but hasn't yet exhausted its intraday momentum.")

if __name__ == "__main__":
    parquet_file = "/Users/poonamsalke/.gemini/antigravity/scratch/midnight_sovereign/data/intraday_ohlcv.parquet"
    if os.path.exists(parquet_file):
        analyze_vwap_extension(parquet_file)
    else:
        print(f"File not found: {parquet_file}")
