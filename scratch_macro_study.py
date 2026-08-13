import os
import math
import psycopg2
import polars as pl
from dotenv import load_dotenv

def norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def bs_price(S, K, T, r, sigma, opt_type):
    if T <= 0:
        return max(0.0, S - K) if opt_type == 'CE' else max(0.0, K - S)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if opt_type == 'CE':
        return S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)

def run_macro_study():
    load_dotenv()
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'quant'),
        password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
        dbname=os.getenv('POSTGRES_DB', 'market_data')
    )
    
    query = """
        SELECT time, symbol, close, volume 
        FROM daily_ohlcv 
        WHERE time >= '2026-04-01' AND time <= '2026-07-30'
        ORDER BY symbol, time
    """
    df = pl.read_database(query, conn)
    
    # NIFTY 50 MACRO TREND
    nifty = df.filter(pl.col("symbol") == "NIFTY 50").sort("time")
    nifty = nifty.with_columns([
        pl.col("close").rolling_mean(window_size=20).alias("nifty_sma_20"),
        pl.col("close").rolling_mean(window_size=10).alias("nifty_sma_10"),
        (pl.col("close") / pl.col("close").shift(5) - 1).alias("nifty_return_5d")
    ])
    
    # Keep only the relevant macro columns
    nifty = nifty.select(["time", "close", "nifty_sma_20", "nifty_sma_10", "nifty_return_5d"]).rename({"close": "nifty_close"})
    
    df = df.filter(~pl.col("symbol").str.contains(r"\d")).sort(["symbol", "time"])
    
    # Base Indicators
    df = df.with_columns([
        (pl.col("close").shift(-1) / pl.col("close") - 1).over("symbol").alias("fwd_return_1d"),
        (pl.col("close").shift(-3) / pl.col("close") - 1).over("symbol").alias("fwd_return_3d"),
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
    ])
    
    # Core Strategy Metrics
    df = df.with_columns([
        ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
    ])
    
    # JOIN MACRO DATA
    df = df.join(nifty, on="time", how="left")

    # The 150 setups
    setups = df.filter(
        (pl.col("bbw") < 0.22) & 
        (pl.col("vol_surge") > 2.5) & 
        (pl.col("close") > pl.col("sma_20")) & 
        (pl.col("fwd_return_3d").is_not_null()) &
        (pl.col("time") >= pl.datetime(2026, 7, 1, time_zone="UTC")) &
        (pl.col("time") <= pl.datetime(2026, 7, 30, time_zone="UTC"))
    )
    
    r = 0.07
    sigma = 0.25
    DTE = 14.0 / 365.0
    DTE_end = 11.0 / 365.0
    
    all_trades = []
    
    for row in setups.iter_rows(named=True):
        S0 = row["close"]
        S_target = S0 * (1 + row["fwd_return_3d"])
        
        opt_price_0 = bs_price(S0, S0, DTE, r, sigma, 'CE')
        opt_price_end = bs_price(S_target, S0, DTE_end, r, sigma, 'CE')
        
        if opt_price_0 > 0:
            trade_pnl = (opt_price_end / opt_price_0) - 1
            if trade_pnl <= -0.50: trade_pnl = -0.50
            
            data = {
                'symbol': row['symbol'],
                'date': str(row['time'].date()),
                'pnl': trade_pnl,
                'nifty_close': row['nifty_close'],
                'nifty_sma_20': row['nifty_sma_20'],
                'nifty_sma_10': row['nifty_sma_10'],
                'nifty_return_5d': row['nifty_return_5d']
            }
            all_trades.append(data)
            
    print(f"Total Base Setups: {len(all_trades)}")
    print(f"Base Uncapped PnL: +{sum(t['pnl'] for t in all_trades)*100:.2f}%\n")
    
    # 1. Macro Filter: Nifty > 20 SMA
    macro_1 = [t for t in all_trades if t['nifty_close'] > t['nifty_sma_20']]
    pnl_1 = sum(t['pnl'] for t in macro_1)
    
    # 2. Macro Filter: Nifty > 10 SMA (Shorter term trend)
    macro_2 = [t for t in all_trades if t['nifty_close'] > t['nifty_sma_10']]
    pnl_2 = sum(t['pnl'] for t in macro_2)
    
    # 3. Macro Filter: Nifty 5-Day Return > 0 (Positive short-term momentum)
    macro_3 = [t for t in all_trades if t['nifty_return_5d'] and t['nifty_return_5d'] > 0]
    pnl_3 = sum(t['pnl'] for t in macro_3)
    
    print("=== MACRO FILTER RESULTS ===")
    print(f"Filter 1: Nifty > 20 SMA")
    print(f"Trades Taken: {len(macro_1)}")
    print(f"Winners: {len([t for t in macro_1 if t['pnl'] > 0])}")
    print(f"Losers: {len([t for t in macro_1 if t['pnl'] <= 0])}")
    print(f"New PnL: +{pnl_1*100:.2f}%\n")
    
    print(f"Filter 2: Nifty > 10 SMA")
    print(f"Trades Taken: {len(macro_2)}")
    print(f"Winners: {len([t for t in macro_2 if t['pnl'] > 0])}")
    print(f"Losers: {len([t for t in macro_2 if t['pnl'] <= 0])}")
    print(f"New PnL: +{pnl_2*100:.2f}%\n")
    
    print(f"Filter 3: Nifty 5-Day Return > 0")
    print(f"Trades Taken: {len(macro_3)}")
    print(f"Winners: {len([t for t in macro_3 if t['pnl'] > 0])}")
    print(f"Losers: {len([t for t in macro_3 if t['pnl'] <= 0])}")
    print(f"New PnL: +{pnl_3*100:.2f}%\n")

if __name__ == "__main__":
    run_macro_study()
