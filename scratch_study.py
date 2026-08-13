import os
import math
import psycopg2
import polars as pl
from dotenv import load_dotenv
import statistics

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

def run_study():
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
    df = df.filter(~pl.col("symbol").str.contains(r"\d")).sort(["symbol", "time"])
    
    # Base Indicators
    df = df.with_columns([
        (pl.col("close").shift(-1) / pl.col("close") - 1).over("symbol").alias("fwd_return_1d"),
        (pl.col("close").shift(-3) / pl.col("close") - 1).over("symbol").alias("fwd_return_3d"),
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
        pl.col("close").rolling_mean(window_size=50).over("symbol").alias("sma_50"),
        (pl.col("close") / pl.col("close").shift(5) - 1).over("symbol").alias("return_5d"),
        (pl.col("close") - pl.col("close").shift(1)).over("symbol").alias("price_diff")
    ])
    
    # RSI Components
    df = df.with_columns([
        pl.when(pl.col("price_diff") > 0).then(pl.col("price_diff")).otherwise(0).rolling_mean(window_size=14).over("symbol").alias("gain_14"),
        pl.when(pl.col("price_diff") < 0).then(pl.col("price_diff").abs()).otherwise(0).rolling_mean(window_size=14).over("symbol").alias("loss_14")
    ])
    
    # Core Strategy Metrics
    df = df.with_columns([
        ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
        (100 - (100 / (1 + (pl.col("gain_14") / pl.col("loss_14"))))).alias("rsi_14"),
        ((pl.col("close") / pl.col("sma_50")) - 1).alias("dist_50sma")
    ])
    
    df = df.with_columns([
        pl.col("vol_surge").rolling_mean(window_size=3).over("symbol").alias("vol_surge_3d")
    ])

    # The ultra-tight setups to reduce capital requirements
    setups = df.filter(
        (pl.col("bbw") < 0.15) & 
        (pl.col("vol_surge") > 4.0) & 
        (pl.col("close") > pl.col("sma_20")) & 
        (pl.col("fwd_return_3d").is_not_null()) &
        (pl.col("time") >= pl.datetime(2026, 7, 1, time_zone="UTC")) &
        (pl.col("time") <= pl.datetime(2026, 7, 30, time_zone="UTC"))
    )
    
    r = 0.07
    sigma = 0.25
    DTE = 14.0 / 365.0
    DTE_end = 11.0 / 365.0
    
    winners = []
    losers = []
    
    for row in setups.iter_rows(named=True):
        S0 = row["close"]
        S_target = S0 * (1 + row["fwd_return_3d"])
        
        opt_price_0 = bs_price(S0, S0, DTE, r, sigma, 'CE')
        opt_price_end = bs_price(S_target, S0, DTE_end, r, sigma, 'CE')
        
        if opt_price_0 > 0:
            trade_pnl = (opt_price_end / opt_price_0) - 1
            # Apply SL
            if trade_pnl <= -0.50: trade_pnl = -0.50
            
            data = {
                'symbol': row['symbol'],
                'date': str(row['time'].date()),
                'pnl': trade_pnl,
                'bbw': row['bbw'],
                'vol': row['vol_surge'],
                'vol_3d': row['vol_surge_3d'],
                'rsi': row['rsi_14'],
                'dist_50': row['dist_50sma'],
                'return_5d': row['return_5d']
            }
            
            if trade_pnl > 0.50:
                winners.append(data)
            elif trade_pnl <= 0.0:
                losers.append(data)
                
    total_uncapped_pnl = sum(w['pnl'] for w in winners) + sum(l['pnl'] for l in losers)
                
    print(f"Total Evaluated: {len(winners) + len(losers)}")
    print(f"Massive Winners (>50%): {len(winners)}")
    print(f"Total Losers (<=0%): {len(losers)}")
    print(f"Cumulative Uncapped PnL if we took ALL trades: +{total_uncapped_pnl*100:.2f}%\n")
    
    def avg(lst, key): return sum(d[key] for d in lst) / len(lst) if lst else 0
    
    print("=== STATISTICAL COMPARISON ===")
    print(f"{'Metric':<20} | {'Winners (40)':<15} | {'Losers (149)':<15}")
    print("-" * 55)
    print(f"{'Avg RSI':<20} | {avg(winners, 'rsi'):<15.2f} | {avg(losers, 'rsi'):<15.2f}")
    print(f"{'Avg BBW':<20} | {avg(winners, 'bbw'):<15.3f} | {avg(losers, 'bbw'):<15.3f}")
    print(f"{'Avg 1D Vol Surge':<20} | {avg(winners, 'vol'):<15.2f}x | {avg(losers, 'vol'):<15.2f}x")
    print(f"{'Avg 3D Vol Surge':<20} | {avg(winners, 'vol_3d'):<15.2f}x | {avg(losers, 'vol_3d'):<15.2f}x")
    print(f"{'Avg Dist to 50-SMA':<20} | {avg(winners, 'dist_50')*100:<14.2f}% | {avg(losers, 'dist_50')*100:<14.2f}%")
    print(f"{'Avg 5-Day Return':<20} | {avg(winners, 'return_5d')*100:<14.2f}% | {avg(losers, 'return_5d')*100:<14.2f}%")

if __name__ == "__main__":
    run_study()
