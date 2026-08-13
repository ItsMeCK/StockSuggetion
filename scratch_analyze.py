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

def analyze_winners():
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
        WHERE time >= '2026-05-01' AND time <= '2026-07-30'
        ORDER BY symbol, time
    """
    df = pl.read_database(query, conn)
    df = df.filter(~pl.col("symbol").str.contains(r"\d")).sort(["symbol", "time"])
    
    df = df.with_columns([
        (pl.col("close").shift(-1) / pl.col("close") - 1).over("symbol").alias("fwd_return_1d"),
        (pl.col("close").shift(-3) / pl.col("close") - 1).over("symbol").alias("fwd_return_3d"),
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
    ])
    
    df = df.with_columns([
        ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge")
    ])

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
    
    winners = []
    
    for row in setups.iter_rows(named=True):
        S0 = row["close"]
        S_target = S0 * (1 + row["fwd_return_3d"])
        
        opt_price_0 = bs_price(S0, S0, DTE, r, sigma, 'CE')
        opt_price_end = bs_price(S_target, S0, DTE_end, r, sigma, 'CE')
        
        if opt_price_0 > 0:
            trade_pnl = (opt_price_end / opt_price_0) - 1
            if trade_pnl > 0.50:  # Looking for massive >50% option return winners
                winners.append({
                    'symbol': row['symbol'],
                    'date': str(row['time'].date()),
                    'pnl': trade_pnl,
                    'bbw': row['bbw'],
                    'vol': row['vol_surge']
                })
                
    print(f"Out of {len(setups)} setups, found {len(winners)} MASSIVE winners (> +50% Option PnL).")
    for w in sorted(winners, key=lambda x: x['pnl'], reverse=True):
        print(f"{w['symbol']} on {w['date']} -> +{w['pnl']*100:.1f}% (BBW: {w['bbw']:.3f}, Vol: {w['vol']:.1f}x)")

if __name__ == "__main__":
    analyze_winners()
