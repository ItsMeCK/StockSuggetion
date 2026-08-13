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
        SELECT time, symbol, close, high, low, volume 
        FROM daily_ohlcv 
        WHERE time >= '2026-03-01' AND time <= '2026-07-30'
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
        (pl.col("close") - pl.col("close").shift(1)).over("symbol").alias("price_diff"),
        ((pl.col("high") - pl.col("low")) / pl.col("close")).rolling_mean(window_size=10).over("symbol").alias("atr_10")
    ])
    
    # RSI
    df = df.with_columns([
        pl.when(pl.col("price_diff") > 0).then(pl.col("price_diff")).otherwise(0).rolling_mean(window_size=14).over("symbol").alias("gain_14"),
        pl.when(pl.col("price_diff") < 0).then(pl.col("price_diff").abs()).otherwise(0).rolling_mean(window_size=14).over("symbol").alias("loss_14")
    ])
    
    # Core Strategy Metrics
    df = df.with_columns([
        ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
        (100 - (100 / (1 + (pl.col("gain_14") / pl.col("loss_14"))))).alias("rsi_14"),
        ((pl.col("close") / pl.col("sma_50")) - 1).alias("dist_50sma"),
        (pl.col("close") / pl.col("high").rolling_max(window_size=50).over("symbol") - 1).alias("dist_50d_high")
    ])

    # The 44 ultra-tight setups
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
            if trade_pnl <= -0.50: trade_pnl = -0.50
            
            data = {
                'symbol': row['symbol'],
                'date': str(row['time'].date()),
                'pnl': trade_pnl * 100,
                'rsi': row['rsi_14'],
                'return_5d': row['return_5d'] * 100 if row['return_5d'] else 0,
                'atr_10': row['atr_10'] * 100 if row['atr_10'] else 0,
                'dist_50d_high': row['dist_50d_high'] * 100 if row['dist_50d_high'] else 0
            }
            
            if trade_pnl > 0.0:
                winners.append(data)
            else:
                losers.append(data)
                
    print(f"=== WINNERS ({len(winners)}) ===")
    for w in sorted(winners, key=lambda x: x['pnl'], reverse=True):
        print(f"{w['symbol']:<12} PnL: +{w['pnl']:<7.1f}% | RSI: {w['rsi']:<5.1f} | 5d Ret: {w['return_5d']:<5.1f}% | Dist 50d High: {w['dist_50d_high']:<5.1f}% | ATR: {w['atr_10']:.1f}%")

    print(f"\n=== LOSERS ({len(losers)}) ===")
    for l in sorted(losers, key=lambda x: x['pnl']):
        print(f"{l['symbol']:<12} PnL: {l['pnl']:<8.1f}% | RSI: {l['rsi']:<5.1f} | 5d Ret: {l['return_5d']:<5.1f}% | Dist 50d High: {l['dist_50d_high']:<5.1f}% | ATR: {l['atr_10']:.1f}%")

if __name__ == "__main__":
    run_study()
