import os
import math
import psycopg2
import polars as pl
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
import numpy as np

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

def run_exploration():
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
    
    # Advanced Feature Engineering
    df = df.with_columns([
        (pl.col("close").shift(-1) / pl.col("close") - 1).over("symbol").alias("fwd_return_1d"),
        (pl.col("close").shift(-3) / pl.col("close") - 1).over("symbol").alias("fwd_return_3d"),
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
        pl.col("close").rolling_mean(window_size=50).over("symbol").alias("sma_50"),
        pl.col("close").rolling_mean(window_size=200).over("symbol").alias("sma_200"),
        (pl.col("close") / pl.col("close").shift(5) - 1).over("symbol").alias("return_5d"),
        (pl.col("close") / pl.col("close").shift(10) - 1).over("symbol").alias("return_10d"),
        (pl.col("close") / pl.col("close").shift(20) - 1).over("symbol").alias("return_20d"),
        ((pl.col("high") - pl.col("low")) / pl.col("close")).rolling_mean(window_size=10).over("symbol").alias("atr_10"),
    ])
    
    # Core Strategy Metrics
    df = df.with_columns([
        ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
        ((pl.col("close") / pl.col("sma_50")) - 1).alias("dist_50sma"),
        ((pl.col("close") / pl.col("sma_200")) - 1).alias("dist_200sma"),
        (pl.col("close") / pl.col("high").rolling_max(window_size=50).over("symbol") - 1).alias("dist_50d_high")
    ])
    
    df = df.with_columns([
        pl.col("vol_surge").rolling_mean(window_size=3).over("symbol").alias("vol_surge_3d"),
        pl.col("vol_surge").rolling_mean(window_size=5).over("symbol").alias("vol_surge_5d")
    ])

    # The original 155 setups
    setups = df.filter(
        (pl.col("bbw") < 0.22) & 
        (pl.col("vol_surge") > 2.5) & 
        (pl.col("close") > pl.col("sma_20")) & 
        (pl.col("fwd_return_3d").is_not_null()) &
        (pl.col("time") >= pl.datetime(2026, 7, 1, time_zone="UTC")) &
        (pl.col("time") <= pl.datetime(2026, 7, 30, time_zone="UTC"))
    ).to_pandas()
    
    r = 0.07
    sigma = 0.25
    DTE = 14.0 / 365.0
    DTE_end = 11.0 / 365.0
    
    X = []
    y = []
    
    for _, row in setups.iterrows():
        S0 = row["close"]
        S_target = S0 * (1 + row["fwd_return_3d"])
        
        opt_price_0 = bs_price(S0, S0, DTE, r, sigma, 'CE')
        opt_price_end = bs_price(S_target, S0, DTE_end, r, sigma, 'CE')
        
        if opt_price_0 > 0:
            trade_pnl = (opt_price_end / opt_price_0) - 1
            if trade_pnl <= -0.50: trade_pnl = -0.50
            
            features = [
                row['bbw'], row['vol_surge'], row['vol_surge_3d'], row['vol_surge_5d'],
                row['dist_50sma'], row['dist_200sma'], row['dist_50d_high'],
                row['return_5d'], row['return_10d'], row['return_20d'], row['atr_10']
            ]
            
            # Target: 1 if massive winner (>50%), 0 if loser (<=0%)
            # Ignore the middle ground (0 to 50%) for clearer ML signal
            if trade_pnl > 0.50:
                X.append(features)
                y.append(1)
            elif trade_pnl <= 0.0:
                X.append(features)
                y.append(0)
                
    X = np.array(X)
    y = np.array(y)
    
    # Train a quick Random Forest to see if non-linear ML can find a signal
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    
    clf = RandomForestClassifier(n_estimators=100, max_depth=5, class_weight='balanced', random_state=42)
    clf.fit(X_train, y_train)
    
    y_pred = clf.predict(X_test)
    
    print("=== RANDOM FOREST PREDICTION RESULTS ===")
    print(classification_report(y_test, y_pred))
    
    feature_names = ['bbw', 'vol_surge', 'vol_surge_3d', 'vol_surge_5d', 
                     'dist_50sma', 'dist_200sma', 'dist_50d_high', 
                     'return_5d', 'return_10d', 'return_20d', 'atr_10']
    
    importances = clf.feature_importances_
    indices = np.argsort(importances)[::-1]
    
    print("=== FEATURE IMPORTANCE (What actually separates winners from losers?) ===")
    for f in range(X.shape[1]):
        print(f"{feature_names[indices[f]]:<15}: {importances[indices[f]]:.4f}")

if __name__ == "__main__":
    run_exploration()
