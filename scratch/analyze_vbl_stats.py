import psycopg2
import os
import pandas as pd
import numpy as np

def calculate_rsi(data, periods=14):
    close_delta = data['close'].diff()
    up = close_delta.clip(lower=0)
    down = -1 * close_delta.clip(upper=0)
    ma_up = up.ewm(com=periods - 1, adjust=True, min_periods=periods).mean()
    ma_down = down.ewm(com=periods - 1, adjust=True, min_periods=periods).mean()
    rsi = ma_up / ma_down
    rsi = 100 - (100 / (1 + rsi))
    return rsi

def analyze_vbl():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'quant'),
        password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
        dbname=os.getenv('POSTGRES_DB', 'market_data')
    )
    
    query = """
    SELECT time, open, high, low, close, volume 
    FROM daily_ohlcv 
    WHERE symbol = 'VBL' 
    ORDER BY time ASC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    if df.empty:
        print("No data for VBL")
        return
        
    df['roc'] = df['close'].pct_change(periods=10) * 100
    df['rsi'] = calculate_rsi(df)
    
    # Volume Analysis
    df['vol_sma_20'] = df['volume'].rolling(window=20).mean()
    df['vol_std_20'] = df['volume'].rolling(window=20).std()
    df['vol_z_score'] = (df['volume'] - df['vol_sma_20']) / df['vol_std_20']
    
    # Turnover in Crores
    df['turnover_cr'] = (df['close'] * df['volume']) / 10000000.0
    
    # 5-day tight consolidation check (High - Low percentage over 5 days)
    df['5d_high'] = df['high'].rolling(window=5).max()
    df['5d_low'] = df['low'].rolling(window=5).min()
    df['consolidation_pct'] = ((df['5d_high'] - df['5d_low']) / df['5d_low']) * 100
    
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    
    print("--- VBL STATISTICAL FOOTPRINT ---")
    print(f"Date: {latest['time']}")
    print(f"Close: {latest['close']:.2f}")
    print(f"RSI (14): {latest['rsi']:.2f}")
    print(f"Rate of Change (10d): {latest['roc']:.2f}%")
    print(f"Volume: {latest['volume']:,.0f}")
    print(f"20-Day Avg Volume: {latest['vol_sma_20']:,.0f}")
    print(f"Volume Z-Score: {latest['vol_z_score']:.2f}")
    print(f"Turnover: ₹{latest['turnover_cr']:.2f} Cr")
    print(f"5-Day Consolidation Range: {latest['consolidation_pct']:.2f}%")
    
    print("\n--- RECENT 5 DAYS VOLUME VS PRICE ---")
    recent = df.tail(5)
    for index, row in recent.iterrows():
        print(f"Date: {row['time'].date()} | Close: {row['close']:.2f} | Vol Z: {row['vol_z_score']:.2f} | Turn: ₹{row['turnover_cr']:.0f} Cr | % Change: {row['close']/df.iloc[index-1]['close']*100 - 100:.2f}%")

if __name__ == "__main__":
    analyze_vbl()
