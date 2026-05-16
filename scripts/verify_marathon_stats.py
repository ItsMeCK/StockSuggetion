import os
import psycopg2
import pandas as pd
import polars as pl
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def calculate_improvement_stats():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )
    
    # 1. Get all decisions and their subsequent returns
    query = """
    WITH signal_performance AS (
        SELECT 
            l.time::date as scan_date,
            l.symbol,
            l.status,
            l.price_at_scan,
            e.close as price_7d_later,
            ((e.close - l.price_at_scan) / l.price_at_scan * 100) as actual_return
        FROM decision_ledger l
        JOIN daily_ohlcv e ON l.symbol = e.symbol
        WHERE l.mode = 'back_audit'
          AND l.price_at_scan > 0
          AND e.time::date = l.time::date + INTERVAL '7 days'
    )
    SELECT * FROM signal_performance
    """
    df = pd.read_sql(query, conn)
    df['scan_date'] = pd.to_datetime(df['scan_date'])
    df['week'] = df['scan_date'].dt.isocalendar().week
    df['month'] = df['scan_date'].dt.month
    
    stats = []
    
    for (month, week), group in df.groupby(['month', 'week']):
        total_market_winners = len(group[group['actual_return'] >= 7.0])
        our_signaled_winners = len(group[(group['status'] == 'SIGNALED') & (group['actual_return'] >= 7.0)])
        our_false_positives = len(group[(group['status'] == 'SIGNALED') & (group['actual_return'] <= -5.0)])
        total_signals = len(group[group['status'] == 'SIGNALED'])
        
        capture_rate = (our_signaled_winners / total_market_winners * 100) if total_market_winners > 0 else 0
        accuracy = (our_signaled_winners / total_signals * 100) if total_signals > 0 else 0
        
        stats.append({
            "Month": month,
            "Week": week,
            "Total Winners in Market": total_market_winners,
            "Winners Captured": our_signaled_winners,
            "Loss Makers (Traps)": our_false_positives,
            "Capture Rate (%)": round(capture_rate, 2),
            "Signal Accuracy (%)": round(accuracy, 2)
        })
    
    result_df = pd.DataFrame(stats)
    print("\n🏛️ SOVEREIGN MARATHON IMPROVEMENT STATS")
    print("==========================================")
    print(result_df.to_string(index=False))
    
    # Calculate Monthly Improvement
    print("\n📈 MONTHLY ALPHA CONVERGENCE")
    monthly = result_df.groupby('Month').agg({
        "Capture Rate (%)": "mean",
        "Signal Accuracy (%)": "mean",
        "Loss Makers (Traps)": "sum"
    })
    print(monthly)

if __name__ == "__main__":
    calculate_improvement_stats()
