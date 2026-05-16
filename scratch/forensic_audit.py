import os
import json
import psycopg2
import polars as pl
from dotenv import load_dotenv
from agents.librarian_agent import SovereignLibrarian

load_dotenv()

def forensic_audit(symbol, target_date):
    print(f"🕵️ Forensic Audit: {symbol} on {target_date}")
    
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'quant'),
        password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
        database=os.getenv('POSTGRES_DB', 'market_data')
    )
    
    # Get the data for that day + 20 days prior for metrics
    query = f"SELECT * FROM daily_ohlcv WHERE symbol = '{symbol}' AND time <= '{target_date}' ORDER BY time DESC LIMIT 30"
    df = pl.read_database(query, conn)
    
    if df.is_empty():
        print("❌ No data found.")
        return

    # Calculate metrics like the marathon does
    df = df.sort('time')
    df = df.with_columns([
        (pl.col('close').pct_change(10) * 100).alias('roc_10'),
        (pl.col('close').pct_change(20) * 100).alias('roc_20'),
        (pl.col('close').pct_change(1) * 100).alias('roc_1'),
        (pl.col('volume') / pl.col('volume').rolling_mean(10)).alias('volume_ratio'),
        ((pl.col('close') - pl.col('close').rolling_mean(20)) / pl.col('close').rolling_mean(20) * 100).alias('extension_pct')
    ])
    
    latest = df.to_dicts()[-1]
    
    # Run Librarian
    librarian = SovereignLibrarian()
    result = librarian.audit_setup(symbol, latest)
    
    print("\n--- LIBRARIAN VERDICT ---")
    print(json.dumps(result, indent=2))
    print("\n--- RAW METRICS ---")
    print(f"Close: {latest['close']}, Open: {latest['open']}")
    print(f"ROC_10: {latest['roc_10']:.2f}, ROC_20: {latest['roc_20']:.2f}")
    print(f"Volume Ratio (RVOL): {latest['volume_ratio']:.2f}")
    print(f"Extension %: {latest['extension_pct']:.2f}")

if __name__ == "__main__":
    forensic_audit("63MOONS", "2026-04-12")
