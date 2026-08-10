import os
import sys
import psycopg2

def main():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'quant'),
        password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
        dbname=os.getenv('POSTGRES_DB', 'market_data')
    )
    cur = conn.cursor()
    cur.execute("SELECT MAX(time::date) FROM daily_ohlcv")
    max_dt_stock = cur.fetchone()[0]
    
    cur.execute("SELECT MAX(date) FROM option_oi_daily")
    max_dt_opt = cur.fetchone()[0]
    
    print(f"Max stock date in DB: {max_dt_stock}")
    print(f"Max option date in DB: {max_dt_opt}")
    
    # Check if we have records for CONCOR, LICHSGFIN, RELIANCE, SBICARD, VOLTAS on July 9th
    cur.execute("""
        SELECT symbol, time::date, open, high, low, close, volume 
        FROM daily_ohlcv 
        WHERE symbol IN ('CONCOR', 'LICHSGFIN', 'RELIANCE', 'SBICARD', 'VOLTAS')
        AND time::date = '2026-07-09'
    """)
    rows = cur.fetchall()
    print(f"July 9th daily stock records count: {len(rows)}")
    for r in rows:
        print(r)
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
