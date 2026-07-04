import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# List of F&O Options Segment tickers as defined by the user
OPTIONS_UNIVERSE = {
    "CUMMINSIND", "AXISBANK", "ADANIPOWER", "TECHM", "GRASIM", "TRENT", 
    "BALKRISIND", "FEDERALBNK", "ATGL", "SIEMENS", "GAIL", "UPL", 
    "ASTRAL", "DIXON", "REDINGTON", "LT", "LTF", "IDBI", "MOTHERSON", 
    "IDFCFIRSTB"
}

def analyze():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        dbname=os.getenv("POSTGRES_DB", "market_data")
    )
    cur = conn.cursor()
    
    cur.execute("""
        SELECT ticker, status, system_time, notes
        FROM trade_events
        WHERE system_time >= '2026-05-21 00:00:00+00'
        ORDER BY system_time ASC
    """)
    rows = cur.fetchall()
    
    print("All Options Segment Ledger Events since May 21:")
    for r in rows:
        ticker, status, system_time, notes = r
        if ticker in OPTIONS_UNIVERSE:
            print(f"Ticker: {ticker:12} | Status: {status:12} | Time: {system_time} | Notes: {notes}")
            
    cur.close()
    conn.close()

if __name__ == "__main__":
    analyze()
