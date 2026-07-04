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
        SELECT ticker, status, price, system_time::date as event_date, notes
        FROM trade_events
        WHERE status IN ('SIGNALED', 'AMO_PLACED')
          AND system_time >= '2026-05-21 00:00:00+00'
        ORDER BY system_time ASC
    """)
    rows = cur.fetchall()
    
    by_date = {}
    total_options_suggestions = 0
    unique_options = set()
    
    for r in rows:
        ticker, status, price, event_date, notes = r
        if ticker in OPTIONS_UNIVERSE:
            if event_date not in by_date:
                by_date[event_date] = set()
            by_date[event_date].add(ticker)
            unique_options.add(ticker)
            total_options_suggestions += 1
        
    print(f"Total Approved Options Suggestions (Post-Critic): {total_options_suggestions}")
    print(f"Total Unique Options Stocks Approved: {len(unique_options)}\n")
    
    print("Daily Options Suggestions Ready for Trade:")
    print("| Date | Count | Approved Options Tickers |")
    print("|------|-------|--------------------------|")
    for d in sorted(by_date.keys()):
        tickers_str = ", ".join(sorted(list(by_date[d])))
        print(f"| {d} | {len(by_date[d])} | {tickers_str} |")
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    analyze()
