import os, sys, pandas as pd, psycopg2, logging
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from run_historical import run_historical_engine
from core.vector_store import SovereignVectorStore

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_outcome(symbol, date):
    # Check price 7 days later to see if it was a loss
    conn = psycopg2.connect(host='localhost', port=5432, user='quant', password='quantpassword', database='market_data')
    cur = conn.cursor()
    cur.execute("SELECT close FROM daily_ohlcv WHERE symbol = %s AND time > %s ORDER BY time ASC LIMIT 7", (symbol, date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    if len(rows) < 7: return "NEUTRAL"
    prices = [float(r[0]) for r in rows]
    entry = prices[0]
    if min(prices) <= entry * 0.95: return "LOSS" # Hit 5% Stop
    if max(prices) >= entry * 1.05: return "WIN" # Hit 5% Target
    return "NEUTRAL"

def train_memory():
    store = SovereignVectorStore()
    # Training Dates: Early April
    train_dates = ['2026-04-02', '2026-04-06', '2026-04-10']
    
    logging.info("--- PHASE 1: TRAINING MEMORY ON EARLY APRIL FAILURES ---")
    for date in train_dates:
        output = run_historical_engine(date)
        approved = output.get("approved", [])
        
        for sym in approved:
            outcome = get_outcome(sym, date)
            if outcome == "LOSS":
                logging.warning(f"TEACHING ENGINE: Recording {sym} as a LOSS (Trap) on {date}")
                # Fetch 60 day embedding
                conn = psycopg2.connect(host='localhost', port=5432, user='quant', password='quantpassword', database='market_data')
                cur = conn.cursor()
                cur.execute("SELECT close FROM daily_ohlcv WHERE symbol = %s AND time <= %s ORDER BY time DESC LIMIT 60", (sym, date))
                prices = [float(r[0]) for r in cur.fetchall()[::-1]]
                cur.close()
                conn.close()
                
                if len(prices) >= 60:
                    base = prices[0]
                    emb = [(p - base) / base for p in prices]
                    store.save_pattern(sym, date, "historical_trap", "LOSS", emb)

def test_learning():
    logging.info("\n--- PHASE 2: TESTING RECOGNITION IN LATE APRIL ---")
    test_dates = ['2026-04-23', '2026-04-30']
    
    for date in test_dates:
        logging.info(f"RUNNING MEMORY-ENABLED AUDIT FOR {date}...")
        output = run_historical_engine(date)
        # The DisqualificationAgent logs should show 'VECTOR_TRAP' vetoes now
        
if __name__ == "__main__":
    # Clear memory first for a clean test
    conn = psycopg2.connect(host='localhost', port=5432, user='quant', password='quantpassword', database='market_data')
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("TRUNCATE pattern_embeddings;")
    cur.close()
    conn.close()
    
    train_memory()
    test_learning()
