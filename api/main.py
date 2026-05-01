import os
import json
import logging
import polars as pl
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
from dotenv import load_dotenv

# Load environment
load_dotenv()

app = FastAPI(title="Midnight Sovereign Terminal API")

# Enable CORS for the React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Use absolute path for robustness
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HISTORY_DIR = os.path.join(BASE_DIR, "run_history")
DIAGNOSTIC_FILE = os.path.join(BASE_DIR, "screening_diagnostics.csv")

def get_latest_run():
    if not os.path.exists(HISTORY_DIR):
        return None
    files = [f for f in os.listdir(HISTORY_DIR) if f.endswith(".json")]
    if not files:
        return None
    files.sort(reverse=True)
    with open(os.path.join(HISTORY_DIR, files[0]), "r") as f:
        return json.load(f)

@app.get("/status")
def get_status():
    latest = get_latest_run()
    if not latest:
        return {"status": "No runs recorded", "regime": "UNKNOWN"}
    return {
        "status": "ONLINE",
        "regime": latest.get("macro_regime"),
        "last_run": latest.get("timestamp"),
        "last_gear_2": latest.get("timestamp"),
        "candidate_count": len(latest.get("candidates", []))
    }

@app.get("/candidates")
def get_candidates():
    latest = get_latest_run()
    if not latest:
        return []
        
    candidates = latest.get("candidates", [])
    conviction_scores = latest.get("conviction_scores", {})
    base_scores = latest.get("base_scores", {})
    
    enriched = []
    df = None
    if os.path.exists(DIAGNOSTIC_FILE):
        df = pl.read_csv(DIAGNOSTIC_FILE)
        
    for sym in candidates:
        score = conviction_scores.get(sym, 0.0)
        base = base_scores.get(sym, 0.0)
        
        data = {
            "symbol": sym,
            "score": score,
            "base_score": base,
            "gear_2_approved": latest.get("entry_trigger_results", {}).get(sym, {}).get("approved", False),
            "gear_2_rejection": latest.get("entry_trigger_results", {}).get(sym, {}).get("rejection_reason", "None")
        }
        
        if df is not None:
            stock_data = df.filter(pl.col("symbol") == sym).to_dicts()
            if stock_data:
                data.update(stock_data[0])
                
        enriched.append(data)
        
    # Sort descending by score
    enriched.sort(key=lambda x: x["score"], reverse=True)
    return enriched

import psycopg2

@app.get("/portfolio")
def get_portfolio_ledger():
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "quant")
    password = os.getenv("POSTGRES_PASSWORD", "quantpassword")
    db_name = os.getenv("POSTGRES_DB", "market_data")

    try:
        conn = psycopg2.connect(
            host=host, port=port, user=user, password=password, dbname=db_name
        )
        cur = conn.cursor()
        # Get latest status per trade_id
        cur.execute("""
            SELECT DISTINCT ON (trade_id) 
                trade_id, ticker, status, price, market_time, system_time, notes 
            FROM trade_events 
            ORDER BY trade_id, system_time DESC;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        portfolio = []
        for r in rows:
            portfolio.append({
                "trade_id": str(r[0]),
                "ticker": r[1],
                "status": r[2],
                "price": float(r[3]) if r[3] else None,
                "market_time": r[4].isoformat() if r[4] else None,
                "system_time": r[5].isoformat(),
                "notes": r[6]
            })
        return portfolio
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/ledger/{trade_id}")
def get_trade_ledger(trade_id: str):
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "quant")
    password = os.getenv("POSTGRES_PASSWORD", "quantpassword")
    db_name = os.getenv("POSTGRES_DB", "market_data")

    try:
        conn = psycopg2.connect(
            host=host, port=port, user=user, password=password, dbname=db_name
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT status, price, market_time, system_time, notes 
            FROM trade_events 
            WHERE trade_id = %s 
            ORDER BY system_time ASC;
        """, (trade_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        events = []
        for r in rows:
            events.append({
                "status": r[0],
                "price": float(r[1]) if r[1] else None,
                "market_time": r[2].isoformat() if r[2] else None,
                "system_time": r[3].isoformat(),
                "notes": r[4]
            })
        return events
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/diagnostics")
def get_all_diagnostics():
    if not os.path.exists(DIAGNOSTIC_FILE):
        raise HTTPException(status_code=404, detail="Diagnostic file not found")
    df = pl.read_csv(DIAGNOSTIC_FILE)
    return df.to_dicts()

# --- Kite Live Price Endpoint ---
from kiteconnect import KiteConnect

api_key = os.getenv("KITE_API_KEY")
access_token = os.getenv("KITE_ACCESS_TOKEN")
kite = None

if api_key and access_token:
    kite = KiteConnect(api_key=api_key)
    try:
        kite.set_access_token(access_token)
        logging.info("KiteConnect initialized for live quotes.")
    except Exception as e:
        logging.error(f"Failed to set kite access token: {e}")

@app.get("/live-price/{symbol}")
def get_live_price(symbol: str):
    if not kite:
        raise HTTPException(status_code=500, detail="KiteConnect not initialized")
    
    exchange_symbol = f"NSE:{symbol}"
    try:
        quote = kite.quote([exchange_symbol])
        if exchange_symbol in quote:
            return {
                "symbol": symbol, 
                "live_price": quote[exchange_symbol]["last_price"],
                "net_change": quote[exchange_symbol].get("net_change", 0),
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=404, detail="Symbol not found in live feed")
    except Exception as e:
        import traceback
        logging.error(f"Live price error: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/live-prices")
def get_all_live_prices():
    if not kite:
        raise HTTPException(status_code=500, detail="KiteConnect not initialized")
    
    latest = get_latest_run()
    if not latest:
        return {}
        
    candidates = latest.get("candidates", [])
    if not candidates:
        return {}
        
    # Zerodha allows fetching multiple quotes at once
    exchange_symbols = [f"NSE:{sym}" for sym in candidates]
    
    try:
        # Chunk if there are more than 500 candidates (Kite quote limit per call is typically 500)
        results = {}
        for i in range(0, len(exchange_symbols), 500):
            chunk = exchange_symbols[i:i+500]
            quote = kite.quote(chunk)
            for k, v in quote.items():
                sym = k.replace("NSE:", "")
                results[sym] = {
                    "symbol": sym,
                    "live_price": v["last_price"],
                    "net_change": v.get("net_change", 0),
                    "timestamp": datetime.now().isoformat()
                }
        return results
    except Exception as e:
        import traceback
        logging.error(f"Bulk live price error: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
