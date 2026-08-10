import os
import logging
import psycopg2
import polars as pl
from kiteconnect import KiteConnect
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "quant")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "quantpassword")
DB_NAME = os.getenv("POSTGRES_DB", "market_data")

def evaluate_watchlist():
    # 1. Connect to DB
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname=DB_NAME)
        cur = conn.cursor()
    except Exception as e:
        logging.error(f"Failed to connect to DB: {e}")
        return

    # 2. Query WATCHING stocks
    cur.execute("SELECT id, symbol, added_date FROM supreme_watchlist WHERE status = 'WATCHING';")
    rows = cur.fetchall()
    
    if not rows:
        logging.info("Supreme Watchlist is empty. Nothing to evaluate.")
        cur.close()
        conn.close()
        return

    logging.info(f"Evaluating {len(rows)} stocks on the Supreme Watchlist...")

    # 3. Check Overall Market Health
    kite = KiteConnect(api_key=os.getenv("KITE_API_KEY", "").strip("'\""))
    kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN", "").strip("'\""))

    try:
        nifty_quote = kite.quote(["NSE:NIFTY 50"])
        data = nifty_quote.get("NSE:NIFTY 50", {})
        last_price = data.get("last_price", 1)
        net_change = data.get("net_change", -100)
        previous_close = last_price - net_change
        nifty_change_pct = (net_change / previous_close) * 100 if previous_close else -100
        
        if nifty_change_pct < -0.5:
            logging.warning(f"Market condition is RED (Nifty 50 change: {nifty_change_pct:.2f}%). Keeping watchlist intact.")
            cur.close()
            conn.close()
            return
        else:
            logging.info(f"Market condition is STABLE (Nifty 50 change: {nifty_change}%). Evaluating symbols...")
    except Exception as e:
        logging.error(f"Failed to fetch Nifty quote: {e}")
        return

    # 4. Fetch History for Averages
    watchlist_symbols = [r[1] for r in rows]
    parquet_path = "data/intraday_ohlcv.parquet"  # We'll use intraday for simplicity to check 20-EMA/SMA, or better, we fetch daily API
    
    from_date = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d")
    to_date = datetime.now().strftime("%Y-%m-%d")
    
    for row in rows:
        wid, symbol, added_date = row
        
        # Check Expiration (5 days)
        if (datetime.now(timezone.utc) - added_date).days >= 5:
            logging.info(f"{symbol} has been on the watchlist for >= 5 days. Expiring.")
            cur.execute("UPDATE supreme_watchlist SET status = 'EXPIRED', last_evaluated = CURRENT_TIMESTAMP WHERE id = %s", (wid,))
            continue
            
        try:
            # 5. Get Technicals
            quote = kite.quote([f"NSE:{symbol}"])
            if f"NSE:{symbol}" not in quote:
                continue
            
            data = quote[f"NSE:{symbol}"]
            close_price = data["last_price"]
            open_price = data["ohlc"]["open"]
            
            # Fetch just enough history for a 20-SMA
            hist = kite.historical_data(data["instrument_token"], from_date, to_date, "day")
            if len(hist) < 20:
                continue
            
            # Calculate 20-SMA
            closes = [r["close"] for r in hist[-20:]]
            sma_20 = sum(closes) / len(closes)
            
            logging.info(f"Evaluating {symbol}: Close={close_price}, Open={open_price}, SMA20={sma_20:.2f}")

            # 6. Evaluate
            if close_price > sma_20 and close_price > open_price:
                # Upgraded to EXECUTED!
                logging.info(f"🚀 {symbol} has confirmed strength! Upgrading to EXECUTED.")
                cur.execute("UPDATE supreme_watchlist SET status = 'EXECUTED', last_evaluated = CURRENT_TIMESTAMP WHERE id = %s", (wid,))
                
                # Insert into trade_events
                import uuid
                trade_id = str(uuid.uuid4())
                cur.execute("""
                    INSERT INTO trade_events (trade_id, ticker, status, price, notes)
                    VALUES (%s, %s, 'AMO_PLACED', %s, %s)
                """, (trade_id, symbol, close_price, "Activated from Supreme Watchlist"))
            
            elif close_price < sma_20 * 0.98: # Dropped 2% below SMA
                logging.info(f"❌ {symbol} broke support. Rejecting.")
                cur.execute("UPDATE supreme_watchlist SET status = 'REJECTED', last_evaluated = CURRENT_TIMESTAMP WHERE id = %s", (wid,))
            
            else:
                logging.info(f"⏳ {symbol} still hovering. Keeping on watchlist.")
                cur.execute("UPDATE supreme_watchlist SET last_evaluated = CURRENT_TIMESTAMP WHERE id = %s", (wid,))

        except Exception as e:
            logging.error(f"Error evaluating {symbol}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Watchlist evaluation complete.")

if __name__ == "__main__":
    evaluate_watchlist()
