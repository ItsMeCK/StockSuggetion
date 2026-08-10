import os
import polars as pl
from kiteconnect import KiteConnect
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time

load_dotenv()
kite = KiteConnect(api_key=os.getenv("KITE_API_KEY", "").strip("'\""))
kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN", "").strip("'\""))

print("Fetching FNO universe...")
instruments = kite.instruments("NFO")
fno_symbols = sorted(list(set([inst['name'] for inst in instruments if inst['instrument_type'] in ['CE', 'PE']])))

all_eq = kite.instruments("NSE")
fno_tokens = {inst['tradingsymbol']: inst['instrument_token'] for inst in all_eq if inst['tradingsymbol'] in fno_symbols}

from_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
to_date = datetime.now().strftime("%Y-%m-%d")

all_data = []
print(f"Fetching {len(fno_tokens)} tokens from {from_date} to {to_date}")
count = 0
for symbol, token in list(fno_tokens.items()):
    try:
        data = kite.historical_data(token, from_date, to_date, "day")
        for row in data:
            all_data.append({
                "time": row["date"].strftime("%Y-%m-%d"),
                "symbol": symbol,
                "open": row["open"],
                "close": row["close"],
                "volume": row["volume"]
            })
    except Exception as e:
        print(f"Error for {symbol}: {e}")
    
    # Respect Zerodha rate limit (3 requests per second)
    time.sleep(0.35)

print(f"Fetched {len(all_data)} historical daily rows.")
if len(all_data) == 0:
    exit(1)

df = pl.DataFrame(all_data)
df = df.sort(["symbol", "time"])

df = df.with_columns([
    pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
    pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
    pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
])
df = df.with_columns([
    ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
    (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
])

setups = df.filter(
    (pl.col("close") > pl.col("sma_20")) & 
    (pl.col("close") > pl.col("open")) &  # Must be a bullish green candle
    (pl.col("bbw") < 0.22) & 
    (pl.col("vol_surge") > 2.5) & 
    (pl.col("time") == to_date)
).sort("vol_surge", descending=True)

symbols = setups["symbol"].to_list()
print(f"\n--- DAILY FNO BREAKOUTS FOR {to_date} ---")
print(f"🎯 Math Engine found {len(symbols)} Daily Breakouts: {symbols}")

if symbols:
    from agents.llm_ranking_agent import LLMRankingAgent
    agent = LLMRankingAgent()
    rankings = agent.rank_trades(symbols)
    
    print("\n🚀 FINAL LLM EXECUTION SIGNALS 🚀")
    if rankings:
        import psycopg2
        
        DB_HOST = os.getenv("DB_HOST", "localhost")
        DB_PORT = os.getenv("DB_PORT", "5432")
        DB_USER = os.getenv("POSTGRES_USER", "quant")
        DB_PASS = os.getenv("POSTGRES_PASSWORD", "quantpassword")
        DB_NAME = os.getenv("POSTGRES_DB", "market_data")
        
        try:
            conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname=DB_NAME)
            cur = conn.cursor()
            
            for i, rank in enumerate(rankings[:2]):
                print(f"Rank {i+1}: {rank['symbol']} | Score: {rank['conviction_score']}")
                print(f"Catalyst: {rank['catalyst_summary']}\n")
                
                # Insert into Supreme Watchlist
                cur.execute("""
                    INSERT INTO supreme_watchlist (symbol, catalyst_summary, conviction_score, status)
                    VALUES (%s, %s, %s, 'WATCHING')
                """, (rank['symbol'], rank['catalyst_summary'], rank['conviction_score']))
            
            conn.commit()
            cur.close()
            conn.close()
            print("✅ Successfully added candidates to the Supreme Watchlist.")
            
        except Exception as e:
            print(f"❌ Failed to insert into Watchlist: {e}")
            
    else:
        print("LLM rejected all candidates.")
