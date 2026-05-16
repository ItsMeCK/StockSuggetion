import os
import polars as pl
from dotenv import load_dotenv
from kiteconnect import KiteConnect
from pipeline.screener import SovereignScreener
from datetime import datetime, time, timedelta

load_dotenv()

def check_regime_and_filters():
    screener = SovereignScreener()
    api_key = os.getenv("KITE_API_KEY")
    access_token = os.getenv("KITE_ACCESS_TOKEN").strip("'")
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    
    # 1. Check Nifty Regime
    instruments = kite.instruments("NSE")
    nifty_token = next(ins['instrument_token'] for ins in instruments if ins['tradingsymbol'] == 'NIFTY 50')
    
    today = datetime.now().date()
    # Fetch last 30 days of Nifty
    records = kite.historical_data(nifty_token, today - timedelta(days=40), today, "day")
    
    df_nifty = pl.DataFrame(records)
    df_nifty = df_nifty.with_columns(pl.col("close").rolling_mean(window_size=20).alias("sma_20"))
    
    latest = df_nifty.tail(1).to_dicts()[0]
    close = latest["close"]
    sma_20 = latest["sma_20"]
    
    regime = "BEARISH" if close < sma_20 else ("BULLISH" if close > sma_20 * 1.01 else "NEUTRAL")
    
    print(f"--- NIFTY 50 REGIME CHECK ---")
    print(f"Close: {close:.2f}")
    print(f"SMA-20: {sma_20:.2f}")
    print(f"Regime: {regime}")
    
    if regime == "BEARISH":
        print("🚨 REJECTED: Market is in BEARISH regime. System is hard-coded to stay in cash.")
    
    # 2. Check Top Gainers for "Over-extension"
    # User says market went up 300 pts. Let's see top movers.
    with open("daily_scan_list.csv", "r") as f:
        symbols = [line.strip() for line in f.readlines()[1:] if line.strip()]
    
    print(f"\n--- CHECKING TOP MOVERS FOR OVER-EXTENSION ---")
    movers = []
    symbol_map = {ins['tradingsymbol']: ins['instrument_token'] for ins in instruments}
    
    for symbol in symbols[:50]: # Check first 50
        token = symbol_map.get(symbol)
        if not token: continue
        try:
            # Fetch last 2 days of daily data
            d_records = kite.historical_data(token, today - timedelta(days=200), today, "day")
            if len(d_records) < 50: continue
            
            # Calculate SMA-50 and extension
            df_s = pl.DataFrame(d_records)
            df_s = df_s.with_columns(pl.col("close").rolling_mean(window_size=50).alias("sma_50"))
            
            latest_s = df_s.tail(1).to_dicts()[0]
            price = latest_s["close"]
            sma_50 = latest_s["sma_50"]
            ext = ((price - sma_50) / sma_50) * 100
            roc_1 = ((price - d_records[-2]["close"]) / d_records[-2]["close"]) * 100
            
            movers.append({"symbol": symbol, "price": price, "ext": ext, "roc_1": roc_1})
        except: continue

    movers.sort(key=lambda x: x['roc_1'], reverse=True)
    print("Top 10 Movers Today:")
    for m in movers[:10]:
        status = "REJECTED (Over-extended > 15%)" if m['ext'] > 15 else ("REJECTED (Gap-up > 4.5%)" if m['roc_1'] > 4.5 else "POTENTIAL")
        print(f"- {m['symbol']}: +{m['roc_1']:.2f}% | Extension: {m['ext']:.2f}% | Status: {status}")

if __name__ == "__main__":
    check_regime_and_filters()
