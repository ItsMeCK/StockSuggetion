# scratch_test_multipliers.py

from core.sector_mapping import get_sector_for_symbol

# Hardcoded data based on today's trades
TODAY_TRADES = [
    {"symbol": "ZYDUSLIFE", "base_score": 92, "ltp": 1141, "vwap": 1130, "sector_ltp": 20500, "sector_close": 20400, "hour": 11},
    {"symbol": "PATANJALI", "base_score": 85, "ltp": 355.7, "vwap": 360, "sector_ltp": 55000, "sector_close": 55050, "hour": 11},
    {"symbol": "DRREDDY", "base_score": 70, "ltp": 1197, "vwap": 1190, "sector_ltp": 20500, "sector_close": 20400, "hour": 12},
    {"symbol": "PRESTIGE", "base_score": 75, "ltp": 1500, "vwap": 1520, "sector_ltp": 950, "sector_close": 960, "hour": 10},
]

print(f"{'='*50}")
print("🔍 SIMULATING HYBRID SCORING MATRIX FOR TODAY'S TRADES")
print(f"{'='*50}")

for trade in TODAY_TRADES:
    symbol = trade["symbol"]
    score = trade["base_score"]
    now_hour = trade["hour"]
    
    threshold = 70 if now_hour in [10, 11, 12] else 85
    
    vwap_multiplier = 1.0
    sector_multiplier = 1.0
    
    sector_index = get_sector_for_symbol(symbol)
    
    # 1. Simulate VWAP Check
    if trade["ltp"] < trade["vwap"]:
        vwap_multiplier = 0.8
        print(f"\n⚠️ {symbol}: Bled below VWAP (LTP: {trade['ltp']} < VWAP: {trade['vwap']}). Applying 0.8x penalty (Block-deal trap).")
    else:
        print(f"\n✅ {symbol}: Holding above VWAP. No penalty.")
        
    # 2. Simulate Sector Check
    if sector_index != "NIFTY 50":
        if trade["sector_ltp"] > trade["sector_close"]:
            sector_multiplier = 1.05
            print(f"✅ {symbol}: Sector Tailwind ({sector_index} is positive). Applying 1.05x boost.")
        else:
            print(f"📉 {symbol}: Sector Headwind ({sector_index} is negative). No boost.")
            
    final_score = score * vwap_multiplier * sector_multiplier
    
    print(f"[{symbol}] Base LLM: {score} | Final: {final_score:.1f} (Threshold needed: {threshold})")
    if final_score >= threshold:
        print(f"🟢 RESULT: TRADE EXECUTED")
    else:
        print(f"🔴 RESULT: REJECTED BY TECHNICAL MATRIX")
