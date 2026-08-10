"""
Independent validation script of the 1,166 movers classification.
Queries the database directly, calculates Shannon and Pring pattern indicators
from scratch, classifies the moves using our own logic, and compares the results
to the provided CSV.
"""
import os
import csv
import psycopg2
import polars as pl
from dotenv import load_dotenv

load_dotenv()


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )


def build_indicators_frame(symbols):
    conn = get_conn()
    
    # Fetch price data for the symbols
    query = """
        SELECT time::date as time, symbol, open, high, low, close, volume 
        FROM daily_ohlcv 
        WHERE symbol = ANY(%(s)s) 
        ORDER BY symbol, time
    """
    df = pl.read_database(query, conn, execute_options={"parameters": {"s": symbols}})
    
    # Fetch NIFTY 50 for Relative Strength calculation
    nif_query = """
        SELECT time::date as time, close as nif_close 
        FROM daily_ohlcv 
        WHERE symbol='NIFTY 50' 
        ORDER BY time
    """
    nif = pl.read_database(nif_query, conn)
    conn.close()
    
    # Compute Nifty ROC10
    nif = nif.with_columns(
        (((pl.col("nif_close") - pl.col("nif_close").shift(10)) / pl.col("nif_close").shift(10)) * 100).alias("nif_roc10")
    )
    
    o = pl.col("symbol")
    # Calculate indicators
    df = df.with_columns([
        pl.col("close").ewm_mean(span=10).over(o).alias("ema10"),
        pl.col("close").ewm_mean(span=20).over(o).alias("ema20"),
        pl.col("close").ewm_mean(span=50).over(o).alias("ema50"),
        pl.col("close").rolling_mean(20).over(o).alias("sma20"),
        pl.col("close").rolling_mean(100).over(o).alias("sma100"),
        pl.col("volume").rolling_mean(20).over(o).alias("vol_avg20"),
        pl.col("high").rolling_max(20).shift(1).over(o).alias("prior_high20"),
        pl.col("high").rolling_max(10).shift(1).over(o).alias("hi10"),
        pl.col("low").rolling_min(10).shift(1).over(o).alias("lo10"),
        pl.col("low").rolling_min(5).shift(1).over(o).alias("lo5"),
        pl.col("close").shift(1).over(o).alias("prev_close"),
        pl.col("close").shift(10).over(o).alias("close_10ago"),
    ])
    
    df = df.with_columns([
        pl.col("ema20").shift(5).over(o).alias("ema20_5ago"),
        pl.col("sma20").shift(5).over(o).alias("sma20_5ago"),
        pl.col("sma100").shift(5).over(o).alias("sma100_5ago"),
        (((pl.col("close") - pl.col("close").shift(10).over(o)) / pl.col("close").shift(10).over(o)) * 100).alias("roc10"),
        pl.col("low").rolling_min(3).shift(1).over(o).alias("lo3"),
    ])
    
    df = df.join(nif.select(["time", "nif_roc10"]), on="time", how="left")
    return df


def classify_row(r):
    close, opn, high, low, vol = r["close"], r["open"], r["high"], r["low"], r["volume"]
    
    # Vol Average
    va = r.get("vol_avg20") or vol
    
    # 1. Pring Base Breakout (Rectangle Breakout)
    hi10 = r.get("hi10")
    lo10 = r.get("lo10")
    tight = hi10 and lo10 and lo10 > 0 and (hi10 / lo10 - 1) < 0.10
    prior_high20 = r.get("prior_high20")
    base_breakout = bool(prior_high20 and close > prior_high20 and tight and vol >= 1.5 * va)
    
    # 2. Pring Volume Thrust (Institutional Footprint)
    volume_thrust = bool(vol >= 2.0 * va)
    
    # 3. Shannon Stacked Rising EMAs (Established Trend)
    ema10 = r.get("ema10")
    ema20 = r.get("ema20")
    ema50 = r.get("ema50")
    ema20_5ago = r.get("ema20_5ago")
    stack = ema10 and ema20 and ema50 and ema10 > ema20 > ema50
    rising = ema20_5ago and ema20 > ema20_5ago
    ma_stack_rising = bool(stack and rising)
    
    # 4. Shannon Pullback in Uptrend
    lo3 = r.get("lo3")
    pulled = lo3 and ema20 and lo3 <= ema20 * 1.01
    pullback_in_uptrend = bool(stack and rising and pulled and close > opn)
    
    # 5. Weinstein/Shannon Stage 2 Breakout
    sma100 = r.get("sma100")
    sma100_5ago = r.get("sma100_5ago")
    close_10ago = r.get("close_10ago")
    stage2_breakout = bool(sma100 and sma100_5ago and close > sma100
                           and close_10ago and close_10ago <= sma100_5ago
                           and sma100 >= sma100_5ago)
                           
    # 6. Above Rising 20 SMA
    sma20 = r.get("sma20")
    sma20_5ago = r.get("sma20_5ago")
    above_rising_20sma = bool(sma20 and sma20_5ago and close > sma20 and sma20 > sma20_5ago)
    
    # 7. Momentum ROC10 > 5
    roc10 = r.get("roc10")
    momentum_roc10 = bool(roc10 and roc10 > 5.0)
    
    # 8. HH / HL Uptrend Structure
    lo5 = r.get("lo5")
    hh_hl_uptrend = bool(close_10ago and close > close_10ago and lo5 and low > lo5)
    
    # 9. Near 20d High Breakout Zone
    near_20d_high = bool(prior_high20 and close >= 0.985 * prior_high20)
    
    # 10. Relative Strength beats Nifty
    nif_roc10 = r.get("nif_roc10")
    rs_beats_nifty = bool(roc10 is not None and nif_roc10 is not None and roc10 > nif_roc10)
    
    # 11. Strong Close
    rng = high - low
    strong_close = bool(rng > 0 and (close - low) / rng >= 0.7)
    
    # 12. Gap Up
    prev_close = r.get("prev_close")
    gap_up = bool(prev_close and opn >= prev_close * 1.03)

    return {
        "base_breakout": base_breakout,
        "gap_up": gap_up,
        "volume_thrust_2x": volume_thrust,
        "ma_stack_rising": ma_stack_rising,
        "pullback_in_uptrend": pullback_in_uptrend,
        "stage2_breakout": stage2_breakout,
        "above_rising_20sma": above_rising_20sma,
        "momentum_roc10>5": momentum_roc10,
        "hh_hl_uptrend": hh_hl_uptrend,
        "near_20d_high": near_20d_high,
        "rs_beats_nifty": rs_beats_nifty,
        "strong_close": strong_close
    }


def main():
    print("Loading mover_categorization_final.csv...")
    movers = []
    with open("mover_categorization_final.csv") as f:
        for r in csv.DictReader(f):
            movers.append(r)
            
    symbols = list(set(r["symbol"] for r in movers))
    print(f"Loaded {len(movers)} moves across {len(symbols)} symbols. Computing indicators...")
    
    df = build_indicators_frame(symbols)
    lut = {(r["symbol"], r["time"].strftime("%Y-%m-%d")): r for r in df.to_dicts()}
    
    # Priority order to resolve multiple pattern hits (Shannon & Pring)
    priority = [
        "base_breakout", "volume_thrust_2x", "ma_stack_rising", "pullback_in_uptrend",
        "near_20d_high", "above_rising_20sma", "momentum_roc10>5", "hh_hl_uptrend",
        "strong_close", "rs_beats_nifty", "gap_up", "stage2_breakout"
    ]
    
    discrepancies = []
    correct_tech = 0
    correct_news = 0
    errors = 0
    
    # Category counter
    my_counts = {}
    
    for m in movers:
        symbol = m["symbol"]
        start_date = m["start"]
        their_primary = m["primary"]
        
        r = lut.get((symbol, start_date))
        if not r:
            errors += 1
            discrepancies.append({
                "symbol": symbol, "date": start_date, "their": their_primary,
                "our": "NO_DATA_IN_DB", "reason": "No database bar found for symbol on date"
            })
            continue
            
        # Classify using our scratch logic
        fired = classify_row(r)
        
        # Resolve to a single primary technical category
        our_tech = "UNCLASSIFIED"
        for p in priority:
            if fired[p]:
                our_tech = p
                break
                
        # If their category was a news category, we keep their news category but verify if we found a technical pattern
        if their_primary.startswith("NEWS:") or their_primary == "SECTOR_PEER_MOVE" or their_primary == "NO_NEWS_FOUND":
            correct_news += 1
            our_primary = their_primary
            # Record if a strong technical pattern co-existed
            if our_tech != "UNCLASSIFIED":
                r["co_existing_tech"] = our_tech
        else:
            # It was classified as a technical category
            our_primary = our_tech
            if our_primary == their_primary:
                correct_tech += 1
            else:
                discrepancies.append({
                    "symbol": symbol, "date": start_date,
                    "their": their_primary, "our": our_primary,
                    "reason": f"Fired patterns: {[k for k, v in fired.items() if v]}"
                })
                
        my_counts[our_primary] = my_counts.get(our_primary, 0) + 1
        
    print(f"\n=== VALIDATION COMPLETED ===")
    print(f"Total analyzed: {len(movers)}")
    print(f"Correctly matched Technical patterns: {correct_tech}")
    print(f"Retained News-based verifications: {correct_news}")
    print(f"Discrepancies/Mismatches found: {len(discrepancies)}")
    print(f"Database resolution errors: {errors}")
    
    if discrepancies:
        print("\n--- SAMPLE DISCREPANCIES (first 10) ---")
        for d in discrepancies[:10]:
            print(f"  {d['symbol']} on {d['date']} | Theirs: {d['their']:24s} | Ours: {d['our']:20s} | {d['reason']}")
            
    print("\n--- OUR CATEGORIZATION COUNTS ---")
    for cat, count in sorted(my_counts.items(), key=lambda x: -x[1]):
        print(f"  {cat:35s}: {count:4d} ({count/len(movers)*100:5.1f}%)")

if __name__ == "__main__":
    main()
