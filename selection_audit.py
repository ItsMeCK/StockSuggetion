import json
import glob
import os
import psycopg2
from dotenv import load_dotenv
from kiteconnect import KiteConnect

def get_db_connection():
    load_dotenv()
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'quant'),
        password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
        dbname=os.getenv('POSTGRES_DB', 'market_data')
    )

def run_audit():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Load Kite for live prices
    load_dotenv()
    kite = KiteConnect(api_key=os.getenv('KITE_API_KEY'))
    kite.set_access_token(os.getenv('KITE_ACCESS_TOKEN'))

    history_files = glob.glob("run_history/run_202605*.json")
    # Group by date and take the latest run per day
    daily_runs = {}
    for f in history_files:
        date = os.path.basename(f).split('_')[1] # YYYYMMDD
        if date not in daily_runs or f > daily_runs[date]:
            daily_runs[date] = f
    
    sorted_files = sorted(daily_runs.values())
    
    results = {
        "Method A: All Signals": {"count": 0, "wins": 0, "total_pnl": 0.0},
        "Method B: Top 3 per Day": {"count": 0, "wins": 0, "total_pnl": 0.0},
        "Method C: Conviction > 135": {"count": 0, "wins": 0, "total_pnl": 0.0},
        "Method D: Top 3 + Conv > 135": {"count": 0, "wins": 0, "total_pnl": 0.0}
    }

    for file_path in sorted_files:
        print(f"Auditing unique daily run: {file_path}")
        with open(file_path, "r") as f:
            data = json.load(f)
        
        date_str = data['timestamp'][:10]
        approved = data.get("approved_allocations", {})
        if not approved: continue
        
        # Sort by conviction score for Top 3
        sorted_approved = sorted(approved.items(), key=lambda x: x[1].get('conviction_score', 0), reverse=True)
        
        # Fetch live prices for all
        symbols = [f"NSE:{s}" for s in approved.keys()]
        quotes = kite.quote(symbols)
        
        for i, (symbol, details) in enumerate(sorted_approved):
            # Find next-day open for this signal
            cur.execute("""
                SELECT open FROM daily_ohlcv 
                WHERE symbol = %s AND time > %s 
                ORDER BY time ASC LIMIT 1
            """, (symbol, date_str))
            res = cur.fetchone()
            # If we don't have DB open price (for today), we MUST get it from Kite ohlc
            buy_price = float(res[0]) if res else 0.0
            kite_sym = f"NSE:{symbol}"
            if buy_price == 0 and kite_sym in quotes:
                buy_price = quotes[kite_sym].get('ohlc', {}).get('open', 0.0)
            
            if buy_price == 0:
                buy_price = details.get('entry', 0.0)
            
            live_price = quotes.get(kite_sym, {}).get('last_price', 0.0)
            
            if buy_price > 0 and live_price > 0:
                pnl = ((live_price - buy_price) / buy_price) * 100
                is_win = pnl > 0
                
                score = details.get('conviction_score', 0)
                
                # Apply Methodologies
                # A: All
                results["Method A: All Signals"]["count"] += 1
                results["Method A: All Signals"]["total_pnl"] += pnl
                if is_win: results["Method A: All Signals"]["wins"] += 1
                
                # B: Top 3
                if i < 3:
                    results["Method B: Top 3 per Day"]["count"] += 1
                    results["Method B: Top 3 per Day"]["total_pnl"] += pnl
                    if is_win: results["Method B: Top 3 per Day"]["wins"] += 1
                
                # C: Conviction > 135
                if score >= 135:
                    results["Method C: Conviction > 135"]["count"] += 1
                    results["Method C: Conviction > 135"]["total_pnl"] += pnl
                    if is_win: results["Method C: Conviction > 135"]["wins"] += 1
                    
                # D: Top 3 + Conv > 135
                if i < 3 and score >= 135:
                    results["Method D: Top 3 + Conv > 135"]["count"] += 1
                    results["Method D: Top 3 + Conv > 135"]["total_pnl"] += pnl
                    if is_win: results["Method D: Top 3 + Conv > 135"]["wins"] += 1

    cur.close()
    conn.close()
    
    print("\n--- METHODOLOGY AUDIT RESULTS (MAY 4-6) ---")
    print(f"{'Methodology':<30} | {'Signals':<8} | {'Win Rate':<10} | {'Avg P&L':<10}")
    print("-" * 65)
    for name, stats in results.items():
        win_rate = (stats["wins"] / stats["count"] * 100) if stats["count"] > 0 else 0
        avg_pnl = (stats["total_pnl"] / stats["count"]) if stats["count"] > 0 else 0
        print(f"{name:<30} | {stats['count']:<8} | {win_rate:>8.1f}% | {avg_pnl:>8.2f}%")

if __name__ == "__main__":
    run_audit()
