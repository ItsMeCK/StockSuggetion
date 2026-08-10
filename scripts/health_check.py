import os
from datetime import datetime
from dotenv import load_dotenv
from core.db_manager import get_all_active_positions, close_position
from core.live_trading import get_kite_exec_client

def run_health_check():
    print(f"\n[{datetime.now()}] 🏥 RUNNING SYSTEM HEALTH CHECK")
    
    # 1. API Ping & Authentication Check
    try:
        kite = get_kite_exec_client()
        profile = kite.profile()
        print(f"✅ Kite API is connected (User: {profile.get('user_name')})")
    except Exception as e:
        print(f"❌ CRITICAL ERROR: Kite API authentication failed: {e}")
        print("Please refresh EXEC_KITE_ACCESS_TOKEN!")
        return # Cannot proceed if API is down
        
    # 2. Fetch Live Kite Positions
    try:
        live_positions = kite.positions().get("net", [])
        live_open_symbols = []
        for pos in live_positions:
            # If quantity is not 0, it means it's an open position
            if pos["quantity"] != 0:
                live_open_symbols.append(pos["tradingsymbol"])
        print(f"📊 Live Open Positions on Zerodha: {live_open_symbols}")
    except Exception as e:
        print(f"❌ Failed to fetch live positions from Kite: {e}")
        return
        
    # 3. Database Reconciliation
    try:
        db_positions = get_all_active_positions()
        print(f"📂 Active Positions in DB: {[p['option_symbol'] for p in db_positions]}")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return
        
    live_buy_flag = os.getenv("LIVE_BUY", "False").lower() == "true"
    
    for db_pos in db_positions:
        opt_symbol = db_pos["option_symbol"]
        
        # If in LIVE_BUY mode, check if the position physically exists in the broker account
        if live_buy_flag:
            if opt_symbol not in live_open_symbols:
                print(f"⚠️ DISCREPANCY DETECTED! {opt_symbol} is active in DB but NOT open on Zerodha.")
                print(f"🔧 Self-Healing: Closing {opt_symbol} in Database to stop trailing monitor.")
                
                # Fetch last known price or use spot to close
                try:
                    quotes = kite.quote([f"NFO:{opt_symbol}"])
                    ltp = quotes[f"NFO:{opt_symbol}"]["last_price"] if f"NFO:{opt_symbol}" in quotes else db_pos["highest_high"]
                except:
                    ltp = db_pos["highest_high"]
                    
                pnl_pct = ((ltp - db_pos["entry_premium"]) / db_pos["entry_premium"]) * 100
                close_position(db_pos, ltp, pnl_pct, "Health Check Reconciliation (Missing from Broker)")
        else:
            # In paper mode, we can't reconcile with actual broker positions, just check for staleness
            entry_time = db_pos.get("entry_time")
            if entry_time:
                hours_open = (datetime.now() - entry_time).total_seconds() / 3600
                if hours_open > 24:
                    print(f"⚠️ STALE PAPER TRADE DETECTED: {opt_symbol} has been open for > 24 hours.")
                    
    print("✅ Health Check completed.")

if __name__ == "__main__":
    run_health_check()
