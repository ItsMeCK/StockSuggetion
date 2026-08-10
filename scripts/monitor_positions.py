import os
import json
from datetime import datetime
from dotenv import load_dotenv

from core.db_manager import get_all_active_positions, update_position_sl, close_position
from core.live_trading import get_kite_data_client, get_kite_exec_client

def monitor_positions():
    """
    Runs continuously (e.g. every minute via cron).
    1. Checks if exchange hit the SL order.
    2. Modifies SL order (Trailing SL) if profit target reached.
    """
    load_dotenv()
    live_buy_flag = os.getenv("LIVE_BUY", "False")
    try:
        kite_data = get_kite_data_client()
        kite_exec = get_kite_exec_client()
    except Exception as e:
        print(f"Error initializing kite clients: {e}")
        return
        
    positions = get_all_active_positions()
    
    if not positions:
        return
        
    # Fetch all current orders from exchange to check status
    try:
        exchange_orders = kite_exec.orders()
        order_dict = {o['order_id']: o for o in exchange_orders}
    except Exception as e:
        print(f"Failed to fetch exchange orders via Exec account: {e}")
        return
        
    for pos in positions:
        order_id = pos["order_id"]
        sl_order_id = pos["sl_order_id"]
        opt_symbol = pos["option_symbol"]
        
        # 1. Check if SL was hit on the exchange
        if sl_order_id in order_dict:
            sl_status = order_dict[sl_order_id]['status']
            if sl_status == "COMPLETE":
                print(f"[{datetime.now()}] 🛑 SL Order {sl_order_id} is COMPLETE for {opt_symbol}!")
                exit_price = order_dict[sl_order_id]['average_price']
                pnl_pct = ((exit_price - pos["entry_premium"]) / pos["entry_premium"]) * 100
                close_position(pos, exit_price, pnl_pct, "Exchange SL Hit")
                continue
                
        # 2. Fetch live quote to check for trailing SL
        try:
            quote = kite_data.quote([f"NFO:{opt_symbol}"])
            if f"NFO:{opt_symbol}" not in quote:
                continue
            ltp = quote[f"NFO:{opt_symbol}"]["last_price"]
        except Exception as e:
            print(f"Error fetching LTP for {opt_symbol} via Data account: {e}")
            continue
            
        p_entry = pos["entry_premium"]
        highest_high = max(pos["highest_high"], ltp)
        current_sl = pos["current_sl"]
        target_activation = p_entry * 1.50
        trailing_active = pos["trailing_active"]
        
        sl_updated = False
        
        # Trailing SL Logic
        if highest_high >= target_activation:
            trailing_active = True
            new_sl = round(highest_high - (highest_high * 0.10), 1)
            if new_sl > current_sl:
                current_sl = new_sl
                sl_updated = True
                
        # Update Exchange SL Order if trailed
        if sl_updated:
            print(f"[{datetime.now()}] 📈 Modifying Exchange SL for {opt_symbol} to {current_sl}")
            if live_buy_flag.lower() == "true":
                try:
                    sl_limit_price = round(current_sl * 0.95, 1)
                    kite_exec.modify_order(
                        variety=kite_exec.VARIETY_REGULAR,
                        order_id=sl_order_id,
                        order_type=kite_exec.ORDER_TYPE_SL,
                        trigger_price=current_sl,
                        price=sl_limit_price
                    )
                    print(f"✅ SL Modified successfully on Exchange!")
                except Exception as e:
                    print(f"❌ Failed to modify Exchange SL: {e}")
            else:
                print(f"Paper mode: Modified simulated SL to {current_sl}")
                
        # Update DB if SL or high changed
        if highest_high > pos["highest_high"] or sl_updated:
            update_position_sl(pos['order_id'], highest_high, current_sl, trailing_active)
            
        print(f"[{datetime.now()}] 🟢 {opt_symbol} | LTP: {ltp} | Entry: {p_entry} | SL: {current_sl:.2f} | Trail Active: {trailing_active}")

if __name__ == "__main__":
    monitor_positions()
