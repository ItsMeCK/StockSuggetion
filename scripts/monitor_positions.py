import os
import json
from datetime import datetime
from dotenv import load_dotenv

from core.db_manager import get_all_active_positions, update_position_sl, close_position, update_sl_order_id, update_highest_oi
from core.live_trading import get_kite_data_client, get_kite_exec_client
from alerts.email_notifier import SovereignEmailer

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
        emailer = SovereignEmailer()
    except Exception as e:
        print(f"Error initializing clients: {e}")
        return
        
    positions = get_all_active_positions()
    
    if not positions:
        return
        
    # Fetch all current orders and positions from exchange
    try:
        exchange_orders = kite_exec.orders()
        order_dict = {o['order_id']: o for o in exchange_orders}
        exchange_positions = kite_exec.positions()
        net_positions = {p['tradingsymbol']: p['quantity'] for p in exchange_positions.get('net', [])}
    except Exception as e:
        print(f"Failed to fetch exchange data via Exec account: {e}")
        return
        
    for pos in positions:
        order_id = pos["order_id"]
        sl_order_id = pos["sl_order_id"]
        opt_symbol = pos["option_symbol"]
        
        # Broker Reconciliation: Check if manually squared off
        qty_held = net_positions.get(opt_symbol, 0)
        if qty_held <= 0:
            print(f"[{datetime.now()}] 👁️ Broker Reconciliation: {opt_symbol} was manually exited (qty={qty_held}). Marking as CLOSED.")
            if sl_order_id and sl_order_id != "FAILED":
                try:
                    kite_exec.cancel_order(variety=kite_exec.VARIETY_REGULAR, order_id=sl_order_id)
                    print(f"Canceled resting SL {sl_order_id} due to manual exit.")
                except Exception as ce:
                    pass
            close_position(pos, pos["entry_premium"], 0.0, "Manual Exit Detected")
            continue
            
        # 0. Check if SL is missing/FAILED and Buy is COMPLETE
        if sl_order_id == "FAILED":
            if order_id in order_dict and order_dict[order_id]['status'] == "COMPLETE":
                print(f"[{datetime.now()}] 🔄 Buy Order {order_id} is COMPLETE. Placing missing SL for {opt_symbol}...")
                if live_buy_flag.lower() == "true":
                    try:
                        sl_limit_price = round(pos["current_sl"] * 0.95, 1)
                        new_sl_id = kite_exec.place_order(
                            variety=kite_exec.VARIETY_REGULAR,
                            exchange=kite_exec.EXCHANGE_NFO,
                            tradingsymbol=opt_symbol,
                            transaction_type=kite_exec.TRANSACTION_TYPE_SELL,
                            quantity=pos["qty"],
                            product=kite_exec.PRODUCT_NRML,
                            order_type=kite_exec.ORDER_TYPE_SL,
                            trigger_price=pos["current_sl"],
                            price=sl_limit_price
                        )
                        print(f"✅ Recovery SL Order Placed! ID: {new_sl_id}")
                        update_sl_order_id(order_id, new_sl_order_id=new_sl_id)
                        pos["sl_order_id"] = new_sl_id
                        sl_order_id = new_sl_id
                    except Exception as e:
                        print(f"❌ Failed to place recovery SL for {opt_symbol}: {e}")
                        emailer.send_live_alert(
                            f"⚠️ URGENT: Margin Failure for {opt_symbol}",
                            f"<p>The engine tried to place the Recovery Stop-Loss for <b>{opt_symbol}</b> but it was blocked by Zerodha.</p>"
                            f"<p><b>Reason:</b> {str(e)}</p>"
                            f"<p style='color: #f85149;'><b>ACTION REQUIRED:</b> Open Zerodha and manually place a Stop-Loss or square off the position immediately!</p>"
                        )
        
        # 1. Check if SL was hit on the exchange
        if sl_order_id in order_dict:
            sl_status = order_dict[sl_order_id]['status']
            if sl_status == "COMPLETE":
                print(f"[{datetime.now()}] 🛑 SL Order {sl_order_id} is COMPLETE for {opt_symbol}!")
                exit_price = order_dict[sl_order_id]['average_price']
                pnl_pct = ((exit_price - pos["entry_premium"]) / pos["entry_premium"]) * 100
                close_position(pos, exit_price, pnl_pct, "Exchange SL Hit")
                emailer.send_live_alert(
                    f"🛑 STOP LOSS HIT: {opt_symbol}",
                    f"<p>The exchange has executed the Stop-Loss order for <b>{opt_symbol}</b>.</p>"
                    f"<p><b>Exit Price:</b> ₹{exit_price}<br><b>PnL:</b> {pnl_pct:.2f}%</p>"
                )
                continue
                
        # 2. Fetch live quote to check for trailing SL
        base_symbol = pos["symbol"]
        try:
            quote = kite_data.quote([f"NFO:{opt_symbol}", f"NSE:{base_symbol}"])
            if f"NFO:{opt_symbol}" not in quote:
                continue
            ltp = quote[f"NFO:{opt_symbol}"]["last_price"]
            live_oi = quote[f"NFO:{opt_symbol}"].get("oi", 0)
            live_volume = quote[f"NFO:{opt_symbol}"].get("volume", 0)
            
            spot_ltp = quote.get(f"NSE:{base_symbol}", {}).get("last_price", 0)
            spot_vwap = quote.get(f"NSE:{base_symbol}", {}).get("average_price", 0)
        except Exception as e:
            print(f"Error fetching quote for {opt_symbol} via Data account: {e}")
            continue
            
        p_entry = pos["entry_premium"]
        
        # Momentum Integrity Logic
        initial_oi = pos.get("initial_oi") or 0
        highest_oi = max(pos.get("highest_oi") or 0, live_oi)
        initial_volume = pos.get("initial_volume") or 0
        
        price_drop_pct = ((pos["highest_high"] - ltp) / pos["highest_high"]) * 100 if pos["highest_high"] > 0 else 0
        oi_drop_pct = ((highest_oi - live_oi) / highest_oi) * 100 if highest_oi > 0 else 0
        
        momentum_broken = False
        break_reason = ""
        
        try:
            entry_dt = pos["entry_time"]
            if isinstance(entry_dt, str):
                entry_dt = datetime.fromisoformat(entry_dt)
            entry_dt = entry_dt.replace(tzinfo=None)
            time_in_trade_mins = (datetime.now() - entry_dt).total_seconds() / 60
        except Exception as e:
            print(f"Error parsing date for {opt_symbol}: {e}")
            time_in_trade_mins = 60 # Default to normal mode on error
            
        # Shakeout Window vs Normal Limits
        if time_in_trade_mins < 30:
            p_drop_limit = 15
            oi_drop_limit = 6
            vol_spike_limit = 2.5
        else:
            p_drop_limit = 8
            oi_drop_limit = 5
            vol_spike_limit = 1.5
            
        vwap_broken = spot_ltp < spot_vwap if spot_vwap > 0 else False
        
        # VWAP Support Override
        if not vwap_broken and time_in_trade_mins < 30:
            # If institutions defend VWAP during shakeout, give massive room to breathe
            p_drop_limit = 20
            
        if price_drop_pct > p_drop_limit and oi_drop_pct > oi_drop_limit:
            momentum_broken = True
            break_reason = "Momentum Break: Long Unwinding"
        elif price_drop_pct > p_drop_limit and live_volume > (initial_volume * vol_spike_limit) and initial_volume > 0:
            momentum_broken = True
            break_reason = "Momentum Break: Selling Pressure"
        elif vwap_broken and price_drop_pct > 10:
            momentum_broken = True
            break_reason = "Momentum Break: VWAP Dump"
        else:
            if time_in_trade_mins > 45:
                if ltp < p_entry * 1.20 and oi_drop_pct >= 0:
                    momentum_broken = True
                    break_reason = "Momentum Break: Stagnation"
                
        if momentum_broken:
            print(f"[{datetime.now()}] 🚨 {break_reason} for {opt_symbol}! Executing SMART EXIT.")
            if live_buy_flag.lower() == "true":
                try:
                    # Cancel resting SL to free up margin
                    if sl_order_id and sl_order_id != "FAILED":
                        try:
                            kite_exec.cancel_order(variety=kite_exec.VARIETY_REGULAR, order_id=sl_order_id)
                            print(f"Canceled resting SL {sl_order_id}")
                        except Exception as ce:
                            print(f"Warning: Failed to cancel SL before exit: {ce}")
                            
                    aggressive_limit = round(ltp * 0.90, 1)
                    kite_exec.place_order(
                        variety=kite_exec.VARIETY_REGULAR,
                        exchange=kite_exec.EXCHANGE_NFO,
                        tradingsymbol=opt_symbol,
                        transaction_type=kite_exec.TRANSACTION_TYPE_SELL,
                        quantity=pos["qty"],
                        product=kite_exec.PRODUCT_NRML,
                        order_type=kite_exec.ORDER_TYPE_LIMIT,
                        price=aggressive_limit
                    )
                    print(f"✅ SMART EXIT Order Placed for {opt_symbol} at limit {aggressive_limit}")
                    emailer.send_live_alert(
                        f"⚡ SMART EXIT Executed: {opt_symbol}",
                        f"<p>The AI detected a predictive Momentum Break (<b>{break_reason}</b>) and successfully executed an aggressive Limit Sell order to close the position.</p>"
                        f"<p><b>Limit Price:</b> ₹{aggressive_limit}</p>"
                    )
                except Exception as e:
                    print(f"❌ Failed to SMART EXIT {opt_symbol}: {e}")
                    emailer.send_live_alert(
                        f"🚨 URGENT: Smart Exit Failed for {opt_symbol}",
                        f"<p>The AI detected a Momentum Break (<b>{break_reason}</b>) and attempted to exit, but Zerodha blocked the order.</p>"
                        f"<p><b>Reason:</b> {str(e)}</p>"
                        f"<p style='color: #f85149;'><b>ACTION REQUIRED:</b> Manually square off <b>{opt_symbol}</b> on Zerodha immediately!</p>"
                    )
                    continue
            else:
                print(f"Paper mode: SMART EXIT simulated for {opt_symbol}")
                
            pnl_pct = ((ltp - pos["entry_premium"]) / pos["entry_premium"]) * 100
            close_position(pos, ltp, pnl_pct, break_reason)
            continue
            
        if highest_oi > (pos.get("highest_oi") or 0):
            update_highest_oi(order_id, highest_oi)
        highest_high = max(pos["highest_high"], ltp)
        current_sl = pos["current_sl"]
        target_activation = p_entry * 1.50
        trailing_active = pos["trailing_active"]
        
        sl_updated = False
        
        # Trailing SL Logic
        if highest_high >= target_activation:
            if "ZYDUS" in opt_symbol:
                new_sl = round(highest_high - (highest_high * 0.05), 1)
            else:
                new_sl = round(highest_high - (highest_high * 0.10), 1)
                
            if new_sl > current_sl:
                if not trailing_active:
                    emailer.send_live_alert(
                        f"🟢 PROFIT LOCKED: +50% on {opt_symbol}",
                        f"<p>The option premium has hit the +50% target! Trailing Stop-Loss is now ACTIVE.</p>"
                        f"<p><b>New Locked SL:</b> ₹{new_sl}</p>"
                    )
                current_sl = new_sl
                sl_updated = True
                trailing_active = True
                
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
            update_position_sl(pos['order_id'], current_sl, highest_high, trailing_active)
            
        print(f"[{datetime.now()}] 🟢 {opt_symbol} | LTP: {ltp} | Entry: {p_entry} | SL: {current_sl:.2f} | Trail Active: {trailing_active}")

if __name__ == "__main__":
    monitor_positions()
