import os
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
from kiteconnect import KiteConnect

def get_kite_data_client():
    load_dotenv()
    api_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    access_token = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")
    if not api_key or not access_token:
        raise ValueError("Missing regular KITE credentials in .env")
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite

def get_kite_exec_client():
    load_dotenv()
    api_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
    access_token = os.getenv("EXEC_KITE_ACCESS_TOKEN", "").strip("'\"")
    if not api_key or not access_token:
        raise ValueError("Missing EXEC_KITE credentials in .env")
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite

def execute_trade(symbol, score, catalyst, entry_time):
    """
    Executes a trade by finding an OTM Call option (approx 1% above spot)
    using the Data account, and places a market BUY order using the Exec account.
    """
    kite_data = get_kite_data_client()
    kite_exec = get_kite_exec_client()
    
    print(f"[{entry_time}] 🚀 Initiating Live Execution for {symbol} (Score: {score})")
    
    # 1. Fetch current Spot Price using Data Account
    try:
        quote = kite_data.quote([f"NSE:{symbol}"])
        if f"NSE:{symbol}" not in quote:
            print(f"Could not fetch spot price for {symbol}")
            return
        spot_price = quote[f"NSE:{symbol}"]["last_price"]
        print(f"LTP for {symbol}: {spot_price}")
    except Exception as e:
        print(f"Error fetching quote for {symbol} via Data account: {e}")
        return
        
    # 2. Find OTM Option (Approx 1% above spot)
    target_strike = spot_price * 1.01
    
    try:
        instruments = kite_data.instruments("NFO")
        symbol_options = [i for i in instruments if i['name'] == symbol and i['instrument_type'] == 'CE']
        
        if not symbol_options:
            print(f"No CE options found for {symbol}")
            return
            
        # Get current month expiry
        symbol_options.sort(key=lambda x: x['expiry'])
        nearest_expiry = symbol_options[0]['expiry']
        current_month_options = [i for i in symbol_options if i['expiry'] == nearest_expiry]
        
        # Find strike closest to target_strike
        closest_option = min(current_month_options, key=lambda x: abs(x['strike'] - target_strike))
        
        option_token = closest_option['instrument_token']
        option_symbol = closest_option['tradingsymbol']
        option_strike = closest_option['strike']
        lot_size = closest_option['lot_size']
        
        print(f"Selected OTM Option: {option_symbol} (Strike: {option_strike} | Spot: {spot_price})")
    except Exception as e:
        print(f"Error resolving option instrument via Data account: {e}")
        return
        
    # 3. Fetch Option Premium
    try:
        opt_quote = kite_data.quote([f"NFO:{option_symbol}"])
        entry_premium = opt_quote[f"NFO:{option_symbol}"]["last_price"]
        initial_oi = opt_quote[f"NFO:{option_symbol}"].get("oi", 0)
        initial_volume = opt_quote[f"NFO:{option_symbol}"].get("volume", 0)
        print(f"Option Premium: {entry_premium} | OI: {initial_oi} | Vol: {initial_volume}")
    except Exception as e:
        print(f"Error fetching option premium via Data account: {e}")
        return
        
    # 4. Place Orders using Exec Account
    load_dotenv()
    live_buy_flag = os.getenv("LIVE_BUY", "False")
    
    order_id = f"PAPER_{int(datetime.now().timestamp())}"
    sl_order_id = f"PAPER_SL_{int(datetime.now().timestamp())}"
    status = "PAPER_TRADED"
    
    initial_sl_price = round(entry_premium * 0.50, 1) # Tick size rounding
    
    if live_buy_flag.lower() == "true":
        try:
            # Institutional High-Watermark Entry (Anti-Chasing)
            # We don't buy the exact close. We wait for it to break 1% higher to confirm momentum continuation.
            trigger_price = round(entry_premium * 1.01, 1)
            limit_price = round(entry_premium * 1.03, 1)
            print(f"Placing ANTI-CHASING SL-Limit BUY order for {option_symbol}. Trigger: {trigger_price}, Limit: {limit_price}")
            order_id = kite_exec.place_order(
                variety=kite_exec.VARIETY_REGULAR,
                exchange=kite_exec.EXCHANGE_NFO,
                tradingsymbol=option_symbol,
                transaction_type=kite_exec.TRANSACTION_TYPE_BUY,
                quantity=lot_size,
                product=kite_exec.PRODUCT_NRML,
                order_type=kite_exec.ORDER_TYPE_SL,
                trigger_price=trigger_price,
                price=limit_price
            )
            print(f"✅ BUY Order Placed! ID: {order_id}")
            status = "LIVE_TRADED"
        except Exception as e:
            print(f"❌ Failed to place LIVE BUY order via Exec account: {e}")
            status = "FAILED"
            return

        # Attempt to place SL-Limit Order immediately (might fail if Buy order hasn't filled and margin is low)
        try:
            sl_limit_price = round(initial_sl_price * 0.95, 1)
            print(f"Placing SL Order at Trigger {initial_sl_price}, Limit {sl_limit_price}")
            sl_order_id = kite_exec.place_order(
                variety=kite_exec.VARIETY_REGULAR,
                exchange=kite_exec.EXCHANGE_NFO,
                tradingsymbol=option_symbol,
                transaction_type=kite_exec.TRANSACTION_TYPE_SELL,
                quantity=lot_size,
                product=kite_exec.PRODUCT_NRML,
                order_type=kite_exec.ORDER_TYPE_SL,
                trigger_price=initial_sl_price,
                price=sl_limit_price
            )
            print(f"✅ SL-M Order Placed! ID: {sl_order_id}")
        except Exception as e:
            print(f"⚠️ Failed to place SL order via Exec account (usually margin limits): {e}")
            print(f"⚠️ Warning: Position is unprotected. Please add SL manually when Buy order fills.")
            sl_order_id = "FAILED"
    else:
        print(f"LIVE_BUY is False. Simulating paper order: {order_id}")
        
    # 5. Save Position State to Postgres
    position = {
        "order_id": order_id,
        "sl_order_id": sl_order_id,
        "status": status,
        "entry_time": entry_time.isoformat(),
        "symbol": symbol,
        "option_symbol": option_symbol,
        "option_token": option_token,
        "spot_entry": spot_price,
        "entry_premium": entry_premium,
        "qty": lot_size,
        "score": score,
        "catalyst": catalyst,
        "current_sl": initial_sl_price,
        "highest_high": entry_premium,
        "trailing_active": False,
        "initial_oi": initial_oi,
        "highest_oi": initial_oi,
        "initial_volume": initial_volume
    }
    
    from core.db_manager import save_active_position
    try:
        save_active_position(position)
        print(f"✅ Position saved to Database: {order_id} (SL: {sl_order_id})")
    except Exception as e:
        print(f"❌ Failed to save position to DB: {e}")
