import os
from dotenv import load_dotenv
from core.live_trading import get_kite_exec_client
from core.db_manager import get_all_active_positions, close_position

def squash_all_trades():
    load_dotenv()
    try:
        kite_exec = get_kite_exec_client()
    except Exception as e:
        print(f"Error initializing kite exec client: {e}")
        return

    positions = get_all_active_positions()
    if not positions:
        print("No active positions in DB to squash.")
        return

    for pos in positions:
        opt_symbol = pos["option_symbol"]
        sl_order_id = pos["sl_order_id"]
        qty = pos["qty"]
        
        print(f"Squashing trade for {opt_symbol}...")
        
        # 1. Cancel the pending SL order
        try:
            kite_exec.cancel_order(
                variety=kite_exec.VARIETY_REGULAR,
                order_id=sl_order_id
            )
            print(f"✅ Cancelled SL order: {sl_order_id}")
        except Exception as e:
            print(f"⚠️ Could not cancel SL order (maybe already executed/cancelled): {e}")
            
        # 2. Place a Limit SELL order to exit the position (5% below current LTP to ensure execution)
        try:
            # We need the LTP to place a limit order, but we can also try a MARKET order for exit?
            # Wait, market orders are blocked for stock options. Let's get a quick quote or just place a limit 10% below spot.
            # We don't have kite_data here, but we can initialize it.
            from core.live_trading import get_kite_data_client
            kite_data = get_kite_data_client()
            quote = kite_data.quote([f"NFO:{opt_symbol}"])
            if f"NFO:{opt_symbol}" in quote:
                ltp = quote[f"NFO:{opt_symbol}"]["last_price"]
                sell_limit = round(ltp * 0.90, 1) # 10% below LTP to guarantee instant execution
                print(f"Placing Limit SELL for {qty} qty of {opt_symbol} at {sell_limit} (LTP was {ltp})")
                
                exit_order_id = kite_exec.place_order(
                    variety=kite_exec.VARIETY_REGULAR,
                    exchange=kite_exec.EXCHANGE_NFO,
                    tradingsymbol=opt_symbol,
                    transaction_type=kite_exec.TRANSACTION_TYPE_SELL,
                    quantity=qty,
                    product=kite_exec.PRODUCT_NRML,
                    order_type=kite_exec.ORDER_TYPE_LIMIT,
                    price=sell_limit
                )
                print(f"✅ SELL order placed successfully: {exit_order_id}")
                
                # Close in DB
                pnl_pct = ((ltp - pos["entry_premium"]) / pos["entry_premium"]) * 100
                close_position(pos, ltp, pnl_pct, "MANUAL_SQUASH_OVEREXTENDED")
            else:
                print(f"❌ Could not fetch quote for {opt_symbol} to place sell order.")
        except Exception as e:
            print(f"❌ Failed to place SELL order for {opt_symbol}: {e}")

if __name__ == "__main__":
    squash_all_trades()
