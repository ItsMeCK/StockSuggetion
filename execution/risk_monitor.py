import logging
import os
from kiteconnect import KiteConnect
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def enforce_risk_limits():
    """
    Scans all open positions and holdings.
    If PnL is >= +5% or <= -5%, it immediately places a MARKET SELL order.
    """
    load_dotenv()
    api_key = os.getenv("EXEC_KITE_API_KEY")
    access_token = os.getenv("EXEC_KITE_ACCESS_TOKEN")
    
    if not api_key or not access_token:
        logging.warning("EXEC account credentials missing. Cannot run risk monitor.")
        return

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    
    TAKE_PROFIT = 0.05
    STOP_LOSS = -0.05
    
    def evaluate_and_exit(items, item_type="position"):
        for item in items:
            qty = item['quantity']
            if item_type == "holding":
                qty = item['quantity'] + item.get('t1_quantity', 0)
                
            # Only monitor active long positions
            if qty > 0:
                symbol = item['tradingsymbol']
                avg_price = item['average_price']
                
                if avg_price == 0:
                    continue
                    
                live_price = item.get('last_price', 0)
                
                # Fetch live quote if not present in the positions payload
                if live_price == 0:
                    try:
                        kite_data = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
                        kite_data.set_access_token(os.getenv("KITE_ACCESS_TOKEN").strip("'"))
                        quote = kite_data.quote([f"NSE:{symbol}"])
                        live_price = quote[f"NSE:{symbol}"]["last_price"]
                    except:
                        continue
                        
                pnl_pct = (live_price - avg_price) / avg_price
                
                if pnl_pct >= TAKE_PROFIT or pnl_pct <= STOP_LOSS:
                    action = "TAKE_PROFIT" if pnl_pct >= TAKE_PROFIT else "STOP_LOSS"
                    logging.info(f"🚨 {action} TRIGGERED for {symbol}: PnL is {pnl_pct*100:.2f}% (Avg: {avg_price}, Live: {live_price})")
                    
                    try:
                        sell_price = round(round((live_price * 0.98) * 20) / 20, 2)
                        kite.place_order(
                            variety=kite.VARIETY_REGULAR,
                            tradingsymbol=symbol,
                            exchange=kite.EXCHANGE_NSE,
                            transaction_type=kite.TRANSACTION_TYPE_SELL,
                            quantity=qty,
                            order_type=kite.ORDER_TYPE_LIMIT,
                            price=sell_price,
                            product=kite.PRODUCT_CNC,
                            validity=kite.VALIDITY_DAY
                        )
                        logging.info(f"✅ Executed exit order for {qty} shares of {symbol}")
                    except Exception as e:
                        logging.error(f"❌ Failed to place exit order for {symbol}: {e}")

    try:
        # 1. Check Positions (Intraday trades bought today)
        positions = kite.positions()
        evaluate_and_exit(positions.get('net', []), "position")
        
        # 2. Check Holdings (BTST trades bought yesterday at 3:15 PM)
        holdings = kite.holdings()
        evaluate_and_exit(holdings, "holding")
        
    except Exception as e:
        logging.error(f"Critical error in risk monitor: {e}")

if __name__ == "__main__":
    logging.info("🛡️ Starting 10-Minute Risk Monitor check...")
    enforce_risk_limits()
    logging.info("✅ Risk Monitor check complete.")
