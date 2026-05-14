import logging
import os
import math
from kiteconnect import KiteConnect
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SovereignExecutionEngine:
    """
    Dedicated Order Management System.
    Handles autonomous order routing for the Sovereign Engine using the secondary Execution Account.
    """
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv("EXEC_KITE_API_KEY")
        self.access_token = os.getenv("EXEC_KITE_ACCESS_TOKEN")
        self.capital_per_trade = 5000.0  # Fixed 5k INR allocation per suggestion
        
        if not self.api_key or not self.access_token:
            logging.warning("EXEC account credentials missing. Operating in Dry-Run mode.")
            self.live = False
        else:
            self.kite = KiteConnect(api_key=self.api_key)
            self.kite.set_access_token(self.access_token)
            self.live = True

    def buy_top_candidates(self, signals: list):
        """
        Takes the top signals (usually 3) and fires 5k CNC Market Orders.
        """
        # Limit to top 3 if more are passed
        signals = sorted(signals, key=lambda x: x['score'], reverse=True)[:3]
        
        if not signals:
            logging.info("No valid signals to execute.")
            return

        for signal in signals:
            symbol = signal['ticker']
            price = signal.get('price', 0.0)
            
            if price <= 0:
                logging.error(f"Cannot execute {symbol} - Invalid price.")
                continue
                
            qty = math.floor(self.capital_per_trade / price)
            if qty == 0:
                logging.warning(f"Price of {symbol} ({price}) is greater than allocated capital ({self.capital_per_trade}). Skipping.")
                continue
                
            logging.info(f"EXECUTING BUY: {qty} shares of {symbol} @ ~{price} (Total: ₹{qty*price:.2f})")
            
            if self.live:
                try:
                    limit_price = round(round((price * 1.02) * 20) / 20, 2)
                    order_id = self.kite.place_order(
                        variety=self.kite.VARIETY_REGULAR,
                        tradingsymbol=symbol,
                        exchange=self.kite.EXCHANGE_NSE,
                        transaction_type=self.kite.TRANSACTION_TYPE_BUY,
                        quantity=qty,
                        order_type=self.kite.ORDER_TYPE_LIMIT,
                        price=limit_price,
                        product=self.kite.PRODUCT_CNC,
                        validity=self.kite.VALIDITY_DAY
                    )
                    logging.info(f"✅ BUY Order Placed: {order_id} (Limit: {limit_price})")
                except Exception as e:
                    logging.error(f"❌ Failed to place BUY order for {symbol}: {e}")

    def squash_all_positions(self):
        """
        The 3:15 PM End-of-Day Flattening.
        Sells all open Positions (today's trades) and Holdings (yesterday's Incubator trades).
        """
        logging.info("🚨 INITIATING END-OF-DAY PORTFOLIO SQUASH 🚨")
        if not self.live:
            logging.info("Dry-Run: Simulated squashing all positions.")
            return

        try:
            # 1. Squash Holdings (T1/Delivery from yesterday)
            holdings = self.kite.holdings()
            for holding in holdings:
                qty = holding['quantity'] + holding['t1_quantity']
                symbol = holding['tradingsymbol']
                
                if qty > 0:
                    logging.info(f"Squashing Holding: {qty} shares of {symbol}")
                    try:
                        # Use primary data account for quotes
                        kite_data = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
                        kite_data.set_access_token(os.getenv("KITE_ACCESS_TOKEN").strip("'"))
                        quote = kite_data.quote([f"NSE:{symbol}"])
                        live_price = quote[f"NSE:{symbol}"]["last_price"]
                        limit_price = round(round((live_price * 0.98) * 20) / 20, 2)
                        
                        self.kite.place_order(
                            variety=self.kite.VARIETY_REGULAR,
                            tradingsymbol=symbol,
                            exchange=self.kite.EXCHANGE_NSE,
                            transaction_type=self.kite.TRANSACTION_TYPE_SELL,
                            quantity=qty,
                            order_type=self.kite.ORDER_TYPE_LIMIT,
                            price=limit_price,
                            product=self.kite.PRODUCT_CNC,
                            validity=self.kite.VALIDITY_DAY
                        )
                    except Exception as e:
                        logging.error(f"❌ Failed to sell holding {symbol}: {e}")

            # 2. Squash Positions (Intraday CNC bought today)
            positions = self.kite.positions()
            # Net positions include CNC orders placed today that haven't moved to holdings
            for pos in positions.get('net', []):
                qty = pos['quantity']
                symbol = pos['tradingsymbol']
                
                # If quantity > 0, it means we have an open long position
                if qty > 0 and pos['product'] == 'CNC':
                    logging.info(f"Squashing Position: {qty} shares of {symbol}")
                    try:
                        kite_data = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
                        kite_data.set_access_token(os.getenv("KITE_ACCESS_TOKEN").strip("'"))
                        quote = kite_data.quote([f"NSE:{symbol}"])
                        live_price = quote[f"NSE:{symbol}"]["last_price"]
                        limit_price = round(round((live_price * 0.98) * 20) / 20, 2)
                        
                        self.kite.place_order(
                            variety=self.kite.VARIETY_REGULAR,
                            tradingsymbol=symbol,
                            exchange=self.kite.EXCHANGE_NSE,
                            transaction_type=self.kite.TRANSACTION_TYPE_SELL,
                            quantity=qty,
                            order_type=self.kite.ORDER_TYPE_LIMIT,
                            price=limit_price,
                            product=self.kite.PRODUCT_CNC,
                            validity=self.kite.VALIDITY_DAY
                        )
                    except Exception as e:
                        logging.error(f"❌ Failed to sell position {symbol}: {e}")
                        
            logging.info("✅ Squash complete. All active capital returned to cash.")
            
        except Exception as e:
            logging.error(f"❌ Critical error during portfolio squash: {e}")

if __name__ == "__main__":
    # Test Dry-Run execution
    engine = SovereignExecutionEngine()
    engine.squash_all_positions()
