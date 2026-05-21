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
        Sells only open Positions/Holdings that have been held for at least 2 trading days.
        """
        logging.info("🚨 INITIATING END-OF-DAY PORTFOLIO SQUASH (T+2 HOLD RULE) 🚨")
        
        # Initialize DB Connection
        import psycopg2
        import datetime
        db_conn = None
        try:
            db_conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", "5432"),
                user=os.getenv("POSTGRES_USER", "quant"),
                password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
                dbname=os.getenv("POSTGRES_DB", "market_data")
            )
        except Exception as e:
            logging.error(f"Squash DB connection failed: {e}. Squashing will proceed with calendar days fallback.")

        # Helper: Get Trading Days Ago
        def get_trading_days_ago(buy_date: datetime.date) -> int:
            if not db_conn:
                return (datetime.date.today() - buy_date).days
            try:
                cur = db_conn.cursor()
                cur.execute("""
                    SELECT DISTINCT time::date 
                    FROM daily_ohlcv 
                    WHERE symbol = 'NIFTY 50' 
                    ORDER BY time::date DESC 
                    LIMIT 20
                """)
                trading_dates = [row[0] for row in cur.fetchall()]
                cur.close()
                if buy_date in trading_dates:
                    return trading_dates.index(buy_date)
                else:
                    closest_dates = [d for d in trading_dates if d >= buy_date]
                    if closest_dates:
                        return trading_dates.index(closest_dates[-1])
                    return 999
            except Exception as ex:
                logging.error(f"Trading days index lookup error: {ex}")
                return (datetime.date.today() - buy_date).days

        # Helper: Get Ticker Buy Date
        def get_ticker_buy_date(symbol: str) -> datetime.date:
            if not db_conn:
                return None
            try:
                cur = db_conn.cursor()
                cur.execute("""
                    SELECT system_time::date 
                    FROM trade_events 
                    WHERE ticker = %s AND status IN ('AMO_PLACED', 'FILLED')
                    ORDER BY system_time ASC LIMIT 1
                """, (symbol,))
                row = cur.fetchone()
                cur.close()
                if row:
                    return row[0]
            except Exception as ex:
                logging.error(f"Error fetching buy date for {symbol}: {ex}")
            return None

        # Helper: Mark ticker as SQUASHED in ledger
        def mark_ticker_squashed(symbol: str, qty: int, price: float):
            if not db_conn:
                return
            try:
                import uuid
                cur = db_conn.cursor()
                trade_id = str(uuid.uuid4())
                cur.execute("""
                    INSERT INTO trade_events (trade_id, ticker, status, price, quantity, order_id, notes)
                    VALUES (%s, %s, 'SQUASHED', %s, %s, 'SQUASH', 'T+2 Hold Period complete. Flattened at EOD.');
                """, (trade_id, symbol, price, qty))
                db_conn.commit()
                cur.close()
            except Exception as ex:
                logging.error(f"Failed to append SQUASH to ledger: {ex}")

        # 1. Evaluate open Holdings (T1/Delivery)
        if self.live:
            try:
                holdings = self.kite.holdings()
            except Exception as e:
                logging.error(f"Failed to fetch holdings from Zerodha: {e}")
                holdings = []
        else:
            # Mock holdings for dry-run
            holdings = [
                {"tradingsymbol": "GLAND", "quantity": 23, "t1_quantity": 0},
                {"tradingsymbol": "SCI", "quantity": 145, "t1_quantity": 0},
                {"tradingsymbol": "INDUSTOWER", "quantity": 115, "t1_quantity": 0}
            ]

        for holding in holdings:
            symbol = holding['tradingsymbol']
            qty = holding['quantity'] + holding['t1_quantity']
            
            if qty > 0:
                buy_date = get_ticker_buy_date(symbol)
                if not buy_date:
                    logging.info(f"T+2 Rule: Holding {symbol} has no buy record in ledger. Skipping squash for safety.")
                    continue
                    
                age_days = get_trading_days_ago(buy_date)
                logging.info(f"T+2 Rule: Holding {symbol} | Bought: {buy_date} | Age: {age_days} trading days")
                
                if age_days >= 2:
                    logging.info(f"🚨 SQUASHING HOLDING: {qty} shares of {symbol} (Held {age_days} trading days)")
                    
                    if self.live:
                        try:
                            # Fetch live price
                            kite_data = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
                            kite_data.set_access_token(os.getenv("KITE_ACCESS_TOKEN").strip("'\""))
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
                            mark_ticker_squashed(symbol, qty, live_price)
                            logging.info(f"✅ SQUASH SELL Placed for {symbol} @ ₹{limit_price}")
                        except Exception as e:
                            logging.error(f"❌ Failed to squash holding {symbol}: {e}")
                    else:
                        logging.info(f"Dry-Run: Simulated squash of holding {symbol}")
                        mark_ticker_squashed(symbol, qty, 100.0)
                else:
                    logging.info(f"T+2 Rule: Holding {symbol} is younger than 2 trading days. Keeping position.")

        # 2. Evaluate open Positions (Intraday CNC bought today)
        if self.live:
            try:
                positions = self.kite.positions()
                net_positions = positions.get('net', [])
            except Exception as e:
                logging.error(f"Failed to fetch positions from Zerodha: {e}")
                net_positions = []
        else:
            net_positions = []

        for pos in net_positions:
            qty = pos['quantity']
            symbol = pos['tradingsymbol']
            
            if qty > 0 and pos['product'] == 'CNC':
                # CNC positions bought today have age = 0 trading days, so they are always held!
                logging.info(f"T+2 Rule: Position {symbol} bought today. Keeping position.")

        # Close DB Connection
        if db_conn:
            db_conn.close()
            
        logging.info("✅ Squash evaluation complete.")

if __name__ == "__main__":
    # Test Dry-Run execution
    engine = SovereignExecutionEngine()
    engine.squash_all_positions()
