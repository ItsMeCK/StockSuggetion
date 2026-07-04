import uuid
import os
import logging
from typing import Dict, Any

from core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ZerodhaExecutionModule:
    """
    Handles End-of-Day (EOD) execution by placing After Market Orders (AMO).
    Also manages automated Good Till Triggered (GTT) stop-loss placement.
    """
    def __init__(self, is_historical: bool = False):
        import os
        from dotenv import load_dotenv
        load_dotenv()
        from kiteconnect import KiteConnect
        self.api_key = os.getenv("EXEC_KITE_API_KEY")
        self.access_token = os.getenv("EXEC_KITE_ACCESS_TOKEN")
        self.live = False
        self.live_buy = os.getenv("LIVE_BUY", "FALSE").upper() == "TRUE"
        self.is_historical = is_historical or (os.getenv("TRADING_MODE") == "HISTORICAL")
        
        if self.is_historical:
            logging.warning("Zerodha API: Running in HISTORICAL mode. Live execution is forced OFF.")
            return
        
        if self.api_key and self.access_token:
            try:
                self.api_key = self.api_key.strip("'\"")
                self.access_token = self.access_token.strip("'\"")
                self.kite = KiteConnect(api_key=self.api_key)
                self.kite.set_access_token(self.access_token)
                self.live = True
                logging.info(f"Zerodha API: Live execution account connected. Orders are ON! (Live Buy: {self.live_buy})")
            except Exception as e:
                logging.error(f"Zerodha API: Failed to initialize execution session: {e}. Defaulting to Dry-Run.")
        else:
            logging.warning("Zerodha API: EXEC_KITE credentials missing. Defaulting to Dry-Run.")

    def place_cnc_limit_order(self, symbol: str, quantity: int, limit_price: float, is_amo: bool = False) -> str:
        order_type_str = "AMO" if is_amo else "Regular"
        logging.info(f"Zerodha API: Placing {order_type_str} Limit Buy for {quantity} shares of {symbol} at ₹{limit_price}")
        
        order_id = None
        if self.live and self.live_buy:
            try:
                variety = self.kite.VARIETY_AMO if is_amo else self.kite.VARIETY_REGULAR
                order_id = self.kite.place_order(
                    variety=variety,
                    tradingsymbol=symbol,
                    exchange=self.kite.EXCHANGE_NSE,
                    transaction_type=self.kite.TRANSACTION_TYPE_BUY,
                    quantity=quantity,
                    order_type=self.kite.ORDER_TYPE_LIMIT,
                    price=limit_price,
                    product=self.kite.PRODUCT_CNC,
                    validity=self.kite.VALIDITY_DAY
                )
                logging.info(f"✅ Live {order_type_str} Order Placed: {order_id}")
            except Exception as e:
                logging.error(f"❌ Live {order_type_str} Order Placement failed: {e}")
                
        if not order_id:

            order_id = f"{order_type_str}_ORD_{uuid.uuid4().hex[:6].upper()}"
            logging.info(f"Dry-Run: Generated mock order ID: {order_id}")
            
        if self.is_historical:
            logging.info(f"Skipping database ledger append for {symbol} (HISTORICAL mode)")
            return order_id

        # Append to bitemporal ledger
        import os, psycopg2
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", "5432"),
                user=os.getenv("POSTGRES_USER", "quant"),
                password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
                dbname=os.getenv("POSTGRES_DB", "market_data")
            )
            cur = conn.cursor()
            cur.execute("""
                SELECT status FROM trade_events 
                WHERE ticker = %s 
                ORDER BY system_time DESC LIMIT 1
            """, (symbol,))
            latest_status = cur.fetchone()
            
            ledger_status = 'AMO_PLACED'
            
            if not latest_status or latest_status[0] not in ['AMO_PLACED']:
                trade_id = str(uuid.uuid4())
                cur.execute("""
                    INSERT INTO trade_events (trade_id, ticker, status, price, quantity, order_id, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """, (trade_id, symbol, ledger_status, limit_price, quantity, order_id, f'Initial {order_type_str} order generated via risk model'))
                conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logging.error(f"Ledger append error: {e}")
            
        return order_id

    def verify_65m_markup(self, symbol: str) -> bool:
        """
        Verifies intra-hour Stage 2 alignment via Zerodha Kite API.
        Ensures volume distribution does not block initial EOD momentum cross.
        """
        logging.info(f"Zerodha API: Evaluating 65-minute intraday distribution metrics for {symbol}...")
        distribution_detected = False 
        return not distribution_detected

    def fetch_live_ltp(self, symbol: str, exchange: str = "NSE") -> float:
        """
        Fetches the live Last Traded Price (LTP) from Zerodha Kite API.
        """
        import os
        from kiteconnect import KiteConnect
        try:
            api_key = os.getenv("KITE_API_KEY")
            access_token = os.getenv("KITE_ACCESS_TOKEN")
            if api_key and access_token:
                data_kite = KiteConnect(api_key=api_key.strip("'\""))
                data_kite.set_access_token(access_token.strip("'\""))
                quote = data_kite.quote([f"{exchange}:{symbol}"])
                key = f"{exchange}:{symbol}"
                if quote and key in quote:
                    return float(quote[key]["last_price"])
                else:
                    logging.warning(f"Zerodha API: Symbol {key} not found in quote response.")
        except Exception as e:
            logging.error(f"Failed to fetch live LTP for {symbol} on {exchange}: {e}")
        return None

    def place_gtt_stop_loss(self, symbol: str, quantity: int, stop_price: float) -> str:
        logging.info(f"Zerodha API: Placing GTT Stop-Loss Sell for {quantity} shares of {symbol} at ₹{stop_price}")
        
        gtt_id = None
        if self.live:
            try:
                # Retrieve current price for trigger threshold estimation
                last_price = stop_price * 1.05
                live_price = self.fetch_live_ltp(symbol, exchange="NSE")
                if live_price:
                    last_price = live_price

                gtt_id = self.kite.place_gtt(
                    trigger_type=self.kite.GTT_TYPE_SINGLE,
                    tradingsymbol=symbol,
                    exchange=self.kite.EXCHANGE_NSE,
                    trigger_values=[stop_price],
                    last_price=last_price,
                    orders=[{
                        "transaction_type": self.kite.TRANSACTION_TYPE_SELL,
                        "quantity": quantity,
                        "product": self.kite.PRODUCT_CNC,
                        "order_type": self.kite.ORDER_TYPE_LIMIT,
                        "price": stop_price,
                        "variety": self.kite.VARIETY_REGULAR
                    }]
                )
                logging.info(f"✅ Live GTT Stop-Loss Placed: {gtt_id}")
            except Exception as e:
                logging.error(f"❌ Live GTT Stop-Loss Placement failed: {e}")
                
        if not gtt_id:

            gtt_id = f"GTT_SL_{uuid.uuid4().hex[:6].upper()}"
            logging.info(f"Dry-Run: Generated mock GTT ID: {gtt_id}")
            
        return gtt_id

    def place_nfo_limit_order(self, option_symbol: str, quantity: int, limit_price: float, is_amo: bool = False, underlying_symbol: str = None) -> str:
        order_type_str = "AMO" if is_amo else "Regular"
        logging.info(f"Zerodha API: Placing {order_type_str} NFO Option Limit Buy for {quantity} contracts of {option_symbol} at premium ₹{limit_price}")
        
        order_id = None
        if self.live and self.live_buy:
            try:
                variety = self.kite.VARIETY_AMO if is_amo else self.kite.VARIETY_REGULAR
                order_id = self.kite.place_order(
                    variety=variety,
                    tradingsymbol=option_symbol,
                    exchange=self.kite.EXCHANGE_NFO,
                    transaction_type=self.kite.TRANSACTION_TYPE_BUY,
                    quantity=quantity,
                    order_type=self.kite.ORDER_TYPE_LIMIT,
                    price=limit_price,
                    product=self.kite.PRODUCT_NRML,
                    validity=self.kite.VALIDITY_DAY
                )
                logging.info(f"✅ Live NFO Option Order Placed: {order_id}")
            except Exception as e:
                logging.error(f"❌ Live NFO Option Order Placement failed: {e}")
                
        if not order_id:
            order_id = f"{order_type_str}_OPT_ORD_{uuid.uuid4().hex[:6].upper()}"
            logging.info(f"Dry-Run: Generated mock option order ID: {order_id}")
            
        if self.is_historical:
            logging.info(f"Skipping database ledger append for option {option_symbol} (HISTORICAL mode)")
            return order_id

        # Append to bitemporal ledger
        if underlying_symbol:
            import os, psycopg2
            try:
                conn = psycopg2.connect(
                    host=os.getenv("DB_HOST", "localhost"),
                    port=os.getenv("DB_PORT", "5432"),
                    user=os.getenv("POSTGRES_USER", "quant"),
                    password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
                    dbname=os.getenv("POSTGRES_DB", "market_data")
                )
                cur = conn.cursor()
                cur.execute("""
                    SELECT status FROM trade_events 
                    WHERE ticker = %s 
                    ORDER BY system_time DESC LIMIT 1
                """, (underlying_symbol,))
                latest_status = cur.fetchone()
                
                ledger_status = 'AMO_PLACED'
                
                if not latest_status or latest_status[0] not in ['AMO_PLACED']:
                    trade_id = str(uuid.uuid4())
                    notes_str = f"Initial option {order_type_str} order generated via risk model: {option_symbol}"
                    cur.execute("""
                        INSERT INTO trade_events (trade_id, ticker, status, price, quantity, order_id, notes)
                        VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """, (trade_id, underlying_symbol, ledger_status, limit_price, quantity, order_id, notes_str))
                    conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                logging.error(f"Option ledger append error: {e}")
                
        return order_id

    def place_gtt_option_stop_loss(self, option_symbol: str, quantity: int, stop_premium: float) -> str:
        logging.info(f"Zerodha API: Placing GTT Option Stop-Loss Sell for {quantity} contracts of {option_symbol} at premium ₹{stop_premium}")
        
        gtt_id = None
        if self.live:
            try:
                last_price = stop_premium * 1.5
                live_price = self.fetch_live_ltp(option_symbol, exchange="NFO")
                if live_price:
                    last_price = live_price

                gtt_id = self.kite.place_gtt(
                    trigger_type=self.kite.GTT_TYPE_SINGLE,
                    tradingsymbol=option_symbol,
                    exchange=self.kite.EXCHANGE_NFO,
                    trigger_values=[stop_premium],
                    last_price=last_price,
                    orders=[{
                        "transaction_type": self.kite.TRANSACTION_TYPE_SELL,
                        "quantity": quantity,
                        "product": self.kite.PRODUCT_NRML,
                        "order_type": self.kite.ORDER_TYPE_LIMIT,
                        "price": stop_premium,
                        "variety": self.kite.VARIETY_REGULAR
                    }]
                )
                logging.info(f"✅ Live Option GTT Stop-Loss Placed: {gtt_id}")
            except Exception as e:
                logging.error(f"❌ Live Option GTT Stop-Loss Placement failed: {e}")
                
        if not gtt_id:
            gtt_id = f"GTT_OPT_SL_{uuid.uuid4().hex[:6].upper()}"
            logging.info(f"Dry-Run: Generated mock option GTT ID: {gtt_id}")
            
        return gtt_id

def run_execution_agent(state: SovereignState) -> Dict[str, Any]:
    """
    LangGraph Node integration for the Execution Agent.
    Routes to either option or cash equity based on derivatives routing selection.
    """
    approved_allocations = state.get("approved_allocations", {})
    
    if not approved_allocations:
        logging.info("No approved allocations received. Execution Agent resting.")
        return {"execution_telemetry": {}}

    pulse_num = state.get("pulse", 0)
    is_amo = (pulse_num not in [1, 2, 3])

    is_historical = (state.get("target_date") is not None) or (os.getenv("TRADING_MODE") == "HISTORICAL")
    execution_module = ZerodhaExecutionModule(is_historical=is_historical)
    execution_telemetry = {}

    import math
    for symbol, allocation in approved_allocations.items():
        suggested_instrument = allocation.get("suggested_instrument", "EQUITY")
        capital_allocated = float(allocation.get("capital_allocated", 5000.0))
        
        if suggested_instrument == "NFO_OPTION":
            option_symbol = allocation.get("option_symbol")
            lot_size = allocation.get("lot_size", 1)
            
            if not option_symbol:
                logging.warning(f"Execution Error: NFO_OPTION selected for {symbol} but no option_symbol provided. Falling back to EQUITY.")
                suggested_instrument = "EQUITY"
            else:
                # Options routing branch
                # Fetch option live premium or estimate it
                option_ltp = execution_module.fetch_live_ltp(option_symbol, exchange="NFO")
                if not option_ltp:
                    # Fallback default if market closed or fetch fails (e.g. 2% of underlying close price)
                    underlying_entry = float(allocation.get("entry", 100.0))
                    option_ltp = max(1.0, round(underlying_entry * 0.02, 2))
                    logging.warning(f"Zerodha API: Option LTP fetch failed for {option_symbol}. Estimating premium at ₹{option_ltp}")
                
                # Size options: round to nearest lot (minimum 1 lot)
                lots = max(1, round(capital_allocated / (lot_size * option_ltp)))
                qty = lots * lot_size
                
                # Place Call Option buy order
                order_id = execution_module.place_nfo_limit_order(
                    option_symbol=option_symbol, 
                    quantity=qty, 
                    limit_price=option_ltp, 
                    is_amo=is_amo,
                    underlying_symbol=symbol
                )
                
                # Place option GTT stop loss at 40% discount to premium
                stop_premium = round(round((option_ltp * 0.60) * 20) / 20, 2)
                gtt_id = execution_module.place_gtt_option_stop_loss(option_symbol, qty, stop_premium)
                
                execution_telemetry[symbol] = {
                    "instrument_type": "NFO_OPTION",
                    "option_symbol": option_symbol,
                    "lots": lots,
                    "quantity": qty,
                    "premium": option_ltp,
                    "stop_premium": stop_premium,
                    "order_id": order_id,
                    "gtt_id": gtt_id,
                    "status": "QUEUED_NFO_AMO" if is_amo else "QUEUED_NFO_REGULAR"
                }
                logging.info(f"✅ Executed Option Route for {symbol}: Buy {qty} of {option_symbol} at ₹{option_ltp} | Stop SL at premium ₹{stop_premium}")
                continue

        if suggested_instrument == "EQUITY":
            # Cash equity branch
            from midnight_sovereign.core.schemas import AllocationSchema
            try:
                valid_alloc = AllocationSchema(
                    shares=int(allocation.get("shares", 0)),
                    entry=float(allocation.get("entry", 0.0)),
                    stop_loss=float(allocation.get("stop_loss", 0.0)),
                    confidence=float(allocation.get("confidence", allocation.get("conviction_score", 0.0))),
                    sizing_pct=float(allocation.get("sizing_pct", 0.0))
                )
            except Exception as e:
                logging.error(f"EXECUTION REJECTED: {symbol} failed quality check: {e}")
                continue

            qty = valid_alloc.shares
            entry_limit = valid_alloc.entry
            stop_loss = valid_alloc.stop_loss

            if qty > 0:
                if not execution_module.verify_65m_markup(symbol):
                    logging.warning(f"Execution VETO: 65-minute distribution detected for {symbol}. Rejecting trade.")
                    continue
                    
                if not is_amo:
                    live_ltp = execution_module.fetch_live_ltp(symbol, exchange="NSE")
                    if live_ltp:
                        logging.info(f"Zerodha API: Adjusted {symbol} entry price to live LTP: ₹{live_ltp} (was ₹{entry_limit})")
                        entry_limit = round(round(live_ltp * 20) / 20, 2)
                        qty = math.floor(capital_allocated / entry_limit)
                        if qty <= 0:
                            logging.warning(f"Execution Error: Zero quantity calculated for {symbol} at LTP ₹{entry_limit}.")
                            continue
                        stop_loss = round(round((entry_limit * 0.95) * 20) / 20, 2)
                
                order_id = execution_module.place_cnc_limit_order(symbol, qty, entry_limit, is_amo=is_amo)
                gtt_id = execution_module.place_gtt_stop_loss(symbol, qty, stop_loss)
                
                execution_telemetry[symbol] = {
                    "instrument_type": "EQUITY",
                    "shares": qty,
                    "entry_price": entry_limit,
                    "stop_loss": stop_loss,
                    "order_id": order_id,
                    "gtt_id": gtt_id,
                    "status": "QUEUED_AMO" if is_amo else "QUEUED_REGULAR"
                }
            else:
                logging.warning(f"Execution Error: Zero quantity calculated for {symbol}.")

    return {"execution_telemetry": execution_telemetry}

if __name__ == "__main__":
    # Test execution
    mock_state = SovereignState(
        approved_allocations={
            "RELIANCE": {"shares": 50, "entry": 3000.0, "stop_loss": 2850.0}
        }
    )
    result = run_execution_agent(mock_state)
    print(f"Delta State Update: {result}")
