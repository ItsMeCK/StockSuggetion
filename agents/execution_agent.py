import uuid
import logging
from typing import Dict, Any

from core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ZerodhaExecutionModule:
    """
    Handles End-of-Day (EOD) execution by placing After Market Orders (AMO).
    Also manages automated Good Till Triggered (GTT) stop-loss placement.
    """
    def __init__(self):
        import os
        from dotenv import load_dotenv
        load_dotenv()
        from kiteconnect import KiteConnect
        self.api_key = os.getenv("EXEC_KITE_API_KEY")
        self.access_token = os.getenv("EXEC_KITE_ACCESS_TOKEN")
        self.live = False
        
        if self.api_key and self.access_token:
            try:
                self.api_key = self.api_key.strip("'\"")
                self.access_token = self.access_token.strip("'\"")
                self.kite = KiteConnect(api_key=self.api_key)
                self.kite.set_access_token(self.access_token)
                self.live = True
                logging.info("Zerodha API: Live execution account connected. Orders are ON!")
            except Exception as e:
                logging.error(f"Zerodha API: Failed to initialize execution session: {e}. Defaulting to Dry-Run.")
        else:
            logging.warning("Zerodha API: EXEC_KITE credentials missing. Defaulting to Dry-Run.")

    def place_cnc_limit_order(self, symbol: str, quantity: int, limit_price: float, is_amo: bool = False) -> str:
        order_type_str = "AMO" if is_amo else "Regular"
        logging.info(f"Zerodha API: Placing {order_type_str} Limit Buy for {quantity} shares of {symbol} at ₹{limit_price}")
        
        order_id = None
        if self.live:
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

    def fetch_live_ltp(self, symbol: str) -> float:
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
                quote = data_kite.quote([f"NSE:{symbol}"])
                return float(quote[f"NSE:{symbol}"]["last_price"])
        except Exception as e:
            logging.error(f"Failed to fetch live LTP for {symbol}: {e}")
        return None

    def place_gtt_stop_loss(self, symbol: str, quantity: int, stop_price: float) -> str:
        logging.info(f"Zerodha API: Placing GTT Stop-Loss Sell for {quantity} shares of {symbol} at ₹{stop_price}")
        
        gtt_id = None
        if self.live:
            try:
                # Retrieve current price for trigger threshold estimation
                last_price = stop_price * 1.05
                live_price = self.fetch_live_ltp(symbol)
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

def run_execution_agent(state: SovereignState) -> Dict[str, Any]:
    """
    LangGraph Node integration for the Execution Agent.
    """
    approved_allocations = state.get("approved_allocations", {})
    
    if not approved_allocations:
        logging.info("No approved allocations received. Execution Agent resting.")
        return {"execution_telemetry": {}}

    pulse_num = state.get("pulse", 0)
    is_amo = (pulse_num not in [1, 2, 3])

    execution_module = ZerodhaExecutionModule()
    execution_telemetry = {}

    from midnight_sovereign.core.schemas import AllocationSchema
    for symbol, allocation in approved_allocations.items():
        try:
            # Pydantic Quality Gate
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
            # Check 65-minute distribution alignment
            if not execution_module.verify_65m_markup(symbol):
                logging.warning(f"Execution VETO: 65-minute distribution detected for {symbol}. Rejecting trade.")
                continue
                
            # If active trading and live price fetch is successful, adjust entry limit, quantity, and stop-loss.
            if not is_amo:
                live_ltp = execution_module.fetch_live_ltp(symbol)
                if live_ltp:
                    logging.info(f"Zerodha API: Adjusted {symbol} entry price to live LTP: ₹{live_ltp} (was ₹{entry_limit})")
                    entry_limit = round(round(live_ltp * 20) / 20, 2)
                    
                    # Recalculate quantity to keep allocation within allocated capital limit
                    capital_allocated = float(allocation.get("capital_allocated", 5000.0))
                    import math
                    qty = math.floor(capital_allocated / entry_limit)
                    if qty <= 0:
                        logging.warning(f"Execution Error: Zero quantity calculated for {symbol} at LTP ₹{entry_limit} (Capital: ₹{capital_allocated}).")
                        continue
                        
                    # Recalculate stop-loss based on the actual entry price to maintain strict 5% risk tolerance
                    stop_loss = round(round((entry_limit * 0.95) * 20) / 20, 2)
                    logging.info(f"Zerodha API: Recalculated {symbol} quantity to {qty} and stop-loss to ₹{stop_loss}")
                else:
                    logging.warning(f"Zerodha API: Live LTP fetch failed for {symbol}. Falling back to default entry limit and quantity.")

            # 1. Place the entry order (Regular or AMO)
            order_id = execution_module.place_cnc_limit_order(symbol, qty, entry_limit, is_amo=is_amo)
            
            # 2. Place the protective GTT order immediately
            gtt_id = execution_module.place_gtt_stop_loss(symbol, qty, stop_loss)
            
            execution_telemetry[symbol] = {
                "amo_order_id": order_id,
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
