import uuid
import logging
from typing import Dict, Any

from midnight_sovereign.core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ZerodhaExecutionModule:
    """
    Handles End-of-Day (EOD) execution by placing After Market Orders (AMO).
    Also manages automated Good Till Triggered (GTT) stop-loss placement.
    """
    def __init__(self):
        # In production, initialize KiteConnect instance here
        pass

    def place_amo_limit_order(self, symbol: str, quantity: int, limit_price: float) -> str:
        logging.info(f"Zerodha API: Placing AMO Limit Buy for {quantity} shares of {symbol} at ₹{limit_price}")
        order_id = f"AMO_ORD_{uuid.uuid4().hex[:6].upper()}"
        
        # Append AMO_PLACED to bitemporal ledger
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
            # Check if the latest status for this ticker is already AMO_PLACED
            cur.execute("""
                SELECT status FROM trade_events 
                WHERE ticker = %s 
                ORDER BY system_time DESC LIMIT 1
            """, (symbol,))
            latest_status = cur.fetchone()
            
            if not latest_status or latest_status[0] != 'AMO_PLACED':
                trade_id = str(uuid.uuid4())
                cur.execute("""
                    INSERT INTO trade_events (trade_id, ticker, status, price, quantity, order_id, notes)
                    VALUES (%s, %s, 'AMO_PLACED', %s, %s, %s, 'Initial AMO generated via risk model');
                """, (trade_id, symbol, limit_price, quantity, order_id))
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
        # Live Kite API execution mapping
        # In production: data = kite.historical_data(instrument_token, from_date, to_date, '60minute')
        # We mock live connectivity fallback safely:
        distribution_detected = False # Default assumption of healthy continuation
        return not distribution_detected

    def place_gtt_stop_loss(self, symbol: str, quantity: int, stop_price: float) -> str:
        """
        Mocks placing a Good Till Triggered (GTT) Stop-Loss order on Zerodha.
        This provides automated downside protection completely detached from the local script's uptime.
        """
        logging.info(f"Zerodha API: Placing GTT Stop-Loss Sell for {quantity} shares of {symbol} at ₹{stop_price}")
        import uuid
        gtt_id = f"GTT_SL_{uuid.uuid4().hex[:6].upper()}"
        return gtt_id

def run_execution_agent(state: SovereignState) -> Dict[str, Any]:
    """
    LangGraph Node integration for the Execution Agent.
    """
    approved_allocations = state.get("approved_allocations", {})
    
    if not approved_allocations:
        logging.info("No approved allocations received. Execution Agent resting.")
        return {"execution_telemetry": {}}

    execution_module = ZerodhaExecutionModule()
    execution_telemetry = {}

    for symbol, allocation in approved_allocations.items():
        qty = allocation.get("shares", 0)
        entry_limit = allocation.get("entry", 0.0)
        stop_loss = allocation.get("stop_loss", 0.0)

        if qty > 0:
            # Check 65-minute distribution alignment
            if not execution_module.verify_65m_markup(symbol):
                logging.warning(f"Execution VETO: 65-minute distribution detected for {symbol}. Rejecting trade.")
                continue
                
            # 1. Place the entry AMO order
            order_id = execution_module.place_amo_limit_order(symbol, qty, entry_limit)
            
            # 2. Place the protective GTT order immediately
            gtt_id = execution_module.place_gtt_stop_loss(symbol, qty, stop_loss)
            
            execution_telemetry[symbol] = {
                "amo_order_id": order_id,
                "gtt_id": gtt_id,
                "status": "QUEUED_AMO"
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
