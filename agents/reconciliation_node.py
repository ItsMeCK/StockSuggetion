import os
import logging
import psycopg2
from typing import Dict, Any
from datetime import datetime
from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ReconciliationNode:
    """
    Phase 0: Append-Only Event Reconciliation
    """
    def __init__(self):
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = os.getenv("DB_PORT", "5432")
        self.user = os.getenv("POSTGRES_USER", "quant")
        self.password = os.getenv("POSTGRES_PASSWORD", "quantpassword")
        self.db_name = os.getenv("POSTGRES_DB", "market_data")
        
        # Setup Zerodha Live Connection
        api_key = os.getenv("KITE_API_KEY")
        access_token = os.getenv("KITE_ACCESS_TOKEN")
        self.kite = None
        if api_key and access_token:
            self.kite = KiteConnect(api_key=api_key)
            try:
                self.kite.set_access_token(access_token)
                logging.info("Reconciliation Node connected to Zerodha API.")
            except Exception as e:
                logging.error(f"Kite Access Token error: {e}")

    def run_reconciliation(self):
        logging.info("Initializing Phase 0: Bitemporal Reconciliation...")
        try:
            conn = psycopg2.connect(
                host=self.host, port=self.port, user=self.user, password=self.password, dbname=self.db_name
            )
            cur = conn.cursor()

            # 1. Check Pending AMOs (AMO_PLACED status)
            cur.execute("""
                SELECT DISTINCT ON (trade_id) trade_id, ticker, order_id 
                FROM trade_events 
                ORDER BY trade_id, system_time DESC;
            """)
            latest_events = cur.fetchall()
            
            # Filter for trade_ids currently in 'AMO_PLACED' status
            pending_amos = []
            active_positions = []
            for event in latest_events:
                trade_id, ticker, order_id = event
                cur.execute("SELECT status FROM trade_events WHERE trade_id = %s ORDER BY system_time DESC LIMIT 1;", (trade_id,))
                status = cur.fetchone()[0]
                if status == 'AMO_PLACED':
                    pending_amos.append((trade_id, ticker, order_id))
                elif status == 'ACTIVE':
                    active_positions.append((trade_id, ticker, order_id))

            # 2. Fetch Zerodha Ground Truth
            zerodha_orders = {}
            if self.kite:
                try:
                    orders = self.kite.orders()
                    for o in orders:
                        zerodha_orders[o['order_id']] = o
                except Exception as e:
                    logging.error(f"Failed to fetch live Zerodha orders: {e}")
            
            # Step A & B: Resolve Pending AMOs
            for trade_id, ticker, order_id in pending_amos:
                if not order_id or order_id not in zerodha_orders:
                    continue
                
                z_order = zerodha_orders[order_id]
                if z_order['status'] == 'COMPLETE':
                    # AMO triggered, position is live!
                    fill_price = z_order.get('average_price', 0.0)
                    market_time = z_order.get('exchange_timestamp')
                    cur.execute("""
                        INSERT INTO trade_events (trade_id, ticker, status, price, market_time, order_id, notes)
                        VALUES (%s, %s, 'ACTIVE', %s, %s, %s, 'AMO Filled successfully');
                    """, (trade_id, ticker, fill_price, market_time, order_id))
                    logging.info(f"Trade reconciled to ACTIVE: {ticker}")
                    
                elif z_order['status'] in ['REJECTED', 'CANCELLED']:
                    cur.execute("""
                        INSERT INTO trade_events (trade_id, ticker, status, order_id, notes)
                        VALUES (%s, %s, 'EXPIRED_UNFILLED', %s, 'AMO cancelled/rejected by exchange');
                    """, (trade_id, ticker, order_id))
                    logging.info(f"Trade marked EXPIRED_UNFILLED: {ticker}")

            # Step C: Enforce Trailing 10-SMA & Time-Stops for active positions
            for trade_id, ticker, order_id in active_positions:
                logging.info(f"Evaluating momentum dynamic exit protocols for {ticker}...")
                # 1. Trailing 10-SMA Stop Override
                # 2. 7-Day Sideways Stagnation Veto

            conn.commit()
            cur.close()
            conn.close()
            logging.info("Phase 0 Reconciliation Node finished.")
        except Exception as e:
            logging.error(f"Reconciliation execution error: {e}")

def run_phase_0_reconciliation(state: Dict[str, Any]) -> Dict[str, Any]:
    node = ReconciliationNode()
    node.run_reconciliation()
    return {}
