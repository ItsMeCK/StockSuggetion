import os
import psycopg2
import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )

class BitemporalStore:
    """
    Data Access Layer to persist and retrieve bitemporal financial metrics,
    guaranteeing we reuse once-processed data and avoid expensive GPT api calls.
    """
    
    @staticmethod
    def has_period_data(symbol: str, actual_period: date) -> bool:
        """Checks if we already have ingested data for the symbol and period."""
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM inst_bitemporal_financials WHERE symbol = %s AND actual_period = %s LIMIT 1",
            (symbol, actual_period)
        )
        exists = cur.fetchone() is not None
        cur.close()
        conn.close()
        return exists

    @staticmethod
    def save_period_data(data: Dict[str, Any]):
        """Persists a quarterly financial record to the bitemporal database."""
        conn = get_db_connection()
        cur = conn.cursor()
        
        insert_query = """
            INSERT INTO inst_bitemporal_financials (
                symbol, actual_period, period_name, ebitda, revenue, net_profit, 
                debt, equity, roic, operating_cash_flow, capex, rd_expenses, order_book, notes
            ) VALUES (
                %(symbol)s, %(actual_period)s, %(period_name)s, %(ebitda)s, %(revenue)s, %(net_profit)s, 
                %(debt)s, %(equity)s, %(roic)s, %(operating_cash_flow)s, %(capex)s, %(rd_expenses)s, %(order_book)s, %(notes)s
            ) ON CONFLICT (symbol, actual_period) DO UPDATE SET
                ebitda = EXCLUDED.ebitda,
                revenue = EXCLUDED.revenue,
                net_profit = EXCLUDED.net_profit,
                debt = EXCLUDED.debt,
                equity = EXCLUDED.equity,
                roic = EXCLUDED.roic,
                operating_cash_flow = EXCLUDED.operating_cash_flow,
                capex = EXCLUDED.capex,
                rd_expenses = EXCLUDED.rd_expenses,
                order_book = EXCLUDED.order_book,
                notes = EXCLUDED.notes,
                system_recorded_at = NOW();
        """
        try:
            cur.execute(insert_query, data)
            conn.commit()
            logging.info(f"Bitemporal save complete for {data['symbol']} at period {data['period_name']}.")
        except Exception as e:
            conn.rollback()
            logging.error(f"Failed to save bitemporal financials for {data.get('symbol')}: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def fetch_12_quarters(symbol: str) -> List[Dict[str, Any]]:
        """Retrieves sorted quarterly financial records (up to 12 consecutive quarters)."""
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                symbol, actual_period, period_name, system_recorded_at, ebitda, revenue, net_profit, 
                debt, equity, roic, operating_cash_flow, capex, rd_expenses, order_book, notes
            FROM inst_bitemporal_financials
            WHERE symbol = %s
            ORDER BY actual_period ASC LIMIT 12
        """, (symbol,))
        
        cols = [
            "symbol", "actual_period", "period_name", "system_recorded_at", "ebitda", "revenue", "net_profit", 
            "debt", "equity", "roic", "operating_cash_flow", "capex", "rd_expenses", "order_book", "notes"
        ]
        
        results = []
        for row in cur.fetchall():
            res_dict = dict(zip(cols, row))
            # Format dates to ISO strings for JSON safety
            if isinstance(res_dict["actual_period"], (date, datetime)):
                res_dict["actual_period"] = res_dict["actual_period"].strftime("%Y-%m-%d")
            if isinstance(res_dict["system_recorded_at"], (date, datetime)):
                res_dict["system_recorded_at"] = res_dict["system_recorded_at"].isoformat()
            results.append(res_dict)
            
        cur.close()
        conn.close()
        return results
