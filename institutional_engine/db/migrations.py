import os
import psycopg2
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )

def run_migrations():
    """
    Creates parallel institutional engine tables in TimescaleDB.
    """
    logging.info("Initiating parallel database migrations for Institutional Engine...")
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 1. Bitemporal Financials Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inst_bitemporal_financials (
            symbol VARCHAR(20) NOT NULL,
            actual_period DATE NOT NULL,
            period_name VARCHAR(20) NOT NULL,
            system_recorded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            ebitda DOUBLE PRECISION,
            revenue DOUBLE PRECISION,
            net_profit DOUBLE PRECISION,
            debt DOUBLE PRECISION,
            equity DOUBLE PRECISION,
            roic DOUBLE PRECISION,
            operating_cash_flow DOUBLE PRECISION,
            capex DOUBLE PRECISION,
            rd_expenses DOUBLE PRECISION,
            order_book DOUBLE PRECISION,
            notes TEXT,
            PRIMARY KEY (symbol, actual_period)
        );
    """)
    logging.info("Table 'inst_bitemporal_financials' verified.")
    
    # 2. Blackboard Shared State Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inst_blackboard_state (
            state_id VARCHAR(50) PRIMARY KEY,
            global_macro VARCHAR(50),
            last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            state_data JSONB
        );
    """)
    logging.info("Table 'inst_blackboard_state' verified.")
    
    # 3. Agent Proposals Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inst_agent_proposals (
            proposal_id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            agent_name VARCHAR(50) NOT NULL,
            action VARCHAR(20) NOT NULL, -- BUY, HOLD, SELL, HEDGE
            confidence_score DOUBLE PRECISION,
            thesis TEXT,
            sizing DOUBLE PRECISION,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
    """)
    logging.info("Table 'inst_agent_proposals' verified.")
    
    # 4. Risk Veto Registry Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inst_veto_registry (
            symbol VARCHAR(20) PRIMARY KEY,
            veto_reason TEXT,
            vetoed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
    """)
    logging.info("Table 'inst_veto_registry' verified.")
    
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Migrations completed successfully.")

if __name__ == "__main__":
    run_migrations()
