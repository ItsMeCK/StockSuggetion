import os
import datetime
import json
import psycopg2
import logging
import uuid
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from midnight_sovereign.core.state import SovereignState
from midnight_sovereign.graph.builder import build_sovereign_graph, build_sovereign_graph_with_checkpointer
from midnight_sovereign.pipeline.screener import SovereignScreener
from midnight_sovereign.agents.macro_gate import run_macro_regime_gate
from midnight_sovereign.agents.reconciliation_node import run_phase_0_reconciliation
from langgraph.checkpoint.postgres import PostgresSaver

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_analysis_only():
    logging.info("==================================================")
    logging.info("MIDNIGHT SOVEREIGN V2.1: ANALYSIS & EXECUTION RUN")
    logging.info("==================================================")

    # 0. Phase 0: Reconciliation
    logging.info("--- PHASE 0: BITEMPORAL RECONCILIATION ---")
    run_phase_0_reconciliation({})

    # 0.1 Data Freshness Check
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            user=os.getenv("POSTGRES_USER", "quant"),
            password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
            dbname=os.getenv("POSTGRES_DB", "market_data")
        )
        cur = conn.cursor()
        cur.execute("SELECT MAX(time) FROM daily_ohlcv")
        latest_data_time = cur.fetchone()[0]
        cur.close()
        conn.close()

        if latest_data_time:
            now = datetime.datetime.now(datetime.timezone.utc)
            # If latest data is older than 3 days (e.g. over a weekend or missed ingestion)
            delta = now - latest_data_time
            if delta.days >= 2:
                logging.warning(f"CRITICAL WARNING: Market data is stale! Latest date: {latest_data_time}. System time: {now}")
                logging.warning("Continuing with stale data for simulation purposes, but execution may be redundant.")
    except Exception as e:
        logging.error(f"Data freshness check failed: {e}")

    # 1. Initialize State
    initial_state = SovereignState(
        macro_regime="",
        fii_net=0.0,
        dii_net=0.0,
        india_vix=0.0,
        dxy=0.0,
        candidates=[],
        heuristic_flags={},
        entry_trigger_results={},
        experience_warnings={},
        vision_validations={},
        approved_allocations={},
        execution_telemetry={},
        error_log=[]
    )

    # 2. Phase 2: Macro Regime Gate
    logging.info("--- PHASE 2: MACRO REGIME GATE ---")
    macro_delta = run_macro_regime_gate(initial_state)
    initial_state.update(macro_delta)

    if initial_state.get("macro_regime") == "CAPITULATION":
        logging.error("SYSTEM HALTED: Macro Regime is CAPITULATION.")
        return

    # 3. Phase 2: Polars Screener (REAL DATA)
    logging.info("--- PHASE 2: REAL DATA POLARS SCREENER ---")
    screener = SovereignScreener()
    candidates, base_scores = screener.run_pipeline()
    initial_state["candidates"] = candidates
    initial_state["base_scores"] = base_scores

    # Append SIGNALED status for all deterministic candidates
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            user=os.getenv("POSTGRES_USER", "quant"),
            password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
            dbname=os.getenv("POSTGRES_DB", "market_data")
        )
        cur = conn.cursor()
        for ticker in candidates:
            # Check if the latest status for this ticker is already SIGNALED
            cur.execute("""
                SELECT status FROM trade_events 
                WHERE ticker = %s 
                ORDER BY system_time DESC LIMIT 1
            """, (ticker,))
            latest_status = cur.fetchone()
            
            if latest_status and latest_status[0] == 'SIGNALED':
                continue
                
            trade_id = str(uuid.uuid4())
            cur.execute("""
                INSERT INTO trade_events (trade_id, ticker, status, notes)
                VALUES (%s, %s, 'SIGNALED', 'Identified by EOD Polars stage 2 screener');
            """, (trade_id, ticker))
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Successfully ledgered {len(candidates)} SIGNALED events.")
    except Exception as e:
        logging.error(f"Ledger append error for signals: {e}")

    if not candidates:
        logging.info("No candidates passed the Deterministic Screener. Ending run.")
        return

    # 4. Phase 3 & 4: LangGraph Orchestration
    logging.info("--- PHASE 3 & 4: LANGGRAPH COGNITIVE ORCHESTRATION ---")
    db_uri = f"postgresql://agent:agentpassword@{os.getenv('DB_HOST', 'localhost')}:5433/sovereign_state"
    
    with PostgresSaver.from_conn_string(db_uri) as checkpointer:
        checkpointer.setup()
        app = build_sovereign_graph_with_checkpointer(checkpointer)
        # Use a date-specific thread_id to avoid state leakage between days
        run_date = datetime.datetime.now().strftime("%Y%m%d")
        config = {"configurable": {"thread_id": f"sovereign_run_{run_date}"}}
        final_state = app.invoke(initial_state, config=config)
    
    # 5. Persistence for UI
    os.makedirs("run_history", exist_ok=True)
    run_record = {
        "timestamp": datetime.datetime.now().isoformat(),
        "macro_regime": final_state.get("macro_regime"),
        "candidates": candidates, 
        "base_scores": final_state.get("base_scores"),
        "entry_trigger_results": final_state.get("entry_trigger_results"),
        "conviction_scores": final_state.get("conviction_scores"),
        "approved_allocations": final_state.get("approved_allocations"),
        "telemetry": final_state.get("execution_telemetry")
    }
    history_path = f"run_history/run_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(history_path, "w") as f:
        json.dump(run_record, f, indent=4)
    
    logging.info(f"Run results saved to {history_path}")
    
    logging.info("==================================================")
    logging.info("RUN COMPLETE.")
    logging.info(f"Final Candidates: {candidates}")
    logging.info(f"Approved Allocations: {list(final_state.get('approved_allocations', {}).keys())}")
    logging.info("==================================================")

if __name__ == "__main__":
    run_analysis_only()
