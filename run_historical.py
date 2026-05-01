import logging
import os
import argparse
from datetime import datetime
from dotenv import load_dotenv
from midnight_sovereign.core.state import SovereignState
from midnight_sovereign.pipeline.screener import SovereignScreener
from midnight_sovereign.agents.macro_gate import run_macro_regime_gate

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_historical_engine(target_date: str):
    logging.info("==================================================")
    logging.info(f"MIDNIGHT SOVEREIGN: HISTORICAL SIMULATION FOR {target_date}")
    logging.info("==================================================")

    # 1. Initialize State
    initial_state = SovereignState(
        target_date=target_date,
        macro_regime="",
        candidates=[],
        heuristic_flags={},
        entry_trigger_results={},
        approved_allocations={},
        execution_telemetry={},
        error_log=[]
    )

    # 2. Macro Regime (Handled by the Gate now)
    initial_state["macro_regime"] = ""

    # 3. Screener with Historical Slicing
    logging.info(f"--- PHASE 2: HISTORICAL POLARS SCREENER ({target_date}) ---")
    screener = SovereignScreener()
    candidates, base_scores = screener.run_pipeline(target_date=target_date)
    initial_state["candidates"] = candidates
    initial_state["base_scores"] = base_scores

    if not candidates:
        logging.info(f"No candidates passed the screener on {target_date}.")
        return

    # 4. LangGraph Orchestration
    logging.info("--- PHASE 3 & 4: LANGGRAPH COGNITIVE ORCHESTRATION ---")
    from langgraph.checkpoint.postgres import PostgresSaver
    from midnight_sovereign.graph.builder import build_sovereign_graph_with_checkpointer
    db_uri = f"postgresql://agent:agentpassword@{os.getenv('DB_HOST', 'localhost')}:5433/sovereign_state"
    
    with PostgresSaver.from_conn_string(db_uri) as checkpointer:
        checkpointer.setup()
        app = build_sovereign_graph_with_checkpointer(checkpointer)
        # Using a distinct thread for historical runs to avoid polluting live state
        thread_id = f"historical_{target_date.replace('-', '')}"
        final_state = app.invoke(initial_state, config={"configurable": {"thread_id": thread_id}})
    
    # 5. Output results
    logging.info("==================================================")
    logging.info(f"HISTORICAL RUN COMPLETE FOR {target_date}")
    logging.info(f"Candidates Found: {candidates}")
    approved = list(final_state.get('approved_allocations', {}).keys())
    logging.info(f"Approved Allocations: {approved}")
    logging.info("==================================================")
    
    return {
        "date": target_date,
        "candidates": candidates,
        "approved": approved
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Midnight Sovereign for a historical date.")
    parser.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")
    args = parser.parse_args()
    
    run_historical_engine(args.date)
