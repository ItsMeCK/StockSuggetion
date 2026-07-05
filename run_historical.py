import logging
import os
import argparse
from datetime import datetime
from dotenv import load_dotenv
from core.state import SovereignState
from pipeline.screener import SovereignScreener
from agents.macro_gate import run_macro_regime_gate

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
        incubator=[],
        flagged_momentum_candidates=[],
        breakouts=[],
        heuristic_flags={},
        entry_trigger_results={},
        news_catalysts={},
        approved_allocations={},
        execution_telemetry={},
        error_log=[]
    )

    # 2. Macro Regime (Handled by the Gate now)
    initial_state["macro_regime"] = ""

    # 3. Screener with Historical Slicing
    logging.info(f"--- PHASE 2: HISTORICAL POLARS SCREENER ({target_date}) ---")
    screener = SovereignScreener()
    candidates, incubator, flagged_momentum, base_scores, macro_regime = screener.run_pipeline(target_date=target_date)
    
    # We process established Stage 2 stocks through the Cognitive Gate
    initial_state["candidates"] = candidates
    initial_state["incubator"] = incubator
    initial_state["flagged_momentum_candidates"] = flagged_momentum
    initial_state["base_scores"] = base_scores
    initial_state["macro_regime"] = macro_regime["regime"]

    if not candidates and not incubator and not flagged_momentum:
        logging.info(f"No candidates or incubator stocks passed the screener on {target_date}.")
        return {
            "date": target_date,
            "candidates": [],
            "incubator": [],
            "approved": []
        }

    # 4. LangGraph Orchestration
    logging.info("--- PHASE 3 & 4: LANGGRAPH COGNITIVE ORCHESTRATION ---")
    from langgraph.checkpoint.postgres import PostgresSaver
    from graph.builder import build_sovereign_graph_with_checkpointer
    db_uri = f"postgresql://agent:agentpassword@{os.getenv('DB_HOST', 'localhost')}:5433/sovereign_state"
    
    import time
    with PostgresSaver.from_conn_string(db_uri) as checkpointer:
        checkpointer.setup()
        app = build_sovereign_graph_with_checkpointer(checkpointer)
        # Using a truly unique thread for every run to prevent state bleeding
        thread_id = f"historical_{target_date.replace('-', '')}_{int(time.time())}"
        final_state = app.invoke(initial_state, config={"configurable": {"thread_id": thread_id}})
    
    # 5. Output results
    logging.info("==================================================")
    logging.info(f"HISTORICAL RUN COMPLETE FOR {target_date}")
    logging.info(f"Candidates Found: {candidates}")
    allocations = final_state.get('approved_allocations', {})
    # Exclude entries dropped by the Conviction Router (the merge_dicts reducer
    # keeps them in state, flagged with dropped=True)
    approved = [s for s, a in allocations.items() if not a.get("dropped")]
    logging.info(f"Approved Allocations: {approved}")
    logging.info("==================================================")

    return {
        "date": target_date,
        "candidates": candidates,
        "incubator": incubator,
        "approved": approved,
        "approved_allocations": {s: a for s, a in allocations.items() if not a.get("dropped")},
        "macro_regime": final_state.get("macro_regime", "UNKNOWN"),
        "agent_scores": final_state.get("agent_scores", {})
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Midnight Sovereign for a historical date.")
    parser.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")
    args = parser.parse_args()
    
    run_historical_engine(args.date)
