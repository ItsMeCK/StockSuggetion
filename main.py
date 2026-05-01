import logging
from dotenv import load_dotenv
import os

# Load environment variables from .env file securely
load_dotenv()

from midnight_sovereign.core.state import SovereignState
from midnight_sovereign.graph.builder import build_sovereign_graph
from midnight_sovereign.pipeline.screener import SovereignScreener
from midnight_sovereign.agents.macro_gate import run_macro_regime_gate
from midnight_sovereign.pipeline.ingestion import run_eod_ingestion

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def execute_daily_run():
    """
    The orchestrator that runs the complete Midnight Sovereign EOD cycle.
    Expected to be triggered via a local cron job post-market close.
    """
    logging.info("==================================================")
    logging.info("INITIATING MIDNIGHT SOVEREIGN V2.1 EOD BATCH RUN")
    logging.info("==================================================")

    # 1. Historical REST Ingestion (Fetch data and push to TimescaleDB)
    logging.info("--- PHASE 1: DATA INGESTION ---")
    run_eod_ingestion()

    # 2. Compile the LangGraph engine (Requires PostgresSaver in production)
    # We compile without the checkpointer for local simulation
    app = build_sovereign_graph({})

    # 3. Initialize the State Object
    initial_state = SovereignState(
        macro_regime="",
        candidates=[],
        heuristic_flags={},
        experience_warnings={},
        vision_validations={},
        approved_allocations={},
        execution_telemetry={},
        error_log=[]
    )

    # 4. Phase 2: Deterministic Pipeline
    logging.info("--- PHASE 2: DETERMINISTIC PIPELINE ---")
    
    # Run the Macro Regime Gate directly to check if we should even proceed
    macro_delta = run_macro_regime_gate(initial_state)
    initial_state.update(macro_delta) # Manual delta merge for the initial router

    if initial_state.get("macro_regime") == "CAPITULATION":
        logging.error("SYSTEM HALTED: Macro Regime is CAPITULATION. No trades will be processed.")
        return

    # Run the Polars Screener
    screener = SovereignScreener()
    candidates, incubator, base_scores = screener.run_pipeline()
    initial_state["candidates"] = candidates
    initial_state["incubator"] = incubator
    initial_state["base_scores"] = base_scores

    # 5. Phase 3 & 4: LangGraph Orchestration (Cognitive Engine + Execution + Reflection)
    if not candidates and not incubator:
        logging.info("No candidates or incubator stocks passed the Deterministic Screener. Ending run.")
        return

    logging.info("--- PHASE 3 & 4: LANGGRAPH COGNITIVE ORCHESTRATION ---")
    
    # Invoke the compiled LangGraph workflow
    final_state = app.invoke(initial_state)
    
    # --- PHASE 5: PERSISTENCE FOR REFLECTION LOOP ---
    import json
    from datetime import datetime
    
    run_record = {
        "timestamp": datetime.now().isoformat(),
        "macro_regime": final_state.get("macro_regime"),
        "candidates": final_state.get("candidates"),
        "approved_allocations": final_state.get("approved_allocations"),
        "telemetry": final_state.get("execution_telemetry")
    }
    
    history_path = f"run_history/run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(history_path, "w") as f:
        json.dump(run_record, f, indent=4)
    
    logging.info(f"Full run state persisted for reflection at: {history_path}")
    
    logging.info("==================================================")
    logging.info("EOD BATCH RUN COMPLETE.")
    logging.info(f"Approved Allocations: {list(final_state.get('approved_allocations', {}).keys())}")
    logging.info("==================================================")

if __name__ == "__main__":
    execute_daily_run()
