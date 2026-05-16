import logging
from dotenv import load_dotenv
import os
import argparse
import schedule
import time
import json
from datetime import datetime

# Load environment variables from .env file securely
load_dotenv()

from core.state import SovereignState
from graph.builder import build_sovereign_graph
from pipeline.screener import SovereignScreener
from agents.macro_gate import run_macro_regime_gate
from pipeline.ingestion import run_eod_ingestion

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_pulse(pulse_number: int):
    """
    Executes a specific intraday pulse.
    """
    logging.info(f"==================================================")
    logging.info(f"INITIATING MIDNIGHT SOVEREIGN PULSE #{pulse_number}")
    logging.info(f"==================================================")

    # 1. Historical REST Ingestion (In pulse mode, this should ideally fetch minute/hour data)
    logging.info("--- PHASE 1: DATA INGESTION ---")
    run_eod_ingestion()

    # 2. Compile the LangGraph engine
    app = build_sovereign_graph({})

    # 3. Initialize the State Object
    initial_state = SovereignState(
        target_date=datetime.now().strftime("%Y-%m-%d"),
        macro_regime="",
        candidates=[],
        incubator=[],
        flagged_momentum_candidates=[],
        breakouts=[],
        base_scores={},
        heuristic_flags={},
        experience_warnings={},
        vision_validations={},
        approved_allocations={},
        execution_telemetry={},
        error_log=[],
        debate_count=0
    )

    # 4. Phase 2: Deterministic Pipeline
    logging.info("--- PHASE 2: DETERMINISTIC PIPELINE ---")
    
    # Run the Macro Regime Gate
    macro_delta = run_macro_regime_gate(initial_state)
    initial_state.update(macro_delta)

    if initial_state.get("macro_regime") == "CAPITULATION":
        logging.error("SYSTEM HALTED: Macro Regime is CAPITULATION.")
        return

    # Run the Polars Screener (Now with Elite Top 2 + Titan Bypass)
    screener = SovereignScreener()
    candidates, incubator, flagged_momentum, base_scores, macro_regime = screener.run_pipeline()
    initial_state["candidates"] = candidates
    initial_state["incubator"] = incubator
    initial_state["flagged_momentum_candidates"] = flagged_momentum
    initial_state["base_scores"] = base_scores

    # 5. Phase 3 & 4: LangGraph Orchestration
    if not candidates and not incubator and not flagged_momentum:
        logging.info("No candidates passed the Deterministic Screener. Ending pulse.")
        return

    logging.info("--- PHASE 3 & 4: LANGGRAPH COGNITIVE ORCHESTRATION ---")
    final_state = app.invoke(initial_state)
    
    # --- PHASE 5: PERSISTENCE ---
    run_record = {
        "pulse": pulse_number,
        "timestamp": datetime.now().isoformat(),
        "macro_regime": final_state.get("macro_regime"),
        "candidates": final_state.get("candidates"),
        "approved_allocations": final_state.get("approved_allocations"),
        "telemetry": final_state.get("execution_telemetry")
    }
    
    os.makedirs("run_history", exist_ok=True)
    history_path = f"run_history/pulse_{pulse_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(history_path, "w") as f:
        json.dump(run_record, f, indent=4)
    
    logging.info(f"Pulse #{pulse_number} complete. Approved: {list(final_state.get('approved_allocations', {}).keys())}")

def main():
    parser = argparse.ArgumentParser(description="Midnight Sovereign Orchestrator")
    parser.add_argument("--pulse", type=int, choices=[1, 2, 3], help="Execute a specific intraday pulse immediately.")
    parser.add_argument("--daemon", action="store_true", help="Run in daemon mode with scheduled pulses.")
    args = parser.parse_args()

    if args.pulse:
        run_pulse(args.pulse)
    elif args.daemon:
        logging.info("Sovereign Daemon Mode Active. Scheduling Pulses at 10:00, 13:00, 15:15 IST.")
        schedule.every().day.at("10:00").do(run_pulse, 1)
        schedule.every().day.at("13:00").do(run_pulse, 2)
        schedule.every().day.at("15:15").do(run_pulse, 3)
        
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        # Default EOD behavior
        run_pulse(0)

if __name__ == "__main__":
    main()
