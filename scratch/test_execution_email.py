import os
import sys
import logging

# Ensure root workspace is in python path
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))

from core.state import SovereignState
from agents.execution_agent import run_execution_agent

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_pulse_execution():
    print("\n" + "="*80)
    print("🧪 RUNNING PULSE EXECUTION TEST")
    print("="*80)

    # 1. Setup mock approved allocations
    mock_allocations = {
        "ZYDUSWELL": {
            "shares": 150,
            "entry": 1850.50,
            "stop_loss": 1750.00,
            "confidence_score": 96.0,
            "sizing_pct": 10.0
        }
    }

    # Test Pulse 1 (Should execute normally)
    print("\n>>> Testing Pulse 1 (Normal Execution Mode)...")
    state_p1 = SovereignState(
        pulse=1,
        approved_allocations=mock_allocations,
        execution_telemetry={},
        error_log=[]
    )
    res_p1 = run_execution_agent(state_p1)
    print(f"Pulse 1 Result Telemetry: {res_p1}")

    # Test Pulse 2 (Should skip order and send email)
    print("\n>>> Testing Pulse 2 (Skip Order & Email Notification Mode)...")
    state_p2 = SovereignState(
        pulse=2,
        approved_allocations=mock_allocations,
        execution_telemetry={},
        error_log=[]
    )
    res_p2 = run_execution_agent(state_p2)
    print(f"Pulse 2 Result Telemetry: {res_p2}")

if __name__ == "__main__":
    test_pulse_execution()
