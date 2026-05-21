import os
import sys
import datetime
import polars as pl
import logging
from dotenv import load_dotenv

# Ensure root workspace is in python path
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))

from core.state import SovereignState
from graph.builder import build_sovereign_graph
from pipeline.screener import SovereignScreener
from agents.macro_gate import run_macro_regime_gate

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.WARNING)

load_dotenv()

# We define the reconstructed candles for 2026-05-18 at each pulse:
PULSE_CANDLES = {
    1: {  # Pulse 1 (10:15 AM)
        "GLAND": {"open": 2122.0, "high": 2135.9, "low": 2034.0, "close": 2108.8, "volume": 2693845.0},
        "SCI": {"open": 329.9, "high": 343.4, "low": 325.25, "close": 340.9, "volume": 6738496.0},
        "INDUSTOWER": {"open": 425.0, "high": 434.55, "low": 421.9, "close": 429.05, "volume": 2231489.0}
    },
    2: {  # Pulse 2 (1:15 PM)
        "GLAND": {"open": 2122.0, "high": 2164.4, "low": 2034.0, "close": 2138.2, "volume": 5000207.0},
        "SCI": {"open": 329.9, "high": 351.7, "low": 325.25, "close": 344.35, "volume": 20592210.0},
        "INDUSTOWER": {"open": 425.0, "high": 434.55, "low": 421.9, "close": 430.55, "volume": 9588111.0}
    },
    3: {  # Pulse 3 (3:15 PM)
        "GLAND": {"open": 2122.0, "high": 2178.0, "low": 2034.0, "close": 2149.8, "volume": 6450541.0},
        "SCI": {"open": 329.9, "high": 351.7, "low": 325.25, "close": 344.6, "volume": 23842797.0},
        "INDUSTOWER": {"open": 425.0, "high": 434.55, "low": 421.9, "close": 431.1, "volume": 11427655.0}
    }
}

class MockScreener(SovereignScreener):
    def __init__(self, pulse_number: int):
        super().__init__()
        self.pulse_number = pulse_number

    def fetch_market_data(self) -> pl.DataFrame:
        # Load historical database data up to today
        df = super().fetch_market_data()
        
        # Override 2026-05-18 data for GLAND, SCI, INDUSTOWER with our pulse-specific candles
        target_date = datetime.date(2026, 5, 18)
        
        # We will convert the Polars DataFrame to dict list, modify the target rows, and convert back
        rows = df.to_dicts()
        pulse_data = PULSE_CANDLES[self.pulse_number]
        
        updated = 0
        for r in rows:
            # Check if row date is today
            r_date = r["time"]
            if isinstance(r_date, datetime.datetime):
                r_date = r_date.date()
            elif isinstance(r_date, datetime.date):
                pass
            else:
                # String conversion if needed
                try:
                    r_date = datetime.datetime.strptime(str(r_date)[:10], "%Y-%m-%d").date()
                except:
                    continue
            
            if r_date == target_date and r["symbol"] in pulse_data:
                sym = r["symbol"]
                candle = pulse_data[sym]
                r["open"] = candle["open"]
                r["high"] = candle["high"]
                r["low"] = candle["low"]
                r["close"] = candle["close"]
                r["volume"] = candle["volume"]
                updated += 1
                
        # If today's rows were not in the database for some reason, we would append them, but they are!
        new_df = pl.DataFrame(rows).cast(df.schema)
        return new_df

def run_pulse_simulation(pulse_num: int):
    print(f"\n⚡ SIMULATING PULSE #{pulse_num} AT THE EXACT TIME...")
    
    # 1. Initialize State
    initial_state = SovereignState(
        target_date="2026-05-18",
        pulse=pulse_num,
        macro_regime="",
        candidates=[],
        incubator=[],
        flagged_momentum_candidates=[],
        breakouts=[],
        base_scores={},
        heuristic_flags={},
        experience_warnings={},
        vision_validations={},
        news_catalysts={},
        approved_allocations={},
        execution_telemetry={},
        error_log=[],
        debate_count=0
    )
    
    # 2. Macro Regime Gate
    macro_delta = run_macro_regime_gate(initial_state)
    initial_state.update(macro_delta)
    
    # 3. Screener
    screener = MockScreener(pulse_num)
    candidates, incubator, flagged_momentum, base_scores, macro_regime = screener.run_pipeline(target_date="2026-05-18", pulse=pulse_num)
    
    initial_state["candidates"] = candidates
    initial_state["incubator"] = incubator
    initial_state["flagged_momentum_candidates"] = flagged_momentum
    initial_state["base_scores"] = base_scores
    
    print(f"   Deterministic Screener Candidates: {candidates}")
    print(f"   Flagged Momentum Candidates: {flagged_momentum}")
    
    # 4. LangGraph Orchestration
    app = build_sovereign_graph({})
    final_state = app.invoke(initial_state)
    
    approved = final_state.get("approved_allocations", {})
    print(f"   --- FINAL RESOLUTION ---")
    if approved:
        for ticker, details in approved.items():
            print(f"   ✅ APPROVED: {ticker} | Shares: {details.get('shares')} | Entry: ₹{details.get('entry')} | Stop-Loss: ₹{details.get('stop_loss')} | Score: {details.get('conviction_score'):.1f}")
    else:
        print("   ❌ NO TRADES APPROVED.")

if __name__ == "__main__":
    for p in [1, 2, 3]:
        run_pulse_simulation(p)
