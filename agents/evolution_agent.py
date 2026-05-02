import os
import json
import logging
import psycopg2
from typing import List, Dict, Any
from pathlib import Path

class EvolutionAgent:
    """
    The 'Scientist' Meta-Brain.
    Identifies failure clusters and mutates the rulebook.
    """
    def __init__(self):
        self.rules_path = Path(__file__).parent.parent / "core" / "context_rules_2.json"
        self.override_path = Path(__file__).parent.parent / "core" / "system_overrides.json"

    def audit_failures(self, lookback_days: int = 30):
        logging.info(f"EVOLUTION_AGENT: Auditing failures from the last {lookback_days} days...")
        
        # 1. FETCH RECENT FAILURES (Stop-Losses)
        # For now, we simulate this by looking for 'LOSS' outcomes in our pattern_embeddings table
        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('TIMESCALE_PORT', '5432'),
                user=os.getenv('TIMESCALE_USER', 'quant'),
                password=os.getenv('TIMESCALE_PASSWORD', 'quantpassword'),
                database=os.getenv('TIMESCALE_DB', 'market_data')
            )
            cur = conn.cursor()
            cur.execute(
                "SELECT symbol, pattern_type, time FROM pattern_embeddings WHERE outcome = 'LOSS' AND time > NOW() - INTERVAL '%s days'",
                (lookback_days,)
            )
            failures = cur.fetchall()
            cur.close()
            conn.close()
            
            if len(failures) < 3:
                logging.info("EVOLUTION_AGENT: Insufficient failure data for mutation. System is stable.")
                return
                
            # 2. IDENTIFY CLUSTERS (Simple pattern-based clustering for now)
            pattern_counts = {}
            for f in failures:
                pattern_counts[f[1]] = pattern_counts.get(f[1], 0) + 1
            
            # 3. GENERATE DYNAMIC OVERRIDES
            overrides = {}
            for pattern, count in pattern_counts.items():
                if count >= 3:
                    logging.warning(f"EVOLUTION_AGENT: Failure Cluster detected for '{pattern}'. Mutating logic.")
                    overrides[pattern] = {
                        "veto_multiplier": 1.5,
                        "hurdle_boost": 10.0,
                        "reason": f"Systemic failure in {pattern} detected via {count} losses."
                    }
            
            # 4. SAVE OVERRIDES
            if overrides:
                with open(self.override_path, 'w') as f:
                    json.dump(overrides, f, indent=4)
                logging.info(f"EVOLUTION_AGENT: Successfully pushed {len(overrides)} Hot-Fix mutations.")
                
        except Exception as e:
            logging.error(f"EVOLUTION_AGENT AUDIT ERROR: {e}")

if __name__ == "__main__":
    evo = EvolutionAgent()
    evo.audit_failures()
