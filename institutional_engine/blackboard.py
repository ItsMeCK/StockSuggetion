import os
import json
import psycopg2
import logging
from typing import Dict, Any, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )

class Blackboard:
    """
    Decoupled Shared State Coordinator representing the Blackboard Layer.
    Agents query and write to this centralized data structure asynchronously.
    """
    def __init__(self, state_id: str = "current"):
        self.state_id = state_id
        self.global_macro = "UNKNOWN"
        self.asset_pool = []
        self.proposals = {}
        self.scorecard = {}
        self.vetoes = {}
        self.load_state()

    def load_state(self):
        """Loads state from the database Blackboard table."""
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT global_macro, state_data FROM inst_blackboard_state WHERE state_id = %s",
            (self.state_id,)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if row:
            self.global_macro = row[0]
            state_data = row[1]
            self.asset_pool = state_data.get("asset_pool", [])
            self.proposals = state_data.get("proposals", {})
            self.scorecard = state_data.get("scorecard", {})
            self.vetoes = state_data.get("vetoes", {})
        else:
            # First time setup
            self.save_state()

    def save_state(self):
        """Persists the in-memory blackboard copy back to Postgres."""
        conn = get_db_connection()
        cur = conn.cursor()
        
        state_data = {
            "asset_pool": self.asset_pool,
            "proposals": self.proposals,
            "scorecard": self.scorecard,
            "vetoes": self.vetoes
        }
        
        cur.execute("""
            INSERT INTO inst_blackboard_state (state_id, global_macro, state_data, last_updated)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (state_id) DO UPDATE SET
                global_macro = EXCLUDED.global_macro,
                state_data = EXCLUDED.state_data,
                last_updated = NOW();
        """, (self.state_id, self.global_macro, json.dumps(state_data)))
        
        conn.commit()
        cur.close()
        conn.close()

    def set_macro_regime(self, regime: str):
        self.global_macro = regime
        self.save_state()

    def update_scorecard(self, symbol: str, audit_results: Dict[str, Any]):
        self.scorecard[symbol] = audit_results
        self.save_state()

    def write_proposal(self, symbol: str, agent_name: str, proposal_dict: Dict[str, Any]):
        if symbol not in self.proposals:
            self.proposals[symbol] = {}
        self.proposals[symbol][agent_name] = proposal_dict
        self.save_state()
        
        # Persist individually in the inst_agent_proposals ledger too
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO inst_agent_proposals (symbol, agent_name, action, confidence_score, thesis, sizing)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (symbol, agent_name, proposal_dict.get("action"), proposal_dict.get("confidence"), 
              proposal_dict.get("thesis"), proposal_dict.get("sizing", 0.0)))
        conn.commit()
        cur.close()
        conn.close()

    def raise_veto(self, symbol: str, reason: str):
        self.vetoes[symbol] = {
            "reason": reason,
            "timestamp": datetime.now().isoformat() if hasattr(datetime, 'now') else "2026-05-17T08:15:00"
        }
        self.save_state()
        
        # Persist in inst_veto_registry
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO inst_veto_registry (symbol, veto_reason)
            VALUES (%s, %s)
            ON CONFLICT (symbol) DO UPDATE SET
                veto_reason = EXCLUDED.veto_reason,
                vetoed_at = NOW();
        """, (symbol, reason))
        conn.commit()
        cur.close()
        conn.close()

    def is_vetoed(self, symbol: str) -> bool:
        return symbol in self.vetoes
