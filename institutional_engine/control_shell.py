import logging
from typing import Dict, Any, List
from institutional_engine.blackboard import Blackboard

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ControlShell:
    """
    Control Shell Scheduler: The Blackboard brain.
    Asynchronously monitors the blackboard, resolves Oracle/Hunter conflicts,
    and enforces absolute vetoes from the Sentinel.
    """
    def __init__(self, blackboard: Blackboard):
        self.blackboard = blackboard

    def harmonize_symbol(self, symbol: str) -> Dict[str, Any]:
        logging.info(f"🧠 Control Shell: Coordinating final synthesis for {symbol}...")
        
        # 1. Check Risk Sentinel Veto Registry first
        if self.blackboard.is_vetoed(symbol):
            veto_info = self.blackboard.vetoes[symbol]
            logging.error(f"   [BLOCKED] Control Shell rejected execution of {symbol}. Reason: {veto_info['reason']}")
            return {
                "symbol": symbol,
                "execution_approved": False,
                "status": "VETOED",
                "notes": veto_info["reason"]
            }
            
        proposals = self.blackboard.proposals.get(symbol, {})
        oracle_proposal = proposals.get("Value Oracle", {})
        hunter_proposal = proposals.get("Alpha Hunter", {})
        
        oracle_action = oracle_proposal.get("action", "HOLD")
        hunter_action = hunter_proposal.get("action", "HOLD")
        
        execution_approved = False
        final_action = "HOLD"
        final_sizing = 0.0
        final_strategy = "CASH_RESERVE"
        notes = ""
        
        # 2. Reconcile Philosophy Conflict (The Oracle vs The Hunter)
        if oracle_action == "BUY" and hunter_action == "BUY":
            # Direct Alignment: Elite Buy Signal!
            execution_approved = True
            final_action = "BUY"
            final_sizing = oracle_proposal.get("sizing", 50000.0)
            final_strategy = "LONG_ACCUMULATE"
            notes = "Direct Alignment: Long-Term Value & Short-Term Momentum in sync. Standard Long Accumulation."
            
        elif oracle_action == "BUY" and hunter_action == "HEDGE":
            # Value says Buy, but Hunter sees Short-term Wave Corrective pressure
            # synthetic harmonization: Execute the buy, but Overlay a hedge!
            execution_approved = True
            final_action = "BUY"
            final_sizing = oracle_proposal.get("sizing", 50000.0)
            final_strategy = "HEDGED_COLLAR"
            notes = "Synthetic Harmonization: Long-Term Compounder under short-term Corrective Wave C. Execute BUY + Buy 5% OTM Protective Put Option."
            
        elif oracle_action == "BUY" and hunter_action == "HOLD":
            # Value says Buy, Hunter is neutral
            execution_approved = True
            final_action = "BUY"
            final_sizing = 5000.0  # Base size since no short term momentum confirmation
            final_strategy = "SCALE_IN"
            notes = "Value confirmed but short-term momentum is neutral. Scaling in with baseline ₹5k allocation."
            
        else:
            # Oracle says HOLD or SELL
            execution_approved = False
            final_action = "HOLD"
            final_sizing = 0.0
            final_strategy = "CASH_RESERVE"
            notes = "Failed to pass primary institutional value hurdles. Maintaining cash reserve."
            
        final_allocation = {
            "symbol": symbol,
            "execution_approved": execution_approved,
            "action": final_action,
            "sizing": final_sizing,
            "strategy": final_strategy,
            "notes": notes,
            "oracle_confidence": oracle_proposal.get("confidence", 0.0),
            "hunter_confidence": hunter_proposal.get("confidence", 0.0)
        }
        
        logging.info(f"   Final Harmony: {symbol} -> {final_strategy} (Approved: {execution_approved}, Size: ₹{final_sizing:,.0f})")
        return final_allocation
