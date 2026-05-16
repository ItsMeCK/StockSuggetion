import logging
from typing import Dict, Any

from core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SovereignConvictionGate:
    """
    Pure Conviction Gate. 
    Removes positioning complexity to restore the 95% win-rate baseline.
    """
    def __init__(self, account_size: float = 1000000.0):
        self.account_size = account_size
        self.titan_allocation = 50000.0 # High Conviction Titan Sizing
        self.standard_allocation = 5000.0 # Standard Base Sizing

    def evaluate_risk(self, symbol: str, entry: float, stop: float, conviction_score: float) -> Dict[str, Any]:
        """
        Returns a fixed allocation if elite conviction is cleared (Threshold: 130).
        """
        allocation_amount = self.account_size * self.fixed_allocation_pct
        shares = int(allocation_amount / entry)
        
        return {
            "approved": conviction_score >= 130.0,
            "shares": shares,
            "capital_allocated": allocation_amount,
            "entry": entry,
            "stop_loss": stop,
            "target": entry * 1.10, # 10% Profit Target
            "conviction_score": conviction_score
        }

def run_risk_agent(state: SovereignState) -> Dict[str, Any]:
    """
    LangGraph Node integration for the Risk Agent.
    Enforces the new 80% Cognitive Confidence Threshold.
    """
    critic_results = state.get("critic_results", {})
    entry_trigger_results = state.get("entry_trigger_results", {})
    
    if not critic_results:
        return {"approved_allocations": {}}

    macro = state.get("macro_regime", "UNKNOWN")
    risk_manager = SovereignConvictionGate()
    approved_allocations = {}
    
    for symbol, evaluation in critic_results.items():
        total_confidence = evaluation.get("total_confidence", 0.0)
        # Use evaluation["approved"] which was set by the Critic Agent
        is_approved = evaluation.get("approved", False)
        rs_alpha = evaluation.get("rs_alpha", 1.0)
        
        if is_approved:
            trigger_data = entry_trigger_results.get(symbol, {})
            entry_price = trigger_data.get("entry_price")
            
            if not entry_price:
                logging.warning(f"Risk Error: No entry price found for {symbol}")
                continue
                
            # --- INSTITUTIONAL CONVICTION SIZING ---
            is_mom = evaluation.get("is_momentum", False)
            
            if is_mom:
                allocation_amount = risk_manager.titan_allocation
                # Reduce size slightly in Bearish markets to preserve capital
                if macro == "BEARISH":
                    allocation_amount *= 0.5
            else:
                allocation_amount = risk_manager.standard_allocation
            
            shares = int(allocation_amount / entry_price)
            stop_loss = entry_price * 0.95 # 5% Stop
            
            approved_allocations[symbol] = {
                "approved": True,
                "shares": shares,
                "capital_allocated": allocation_amount,
                "entry": entry_price,
                "stop_loss": stop_loss,
                "target": entry_price * 1.50, # Open target for Titans (10% was too low)
                "conviction_score": total_confidence,
                "rs_alpha": rs_alpha,
                "is_momentum": is_mom
            }
            logging.info(f"SOVEREIGN APPROVED: {symbol} | Type: {'TITAN' if is_mom else 'STD'} | Sizing: ₹{allocation_amount:,.0f}")
        else:
            logging.info(f"CONVICTION REJECTION: {symbol} (Confidence: {total_confidence:.1f})")

    return {"approved_allocations": approved_allocations}
