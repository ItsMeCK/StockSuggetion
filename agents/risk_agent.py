import logging
from typing import Dict, Any

from midnight_sovereign.core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RiskAndPositionManager:
    """
    Calculates Fractional Kelly sizing (0.25 * f*).
    Enforces 2% trade risk and 40% total portfolio heat limits.
    Accounts for STT friction.
    """
    def __init__(self, account_size: float = 1000000.0, current_heat: float = 0.25):
        self.account_size = account_size
        self.current_portfolio_heat = current_heat # 25% of account currently at risk
        self.max_trade_risk = 0.02 # 2% max per trade
        self.max_portfolio_heat = 0.40 # 40% max total risk
        
        # 2026 STT assumptions + brokerage
        self.friction_multiplier = 1.002 # Assume 0.2% total friction on round trip

    def calculate_fractional_kelly(self, win_rate: float, win_loss_ratio: float) -> float:
        """
        Calculates f* and returns the Fractional Kelly (0.25 of f*).
        f* = [ p * (R+1) - 1 ] / R
        """
        if win_loss_ratio <= 0:
            return 0.0
            
        f_star = (win_rate * (win_loss_ratio + 1) - 1) / win_loss_ratio
        
        if f_star <= 0:
            return 0.0 # Negative edge, do not trade
            
        fractional_kelly = f_star * 0.25
        return fractional_kelly

    def size_position(self, symbol: str, entry_price: float, stop_loss: float, target_price: float) -> Dict[str, Any]:
        """
        Determines the exact capital allocation and shares to buy based on risk parameters.
        """
        logging.info(f"Calculating risk parameters for {symbol}...")
        
        if entry_price <= stop_loss:
            return {"approved": False, "reason": "Invalid entry/stop logic."}
            
        # 1. Friction Check (Reward-to-Risk >= 3:1 post-friction)
        gross_reward = target_price - entry_price
        gross_risk = entry_price - stop_loss
        
        net_reward = gross_reward - (entry_price * (self.friction_multiplier - 1.0))
        net_risk = gross_risk + (entry_price * (self.friction_multiplier - 1.0))
        
        r_r_ratio = net_reward / net_risk
        
        if r_r_ratio < 3.0:
            return {"approved": False, "reason": f"Failed friction check. R:R is {r_r_ratio:.2f} (Needs > 3.0)"}
            
        # 2. Fractional Kelly Sizing
        # Mocking historical stats from Experience DB (e.g., 45% win rate, 3.5 W/L ratio)
        historical_win_rate = 0.45
        historical_win_loss_ratio = 3.5
        
        f_kelly = self.calculate_fractional_kelly(historical_win_rate, historical_win_loss_ratio)
        
        if f_kelly <= 0:
            return {"approved": False, "reason": "Negative edge detected by Kelly."}
            
        # 3. Apply Hard Constraints
        # Calculate risk amount based on Kelly, but capped at 2% max_trade_risk
        applied_risk_pct = min(f_kelly, self.max_trade_risk)
        
        # Calculate position size
        capital_at_risk = self.account_size * applied_risk_pct
        shares_to_buy = int(capital_at_risk / net_risk)
        total_allocation = shares_to_buy * entry_price
        
        # 4. Portfolio Heat Check
        if self.current_portfolio_heat + applied_risk_pct > self.max_portfolio_heat:
            return {"approved": False, "reason": "Portfolio heat limit (40%) exceeded."}
            
        return {
            "approved": True,
            "shares": shares_to_buy,
            "capital_allocated": round(total_allocation, 2),
            "risk_pct": round(applied_risk_pct * 100, 2),
            "entry": entry_price,
            "stop_loss": stop_loss,
            "target": target_price,
            "rr_ratio": round(r_r_ratio, 2)
        }

def run_risk_agent(state: SovereignState) -> Dict[str, Any]:
    """
    LangGraph Node integration for the Risk Agent.
    """
    vision_validations = state.get("vision_validations", {})
    
    base_scores = state.get("base_scores", {})
    heuristic_flags = state.get("heuristic_flags", {})
    
    # We need access to latest candidate data
    # Mocking EOD data endpoints for portfolio selection
    approved_allocations = {}
    conviction_scores = {}
    
    candidate_list = state.get("candidates", [])
    risk_manager = RiskAndPositionManager()
    
    # Score and size all candidates
    unsorted_candidates = []
    entry_trigger_results = state.get("entry_trigger_results", {})
    
    for symbol in candidate_list:
        base_score = base_scores.get(symbol, 30.0)
        dtw_score = heuristic_flags.get(symbol, {}).get("dtw_score", 7.5)
        
        # MOCK dynamic entry/stop values
        entry = 500.0
        stop = 475.0
        target = 600.0 # 1:4 R:R default
        
        allocation = risk_manager.size_position(symbol, entry, stop, target)
        
        rr_ratio = allocation.get("rr_ratio", 3.5)
        if rr_ratio >= 5.0:
            rr_score = 25.0
        elif rr_ratio >= 4.0:
            rr_score = 18.0
        else:
            rr_score = 10.0
            
        final_score = base_score + dtw_score + rr_score
        conviction_scores[symbol] = final_score
        
        unsorted_candidates.append({
            "symbol": symbol,
            "score": final_score,
            "allocation": allocation
        })
        
    # Deployment Loop: Top 3 cap, max 40% portfolio heat
    unsorted_candidates.sort(key=lambda x: x["score"], reverse=True)
    
    current_heat = 0.25
    max_heat = 0.40
    placed_trades = 0
    
    for item in unsorted_candidates:
        if placed_trades >= 3:
            break
            
        symbol = item["symbol"]
        trigger_data = entry_trigger_results.get(symbol, {})
        if not trigger_data.get("approved", False):
            continue
            
        alloc = item["allocation"]
        
        if not alloc.get("approved", False):
            continue
            
        trade_risk_pct = alloc.get("risk_pct", 0.0) / 100.0
        
        if current_heat + trade_risk_pct <= max_heat:
            current_heat += trade_risk_pct
            placed_trades += 1
            approved_allocations[symbol] = alloc
            logging.info(f"CONVICTION APPROVED: {symbol} | Score: {item['score']:.1f}/100 | Total Heat: {current_heat*100:.1f}%")
        else:
            logging.warning(f"HEAT LIMIT REACHED: Skipping {symbol} | Heat would breach 40%")
            break
            
    return {"approved_allocations": approved_allocations, "conviction_scores": conviction_scores}

if __name__ == "__main__":
    # Test execution
    mock_state = SovereignState(
        vision_validations={
            "RELIANCE": {"vision_approved": True},
            "INFY": {"vision_approved": False} # Should be skipped
        }
    )
    result = run_risk_agent(mock_state)
    print(f"Delta State Update: {result}")
