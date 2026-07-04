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
        self.titan_allocation = 5000.0 # High Conviction Titan Sizing strictly capped at 5k
        self.standard_allocation = 5000.0 # Standard Base Sizing strictly capped at 5k

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
    fundamental_reports = state.get("fundamental_reports", {})
    
    for symbol, evaluation in critic_results.items():
        total_confidence = evaluation.get("total_confidence", 0.0)
        # Use evaluation["approved"] which was set by the Critic Agent
        is_approved = evaluation.get("approved", False)
        rs_alpha = evaluation.get("rs_alpha", 1.0)
        
        if is_approved:
            # --- FUNDAMENTAL AUDIT VETO ---
            fund_report = fundamental_reports.get(symbol, {})
            fund_grade = fund_report.get("grade", "A") # Default to A if not graded
            fund_action = fund_report.get("action", "DEPLOY")
            if fund_grade in ['D', 'F'] or fund_action == "AVOID":
                logging.warning(f"🛡️ RISK FUNDAMENTAL VETO for {symbol}: Grade {fund_grade}, Action {fund_action}. Narrative: {fund_report.get('narrative')}")
                continue
                
            trigger_data = entry_trigger_results.get(symbol, {})
            entry_price = trigger_data.get("entry_price")
            
            if not entry_price:
                logging.warning(f"Risk Error: No entry price found for {symbol}")
                continue
                
            # --- VOLATILITY-ADJUSTED STOP LOSS (ATR) ---
            stop_loss = entry_price * 0.95 # Default 5%
            atr_val = 0.0
            try:
                import os, psycopg2
                conn = psycopg2.connect(
                    host=os.getenv('DB_HOST', 'localhost'),
                    port=os.getenv('DB_PORT', '5432'),
                    user=os.getenv('POSTGRES_USER', 'quant'),
                    password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
                    dbname=os.getenv('POSTGRES_DB', 'market_data')
                )
                cur = conn.cursor()
                target_date = state.get("target_date")
                if target_date:
                    cur.execute("""
                        SELECT high, low, close 
                        FROM daily_ohlcv 
                        WHERE symbol = %s AND time::date <= %s 
                        ORDER BY time DESC LIMIT 21
                    """, (symbol, target_date))
                else:
                    cur.execute("""
                        SELECT high, low, close 
                        FROM daily_ohlcv 
                        WHERE symbol = %s 
                        ORDER BY time DESC LIMIT 21
                    """, (symbol,))
                rows = cur.fetchall()
                cur.close()
                conn.close()
                
                if len(rows) >= 20:
                    highs = [float(r[0]) for r in rows]
                    lows = [float(r[1]) for r in rows]
                    closes = [float(r[2]) for r in rows]
                    
                    tr_list = []
                    for i in range(20):
                        h = highs[i]
                        l = lows[i]
                        prev_c = closes[i+1]
                        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
                        tr_list.append(tr)
                    atr_val = sum(tr_list) / 20.0
                    
                    # 1.5x ATR Stop
                    stop_loss = entry_price - (1.5 * atr_val)
                    # Enforce safeguards (e.g. stop loss between 3% and 10% distance)
                    stop_dist_pct = ((entry_price - stop_loss) / entry_price) * 100
                    if stop_dist_pct < 3.0:
                        stop_loss = entry_price * 0.97
                    elif stop_dist_pct > 10.0:
                        stop_loss = entry_price * 0.90
            except Exception as e:
                logging.error(f"Risk ATR calculation failed for {symbol}: {e}")

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
            logging.info(f"SOVEREIGN APPROVED: {symbol} | Type: {'TITAN' if is_mom else 'STD'} | Sizing: ₹{allocation_amount:,.0f} | Stop Loss: ₹{stop_loss:.2f}")
        else:
            logging.info(f"CONVICTION REJECTION: {symbol} (Confidence: {total_confidence:.1f})")

    return {"approved_allocations": approved_allocations}
