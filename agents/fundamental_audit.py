import logging
import os
from typing import Dict, Any, List
from openai import OpenAI
from core.state import SovereignState

class FundamentalAuditAgent:
    """
    Synthesizes web search results (news/fraud) using OpenAI to provide 
    a final fundamental grade and narrative for a stock.
    """
    def __init__(self):
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o")

    def analyze_sovereign_setup(self, symbol: str, news_data: str, fraud_data: str, tech_metrics: Dict[str, Any]) -> Dict[str, Any]:
        logging.info(f"PERFORMING RUTHLESS SOVEREIGN AUDIT FOR {symbol}...")
        
        prompt = f"""
        Analyze the following data for the Indian stock {symbol}. 
        You are a Ruthless Hedge Fund Auditor. Your goal is to VETO any setup that has even a hint of weakness.
        
        --- TECHNICAL RIGOR DATA ---
        Distance to Overhead Resistance: {tech_metrics.get('dist_to_high', 0):.1f}%
        Momentum Velocity: {tech_metrics.get('velocity', 0):.2f}
        Current Phase: {tech_metrics.get('phase', 'Unknown')}
        
        --- FUNDAMENTAL/NEWS DATA ---
        {news_data}
        
        --- FRAUD/AUDIT DATA ---
        {fraud_data}
        
        CRITERIA FOR VETO:
        1. OVERHEAD WALL: If Distance to Resistance is < 5%, it is a 'Trap'. VETO.
        2. RSI DIVERGENCE: If Momentum Velocity is Negative while Price is high, it is a 'Divergence'. VETO.
        3. STATIC ACCUMULATION: If the phase is 'Static' and not 'Ignition', downgrade the grade.
        4. FRAUD/AUDIT: Any auditor resignation or SEBI fine is an immediate F.

        Provide your analysis in JSON format:
        - "narrative": A 1-sentence summary of the 'Truth' of this stock.
        - "red_flags": List any Technical or Fundamental red flags.
        - "grade": A letter grade (A, B, C, D, or F).
        - "sentiment": POSITIVE, NEUTRAL, or NEGATIVE.
        - "action": 'DEPLOY', 'ACCUMULATE', or 'AVOID'.
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            import json
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            logging.error(f"Fundamental Audit AI Error for {symbol}: {e}")
            return {
                "narrative": "Error during AI analysis.",
                "red_flags": [],
                "grade": "N/A",
                "sentiment": "NEUTRAL",
                "catalyst": "Unknown"
            }

def run_fundamental_audit_node(state: SovereignState) -> Dict[str, Any]:
    """
    LangGraph Node for Fundamental Audit. 
    NOTE: In a live environment, this would call a search API. 
    For now, we will store the results in the state for the user to see.
    """
    approved = list(state.get("approved_allocations", {}).keys())
    if not approved:
        return {}

    # We will populate this via a wrapper that provides the search results
    fundamental_reports = state.get("fundamental_reports", {})
    return {"fundamental_reports": fundamental_reports}
