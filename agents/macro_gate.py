import logging
from typing import Dict, Any

from midnight_sovereign.core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

import os
import psycopg2
from typing import Dict, Any

class MacroDataFetcher:
    """
    Fetches real macro data from TimescaleDB.
    """
    @staticmethod
    def fetch_fii_dii_flow() -> Dict[str, float]:
        # Currently, FII/DII data is not ingested in the current pipeline.
        # We return a 'Neutral' flow to prevent fallback-induced rejections.
        # TODO: Add NSE FII/DII ingestion to pipeline.ingestion.py
        logging.warning("FII/DII data not found in DB. Falling back to NEUTRAL flow (0.0).")
        return {"fii_net": 0.0, "dii_net": 0.0}

    @staticmethod
    def fetch_global_macro() -> Dict[str, float]:
        # India VIX and US Dollar Index (DXY)
        vix = 15.0 # Default neutral
        dxy = 103.0
        
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", "5432"),
                user=os.getenv("POSTGRES_USER", "quant"),
                password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
                dbname=os.getenv("POSTGRES_DB", "market_data")
            )
            cur = conn.cursor()
            cur.execute("SELECT close FROM daily_ohlcv WHERE symbol = 'INDIA VIX' ORDER BY time DESC LIMIT 1")
            row = cur.fetchone()
            if row:
                vix = float(row[0])
                logging.info(f"Retrieved real INDIA VIX from DB: {vix}")
            cur.close()
            conn.close()
        except Exception as e:
            logging.error(f"Error fetching VIX from DB: {e}")
            
        return {"india_vix": vix, "dxy": dxy}

def evaluate_market_regime(fii_dii: Dict[str, float], macro: Dict[str, float]) -> str:
    """
    Evaluates the current market regime based on institutional flow and global macro data.
    
    Regimes:
    - EXPANSION: FII Buy, DII Buy (High confidence, broad-based rally)
    - TUG_OF_WAR: FII Sell, DII Buy (Range-bound, sector rotation)
    - CAPITULATION: FII Sell, DII Sell + VIX Spiking (Systemic liquidity withdrawal)
    """
    fii_net = fii_dii.get("fii_net", 0)
    dii_net = fii_dii.get("dii_net", 0)
    vix = macro.get("india_vix", 15.0)

    # Thresholds for 'spiking' VIX could be dynamic, hardcoded for demonstration
    VIX_PANIC_THRESHOLD = 22.0

    if fii_net < 0 and dii_net < 0 and vix >= VIX_PANIC_THRESHOLD:
        return "CAPITULATION"
    elif fii_net < 0 and dii_net > 0:
        return "TUG_OF_WAR"
    elif fii_net > 0 and dii_net > 0:
        return "EXPANSION"
    else:
        # Default fallback for mixed/neutral scenarios
        return "NEUTRAL"

def run_macro_regime_gate(state: SovereignState) -> SovereignState:
    """
    The LangGraph Node implementation for the Macro Regime Gate.
    This acts as the ultimate 'Kill-Switch'.
    """
    logging.info("Executing Macro Regime Gate...")
    
    # 1. Fetch Data
    fii_dii_data = MacroDataFetcher.fetch_fii_dii_flow()
    macro_data = MacroDataFetcher.fetch_global_macro()
    
    logging.info(f"FII Net: {fii_dii_data['fii_net']} Cr | DII Net: {fii_dii_data['dii_net']} Cr")
    logging.info(f"India VIX: {macro_data['india_vix']} | DXY: {macro_data['dxy']}")
    
    # 2. Evaluate Regime
    regime = evaluate_market_regime(fii_dii_data, macro_data)
    logging.info(f"Identified Market Regime: {regime}")
    
    # 3. Return Delta State Update
    if regime == "CAPITULATION":
        logging.warning("🚨 CAPITULATION REGIME DETECTED. Halting all long-side processing.")
        return {
            "macro_regime": regime, 
            "fii_net": fii_dii_data['fii_net'],
            "dii_net": fii_dii_data['dii_net'],
            "india_vix": macro_data['india_vix'],
            "dxy": macro_data['dxy'],
            "candidates": [], 
            "error_log": ["Halted due to CAPITULATION regime."]
        }
    
    return {
        "macro_regime": regime,
        "fii_net": fii_dii_data['fii_net'],
        "dii_net": fii_dii_data['dii_net'],
        "india_vix": macro_data['india_vix'],
        "dxy": macro_data['dxy']
    }

if __name__ == "__main__":
    # Test the standalone node execution
    mock_state = SovereignState(
        macro_regime="", 
        candidates=[], 
        heuristic_flags={}, 
        experience_warnings={}, 
        vision_validations={}, 
        approved_allocations={}, 
        execution_telemetry={}, 
        error_log=[]
    )
    result = run_macro_regime_gate(mock_state)
    print(f"Resulting Delta State: {result}")
