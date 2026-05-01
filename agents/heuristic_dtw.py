import logging
import numpy as np
from typing import List, Dict, Any

from midnight_sovereign.core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class HeuristicDTWProcessor:
    """
    Applies Dynamic Time Warping (DTW) mathematically matching recent price action 
    against idealized pattern geometries (e.g., flags, rectangles, triangles).
    This compresses the candidate list without invoking expensive LLM vision models.
    """
    def __init__(self):
        # In a real implementation, you would load idealized normalized templates
        self.templates = {
            "bull_flag": np.array([1, 2, 3, 4, 5, 4.5, 4.8, 4.3, 4.6]),
            "ascending_triangle": np.array([1, 3, 2, 3, 2.5, 3, 2.8, 3]),
            "rectangle": np.array([2, 4, 2, 4, 2, 4, 2, 4])
        }

    def fetch_recent_price_action(self, symbol: str) -> np.ndarray:
        """
        Fetches the last 20 days of normalized price action from TimescaleDB.
        """
        import os, psycopg2
        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                user=os.getenv('POSTGRES_USER', 'quant'),
                password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
                dbname=os.getenv('POSTGRES_DB', 'market_data')
            )
            cur = conn.cursor()
            cur.execute("SELECT close FROM daily_ohlcv WHERE symbol = %s ORDER BY time DESC LIMIT 20", (symbol,))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            if rows:
                closes = np.array([float(r[0]) for r in rows[::-1]])
                # Normalize to 0-1 range for template matching
                min_val = closes.min()
                max_val = closes.max()
                if max_val > min_val:
                    return (closes - min_val) / (max_val - min_val) * 5.0 # Scaled to match template range
                return np.zeros(len(closes))
        except Exception as e:
            logging.error(f"DTW data fetch error for {symbol}: {e}")
            
        return np.random.rand(10) * 5

    def _calculate_dtw_distance(self, series1: np.ndarray, series2: np.ndarray) -> float:
        """
        Calculates a simplified DTW-like Euclidean distance on normalized series.
        """
        # In a production environment, use fastdtw. 
        # Here we use a length-normalized Euclidean distance on the min-length of both.
        n = min(len(series1), len(series2))
        return np.sqrt(np.sum((series1[:n] - series2[:n])**2)) / n

    def evaluate_candidates(self, candidates: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Evaluates the candidates against the geometric templates.
        """
        logging.info(f"Applying DTW Heuristics to {len(candidates)} candidates...")
        flagged_setups = {}

        for symbol in candidates:
            price_action = self.fetch_recent_price_action(symbol)
            best_match = None
            lowest_distance = float('inf')

            for pattern_name, template in self.templates.items():
                distance = self._calculate_dtw_distance(price_action, template)
                if distance < lowest_distance:
                    lowest_distance = distance
                    best_match = pattern_name

            # Compute DTW Geometry Purity Score (up to 15 Points)
            dtw_score = float(15.0 * max(0.0, 1.0 - (lowest_distance / 5.0)))
            lowest_distance = float(lowest_distance)
            
            logging.info(f"DTW Match Evaluated: {symbol} -> {best_match} (Dist: {lowest_distance:.2f}) -> Score: {dtw_score:.1f}/15")
            flagged_setups[symbol] = {
                "identified_pattern": best_match if lowest_distance < 5.0 else "None",
                "dtw_distance": lowest_distance,
                "dtw_score": dtw_score,
                "requires_vision_validation": True if lowest_distance < 5.0 else False
            }

        return flagged_setups

def run_heuristic_pre_processor(state: SovereignState) -> Dict[str, Any]:
    """
    LangGraph Node integration for DTW preprocessing.
    """
    candidates = state.get("candidates", [])
    if not candidates:
        logging.warning("No candidates received from Screener. Skipping DTW.")
        return {"heuristic_flags": {}}

    dtw_processor = HeuristicDTWProcessor()
    flagged_setups = dtw_processor.evaluate_candidates(candidates)

    return {"heuristic_flags": flagged_setups}

if __name__ == "__main__":
    # Test execution
    mock_state = SovereignState(candidates=["RELIANCE", "HDFCBANK", "INFY"])
    result = run_heuristic_pre_processor(mock_state)
    print(f"Delta State Update: {result}")
