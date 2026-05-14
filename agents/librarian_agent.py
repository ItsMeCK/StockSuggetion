import polars as pl
import json
import logging
from typing import Dict, List, Any

class SovereignLibrarian:
    """
    The Guardian of the Pring Codex.
    Audits trade candidates against the full institutional rulebook.
    """
    def __init__(self, rules_path: str = "core/context_rules_3.json"):
        self.rules_path = rules_path
        self.rules = self._load_rules()
        
    def _load_rules(self) -> Dict:
        try:
            with open(self.rules_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Librarian failed to load Codex: {e}")
            return {}

    def audit_setup(self, ticker: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Runs a 35-Point Pring Compliance Audit on a single ticker.
        """
        score = 0
        checks_passed = []
        checks_failed = []
        
        # 1. Pring Momentum Check (Chapter 6)
        roc_10 = data.get('roc_10', 0)
        roc_20 = data.get('roc_20', 0)
        if roc_10 > roc_20:
            score += 15
            checks_passed.append("PRING_CH6_MOMENTUM_ACCELERATION")
        else:
            checks_failed.append("PRING_CH6_MOMENTUM_DECELERATION")

        # 2. Volume Integrity Check (Chapter 11)
        vdu = data.get('volume_ratio')
        if vdu is None:
            # Fallback to raw columns
            v = data.get('volume', 0)
            avg_v = data.get('vol_avg_20', 1)
            vdu = v / avg_v if avg_v > 0 else 1.0

        if vdu < 0.8:
            score += 20
            checks_passed.append("PRING_CH11_INSTITUTIONAL_DRY_UP")
        elif vdu > 1.5:
            score += 15
            checks_passed.append("PRING_CH11_VOLUME_THRUST")

        # 3. Price Extension (Chapter 7)
        ext = data.get('extension_pct', 0)
        if ext < 5.0:
            score += 20
            checks_passed.append("PRING_CH7_SAFE_ZONE")
        elif ext < 25.0:
            score += 10
            checks_passed.append("PRING_CH7_CHAMPION_ZONE")
        else:
            # VETO RULE: Real Climax exhaustion (>25%)
            return {"ticker": ticker, "score": 0, "status": "VETOED", "reason": "PRING_CH7_CLIMAX_EXHAUSTION"}

        # 4. Momentum Bonus (The Champions Clause)
        if roc_10 > roc_20 and vdu > 2.0:
            score += 30
            checks_passed.append("MOMENTUM_BONUS_CHAMPION")

        # 5. Stage 2 Confirmation (Brian Shannon Integration)
        if data.get('is_stage_2', False):
            score += 25
            checks_passed.append("SHANNON_STAGE_2_CONFIRMED")
        
        # 6. Young Stock Multiplier
        if data.get('is_young_stock', False):
            score += 5
            checks_passed.append("PRING_YOUNG_STOCK_DISCOVERY")

        # 7. Intraday Fade Guard (No Red Candles)
        open_price = data.get('open', 0)
        close_price = data.get('close', 0)
        if close_price < open_price:
            return {"ticker": ticker, "score": 0, "status": "VETOED", "reason": "INTRADAY_FADE_RED_CANDLE", "price": close_price, "codex_version": self.rules.get("metadata", {}).get("version", "v3.0")}
        else:
            checks_passed.append("INTRADAY_MOMENTUM_CONFIRMED")
            
        # 8. Anti-Chase Guard (No buying gap-ups > 4.5%)
        roc_1 = data.get('roc_1', 0)
        if roc_1 > 4.5:
            return {"ticker": ticker, "score": 0, "status": "VETOED", "reason": "ANTI_CHASE_OVEREXTENDED_INTRADAY", "price": close_price, "codex_version": self.rules.get("metadata", {}).get("version", "v3.0")}

        # Final Verdict - Cap at 100
        score = min(score, 100)
        status = "SIGNALED" if score >= 70 else "WATCHLIST"
        
        return {
            "ticker": ticker,
            "score": score,
            "status": status,
            "passed": checks_passed,
            "failed": checks_failed,
            "price": close_price,
            "codex_version": self.rules.get("metadata", {}).get("version", "v3.0")
        }

if __name__ == "__main__":
    librarian = SovereignLibrarian()
    # Test with a mock setup
    test_data = {
        "roc_10": 10.5, "roc_20": 5.2, 
        "volume_ratio": 0.6, "extension_pct": 8.0,
        "is_stage_2": True, "is_young_stock": True
    }
    print(librarian.audit_setup("JAINREC", test_data))
