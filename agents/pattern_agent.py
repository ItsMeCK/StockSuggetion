import json
import logging
from typing import Dict, Any
from pathlib import Path

import polars as pl

from core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class VisionPatternAgent:
    """
    Deterministic institutional auditor for candidate setups.

    This used to be an LLM call (GPT-4o) named "Vision" - but it never actually
    read a chart image. It pasted a text table of OHLCV numbers into a prompt
    and asked a text-completion model to eyeball volume surges, breakouts, and
    wick exhaustion. Since the input was already pure numbers, those same
    checks are now exact arithmetic: free, deterministic, and reproducible
    across backtest reruns (no LLM run-to-run variance).
    """
    def __init__(self):
        self.rules = self._load_context_rules()
        self.cache_path = Path(__file__).parent.parent / "vision_cache.json"
        self.cache = self._load_cache()

    def _load_cache(self) -> Dict[str, Any]:
        if self.cache_path.exists():
            try:
                with open(self.cache_path, 'r') as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_cache(self):
        try:
            with open(self.cache_path, 'w') as f:
                json.dump(self.cache, f, indent=4)
        except Exception as e:
            logging.error(f"Failed to save vision cache: {e}")

    def _load_context_rules(self) -> Dict[str, Any]:
        """Loads the MASTER institutional rules (v2.3)."""
        core_dir = Path(__file__).parent.parent / "core"
        rules_path = core_dir / "context_rules_3.json"

        try:
            with open(rules_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logging.error(f"Rulebook not found at {rules_path}. Ensure context_rules_2.json exists.")
            return {}

    def _score_setup(self, rows) -> Dict[str, Any]:
        """
        Replicates the original prompt's three institutional-auditor checks
        with exact arithmetic on the same OHLCV rows that used to get pasted
        into a text prompt:
          1. SPONSORSHIP: volume surge vs 20-day average (primary driver)
          2. BREAKOUT VALIDITY: close vs prior 20-day high, confirmed by ATR expansion
          3. RISK AUDIT: upper-wick rejection ("Pinocchio Bar" exhaustion)
        """
        # rows arrive DESC (newest first) from the DB query; reverse to chronological order
        chrono = list(reversed(rows))
        df = pl.DataFrame(
            [(r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])) for r in chrono],
            schema=["time", "open", "high", "low", "close", "volume"],
            orient="row"
        )

        df = df.with_columns([
            pl.col("volume").rolling_mean(window_size=20).alias("vol_avg_20"),
            pl.col("high").shift(1).rolling_max(window_size=20).alias("high_20_prior"),
            pl.max_horizontal([
                (pl.col("high") - pl.col("low")),
                (pl.col("high") - pl.col("close").shift(1)).abs(),
                (pl.col("low") - pl.col("close").shift(1)).abs()
            ]).alias("true_range")
        ])
        df = df.with_columns([
            pl.col("true_range").rolling_mean(window_size=3).alias("atr_3"),
            pl.col("true_range").rolling_mean(window_size=20).alias("atr_20")
        ])

        latest = df.tail(1).to_dicts()[0]
        close = latest["close"]
        high = latest["high"]
        low = latest["low"]
        volume = latest["volume"]

        vol_avg_20 = latest["vol_avg_20"] or volume or 1.0
        high_20_prior = latest["high_20_prior"] if latest["high_20_prior"] is not None else high
        atr_3 = latest["atr_3"] if latest["atr_3"] is not None else 0.0
        atr_20 = latest["atr_20"] if latest["atr_20"] is not None else atr_3

        volume_ratio = volume / vol_avg_20 if vol_avg_20 else 1.0
        breakout = close > high_20_prior
        atr_expansion = atr_3 > atr_20
        upper_wick_pct = (high - close) / (high - low) if high > low else 0.0

        # 1. Sponsorship component - primary driver, same emphasis as the original rubric
        if volume_ratio >= 3.0:
            vol_component = 100.0
        elif volume_ratio >= 1.5:
            vol_component = 80.0
        elif volume_ratio >= 1.0:
            vol_component = 65.0
        else:
            vol_component = 40.0

        # 2. Breakout validity component
        if breakout and atr_expansion:
            breakout_component = 15.0
        elif breakout:
            breakout_component = 5.0
        else:
            breakout_component = -10.0

        # 3. Exhaustion / false-breakout penalty (Pinocchio Bar check)
        dq_flag = "None"
        if upper_wick_pct >= 0.5:
            exhaustion_penalty = 30.0
            if breakout:
                dq_flag = "pinocchio_bar"
        elif upper_wick_pct >= 0.35:
            exhaustion_penalty = 15.0
        else:
            exhaustion_penalty = 0.0

        if dq_flag == "None" and breakout and volume_ratio < 1.2:
            dq_flag = "false_breakout"

        vision_score = max(0.0, min(100.0, vol_component + breakout_component - exhaustion_penalty))

        # HARD VETO if a disqualification flag is raised (same cap the LLM path used)
        if dq_flag != "None":
            vision_score = min(vision_score, 65.0)

        reason = (
            f"Deterministic audit: Volume {volume_ratio:.2f}x 20d avg, "
            f"Breakout={'Yes' if breakout else 'No'} (ATR expansion={'Yes' if atr_expansion else 'No'}), "
            f"Upper wick={upper_wick_pct:.0%} of range."
        )

        return {
            "vision_score": int(round(vision_score)),
            "reason": reason,
            "disqualification_flag": dq_flag
        }

    def analyze_chart(self, symbol: str, pattern_hint: str, target_date: str = None) -> Dict[str, Any]:
        """
        Deterministically audits the institutional validity of a setup using
        exact arithmetic on OHLCV data, with symbol_date caching.
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

            if target_date:
                query = "SELECT time, open, high, low, close, volume FROM daily_ohlcv WHERE symbol = %s AND time::date <= %s ORDER BY time DESC LIMIT 60"
                cur.execute(query, (symbol, target_date))
            else:
                query = "SELECT time, open, high, low, close, volume FROM daily_ohlcv WHERE symbol = %s ORDER BY time DESC LIMIT 60"
                cur.execute(query, (symbol,))

            rows = cur.fetchall()
            cur.close()
            conn.close()

            if not rows:
                logging.warning(f"No historical data found for {symbol} up to {target_date}")
                return {"vision_approved": True, "vision_score": 50, "identified_pattern": "unknown", "reason": "No data"}

            # Create a unique cache key based on symbol and the most recent timestamp in the data
            latest_date = rows[0][0].strftime('%Y-%m-%d')
            cache_key = f"{symbol}_{latest_date}"

            if cache_key in self.cache:
                logging.info(f"CACHE HIT: Retrieving audit for {symbol} on {latest_date}")
                return self.cache[cache_key]

            if len(rows) < 21:
                vision_result = {
                    "vision_approved": True,
                    "vision_score": 50,
                    "identified_pattern": pattern_hint,
                    "reason": f"Insufficient history ({len(rows)} bars) for deterministic audit; neutral score assigned.",
                    "disqualification_flag": "None",
                    "whipsaw_risk": "High",
                    "cached_at": latest_date
                }
                self.cache[cache_key] = vision_result
                self._save_cache()
                return vision_result

            logging.info(f"Running deterministic institutional audit for {symbol} (no cache found for {latest_date})...")
            scored = self._score_setup(rows)
            vision_score = scored["vision_score"]

            vision_result = {
                "vision_approved": vision_score >= 70,
                "vision_score": vision_score,
                "identified_pattern": pattern_hint,
                "reason": scored["reason"],
                "disqualification_flag": scored["disqualification_flag"],
                "whipsaw_risk": "Low" if vision_score > 75 else "High",
                "cached_at": latest_date
            }

            # Save to cache
            self.cache[cache_key] = vision_result
            self._save_cache()

            return vision_result

        except Exception as e:
            logging.error(f"Deterministic Vision Analysis failed for {symbol}: {e}")
            raise RuntimeError(f"Deterministic Vision Analysis failed: {e}") from e

def run_pattern_agent(state: SovereignState) -> Dict[str, Any]:
    """
    LangGraph Node integration for the Pattern Agent.
    """
    heuristic_flags = state.get("heuristic_flags", {})
    experience_warnings = state.get("experience_warnings", {})
    
    vision_agent = VisionPatternAgent()
    vision_validations = {}
    target_date = state.get("target_date")
    candidates = state.get("candidates", [])
    
    # Initialize a new agent_scores map to return (LangGraph reducer pattern)
    agent_scores = state.get("agent_scores", {})
    new_agent_scores = {k: v.copy() for k, v in agent_scores.items()}
    
    for symbol in candidates:
        # Skip if the Meta-Gate vetoed this setup
        if symbol in experience_warnings and len(experience_warnings[symbol]) > 0:
            logging.info(f"Skipping Vision analysis for {symbol} due to Meta-Gate VETO.")
            continue
            
        # Get hint if available from DTW
        pattern = heuristic_flags.get(symbol, {}).get("identified_pattern", "unknown")
        
        # Execute Vision Analysis
        vision_result = vision_agent.analyze_chart(symbol, pattern, target_date)
        vision_validations[symbol] = vision_result
        
        # Update local agent_scores
        if symbol not in new_agent_scores:
            new_agent_scores[symbol] = {}
        new_agent_scores[symbol]["vision"] = float(vision_result.get("vision_score", 50))
        
        logging.info(f"FINAL VISION SCORE for {symbol}: {new_agent_scores[symbol]['vision']}")
        
        if vision_result["vision_approved"]:
            logging.info(f"VISION APPROVED: {symbol} is a valid {pattern} setup.")
        else:
            logging.warning(f"VISION REJECTED: {symbol} - {vision_result['reason']}")
    
    return {"vision_validations": vision_validations, "agent_scores": new_agent_scores}

if __name__ == "__main__":
    # Test execution
    mock_state = SovereignState(
        heuristic_flags={
            "RELIANCE": {"identified_pattern": "rectangle"},
            "INFY": {"identified_pattern": "ascending_triangle"},
            "TCS": {"identified_pattern": "ascending_triangle"}
        },
        experience_warnings={
            "TCS": ["Vetoed by Experience DB"] # TCS should be skipped
        }
    )
    result = run_pattern_agent(mock_state)
    print(f"Delta State Update: {result}")
