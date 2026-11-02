import os
import logging
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime, time
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class TrapType(Enum):
    NONE = "NONE"
    BULL_EXHAUSTION_WICK = "BULL_EXHAUSTION_WICK"
    BEAR_ABSORPTION_WICK = "BEAR_ABSORPTION_WICK"
    LOW_VOLUME_DRIFT = "LOW_VOLUME_DRIFT"
    OVEREXTENDED_CLIMAX = "OVEREXTENDED_CLIMAX"

class HoldAction(Enum):
    HOLD_MULTI_DAY = "HOLD_MULTI_DAY"         # Conviction >= 80: High confidence BTST carry
    HOLD_INTRADAY = "HOLD_INTRADAY"           # Conviction 50-79: Retain intraday, trail SL to cost, exit by 15:20
    EXIT_OR_LOCK_PROFIT = "EXIT_OR_LOCK_PROFIT" # Conviction < 50: Reversal/trap detected, book profit or exit

@dataclass
class CandleMetrics:
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    vol_sma: float
    vwap: float
    current_time: Optional[datetime] = None

@dataclass
class GuardianAssessment:
    symbol: str
    direction: str  # "LONG" or "SHORT"
    entry_price: float
    current_price: float
    pnl_pct: float
    conviction_score: int
    trap_type: TrapType
    hold_action: HoldAction
    reasons: List[str] = field(default_factory=list)
    action_recommendation: str = ""

class PositionGuardian:
    """
    Position Guardian & Hourly Live Co-Pilot:
    - Analyzes active trades every hour at candle close (10:15, 11:15, 12:15, 13:15, 14:15, 15:00).
    - Detects institutional distribution/absorption wicks (traps).
    - Checks VWAP posture and volume expansion.
    - Computes a Multi-Day Hold Conviction Score (0-100) to advise whether to carry BTST or exit intraday.
    """
    
    def evaluate_position(
        self,
        symbol: str,
        direction: str,
        entry_price: float,
        current_price: float,
        candle: CandleMetrics
    ) -> GuardianAssessment:
        pnl_pct = ((current_price - entry_price) / entry_price * 100.0) if direction == "LONG" else ((entry_price - current_price) / entry_price * 100.0)
        
        candle_range = candle.high - candle.low
        upper_wick = candle.high - max(candle.open, candle.close)
        lower_wick = min(candle.open, candle.close) - candle.low
        body = abs(candle.close - candle.open)
        
        upper_wick_ratio = (upper_wick / candle_range) if candle_range > 0 else 0.0
        lower_wick_ratio = (lower_wick / candle_range) if candle_range > 0 else 0.0
        vol_ratio = (candle.volume / candle.vol_sma) if candle.vol_sma > 0 else 1.0
        
        score = 70  # Baseline neutral conviction
        reasons: List[str] = []
        trap = TrapType.NONE
        
        # 1. Institutional Trap Checks
        if direction == "LONG":
            # Exhaustion Wick Trap: heavy volume rally rejected from highs (upper wick > 40%)
            if upper_wick_ratio >= 0.40 and vol_ratio >= 1.8:
                trap = TrapType.BULL_EXHAUSTION_WICK
                score -= 35
                reasons.append(f"Institutional selling into highs (Upper wick {upper_wick_ratio*100:.1f}%, Vol {vol_ratio:.1f}x)")
            elif current_price < candle.vwap:
                score -= 15
                reasons.append(f"Price below VWAP (LTP {current_price:.2f} < VWAP {candle.vwap:.2f})")
            else:
                score += 10
                reasons.append(f"Firm posture above VWAP ({current_price:.2f} > {candle.vwap:.2f})")
                
            # Continuation strength
            close_to_high = (candle.close - candle.low) / candle_range if candle_range > 0 else 0.5
            if close_to_high >= 0.80 and vol_ratio >= 1.2:
                score += 15
                reasons.append(f"Strong high close ({close_to_high*100:.0f}%) with expanding volume ({vol_ratio:.1f}x)")

        elif direction == "SHORT":
            # Absorption Wick Trap: heavy volume selloff rejected from lows (lower wick > 40%)
            if lower_wick_ratio >= 0.40 and vol_ratio >= 1.8:
                trap = TrapType.BEAR_ABSORPTION_WICK
                score -= 35
                reasons.append(f"Institutional absorption at lows (Lower wick {lower_wick_ratio*100:.1f}%, Vol {vol_ratio:.1f}x)")
            elif current_price > candle.vwap:
                score -= 15
                reasons.append(f"Price above VWAP (LTP {current_price:.2f} > VWAP {candle.vwap:.2f})")
            else:
                score += 10
                reasons.append(f"Firm posture below VWAP ({current_price:.2f} < {candle.vwap:.2f})")
                
            # Breakdown continuation strength
            close_to_low = (candle.high - candle.close) / candle_range if candle_range > 0 else 0.5
            if close_to_low >= 0.80 and vol_ratio >= 1.2:
                score += 15
                reasons.append(f"Strong low close ({close_to_low*100:.0f}%) with expanding volume ({vol_ratio:.1f}x)")

        # 2. Volume Health
        if vol_ratio < 0.6:
            score -= 10
            reasons.append(f"Volume contracting/low participation ({vol_ratio:.2f}x SMA)")
            if trap == TrapType.NONE:
                trap = TrapType.LOW_VOLUME_DRIFT

        # 3. Afternoon BTST Session Boost (after 13:30)
        curr_dt = candle.current_time or datetime.now()
        t = curr_dt.time()
        if t >= time(13, 30) and pnl_pct > 0 and trap == TrapType.NONE:
            score += 10
            reasons.append("Sustained afternoon momentum heading into BTST window")

        # Clamp score 0 to 100
        score = max(0, min(100, score))
        
        # Determine Hold Action
        if trap in [TrapType.BULL_EXHAUSTION_WICK, TrapType.BEAR_ABSORPTION_WICK] or score < 50:
            hold_action = HoldAction.EXIT_OR_LOCK_PROFIT
            recommendation = "🚨 INSTITUTIONAL TRAP DETECTED: Book profit immediately or exit to protect capital."
        elif score >= 80:
            hold_action = HoldAction.HOLD_MULTI_DAY
            recommendation = "🔥 HIGH CONVICTION BTST HOLD: Volume & structure aligned. Safe to carry multi-day."
        else:
            hold_action = HoldAction.HOLD_INTRADAY
            recommendation = "⚡ INTRADAY ONLY: Trail SL to entry breakeven. Square off before 15:20 PM."

        return GuardianAssessment(
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            current_price=current_price,
            pnl_pct=pnl_pct,
            conviction_score=score,
            trap_type=trap,
            hold_action=hold_action,
            reasons=reasons,
            action_recommendation=recommendation
        )
