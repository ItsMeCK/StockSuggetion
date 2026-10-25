from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

@dataclass
class FiveMinuteBar:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vol_sma_20: float
    sma_20: float

@dataclass
class TriggerSignal:
    triggered: bool
    symbol: str
    direction: str  # "LONG" | "SHORT" | "NONE"
    instrument_type: str  # "CE" | "PE" | "NONE"
    entry_price: float
    reason: str

def evaluate_5m_trigger(
    bar: FiveMinuteBar,
    min_vol_surge: float = 1.8,
    max_ext_pct: float = 2.5
) -> TriggerSignal:
    """
    Evaluates a closed 5-minute bar for immediate momentum ignition or breakdown (Requirement W3).
    - Eliminates the 45-60 minute lag of hourly candles.
    - Anti-FOMO: Rejects overextended moves (|extension| > 2.5%).
    - Requires volume expansion >= 1.8x and candle close near the extreme (>= 0.85).
    """
    vol_surge = (bar.volume / bar.vol_sma_20) if bar.vol_sma_20 > 0 else 1.0
    ext_pct = ((bar.close - bar.sma_20) / bar.sma_20 * 100.0) if bar.sma_20 > 0 else 0.0
    candle_range = bar.high - bar.low

    close_to_high = (bar.close - bar.low) / candle_range if candle_range > 0 else 0.5
    close_to_low = (bar.high - bar.close) / candle_range if candle_range > 0 else 0.5

    # 1. Trading Window Gate (Claude Requirement Point 10)
    # No-entry window in opening 15m (09:15 - 09:30 noise) and last-entry cutoff (after 14:45)
    t = bar.timestamp.time() if hasattr(bar.timestamp, 'time') else None
    if t is not None:
        from datetime import time
        if time(9, 15) <= t < time(9, 30):
            return TriggerSignal(
                triggered=False,
                symbol=bar.symbol,
                direction="NONE",
                instrument_type="NONE",
                entry_price=bar.close,
                reason="Market opening noise window (09:15 - 09:30 AM). Entries barred."
            )
        if t >= time(14, 45):
            return TriggerSignal(
                triggered=False,
                symbol=bar.symbol,
                direction="NONE",
                instrument_type="NONE",
                entry_price=bar.close,
                reason="Intraday cutoff passed (after 14:45 PM). New entries barred."
            )

    # 2. Anti-FOMO extension check
    if abs(ext_pct) > max_ext_pct:
        return TriggerSignal(
            triggered=False,
            symbol=bar.symbol,
            direction="NONE",
            instrument_type="NONE",
            entry_price=bar.close,
            reason=f"Price extension {ext_pct:+.2f}% exceeds {max_ext_pct}% limit"
        )

    # 2. Bullish 5m Momentum Ignition (Long Call Setup)
    if vol_surge >= min_vol_surge and bar.close > bar.open and close_to_high >= 0.85:
        return TriggerSignal(
            triggered=True,
            symbol=bar.symbol,
            direction="LONG",
            instrument_type="CE",
            entry_price=bar.close,
            reason=f"Bullish 5m ignition (Vol Surge: {vol_surge:.2f}x >= {min_vol_surge}x, Close/High: {close_to_high:.2f})"
        )

    # 3. Bearish 5m Momentum Breakdown (Short Put Setup)
    if vol_surge >= min_vol_surge and bar.close < bar.open and close_to_low >= 0.85:
        return TriggerSignal(
            triggered=True,
            symbol=bar.symbol,
            direction="SHORT",
            instrument_type="PE",
            entry_price=bar.close,
            reason=f"Bearish 5m breakdown (Vol Surge: {vol_surge:.2f}x >= {min_vol_surge}x, Close/Low: {close_to_low:.2f})"
        )

    return TriggerSignal(
        triggered=False,
        symbol=bar.symbol,
        direction="NONE",
        instrument_type="NONE",
        entry_price=bar.close,
        reason=f"No trigger conditions met (Vol: {vol_surge:.2f}x, Ext: {ext_pct:+.2f}%)"
    )

class FiveMinuteTriggerEngine:
    """
    Precision 5-Minute Trigger Scanner (Requirements W3, E3)
    Batches active watchlist symbols into a single KiteConnect quote call every 5 minutes.
    """
    def scan_active_watchlist(
        self,
        kite_client: Any,
        active_watchlist: List[str]
    ) -> List[TriggerSignal]:
        if not active_watchlist:
            return []

        # Single batch quote request (Requirement E3 - respects API rate limits)
        nfo_or_nse_keys = [f"NSE:{s}" for s in active_watchlist]
        try:
            quotes = kite_client.quote(nfo_or_nse_keys) or {}
        except Exception as e:
            print(f"Error fetching batch quotes for 5m watchlist: {e}")
            return []

        signals: List[TriggerSignal] = []

        for symbol in active_watchlist:
            key = f"NSE:{symbol}"
            data = quotes.get(key, {})
            if not data:
                continue

            last_px = float(data.get("last_price", 0.0))
            ohlc = data.get("ohlc", {})
            open_px = float(ohlc.get("open", last_px))
            high_px = float(ohlc.get("high", last_px))
            low_px = float(ohlc.get("low", last_px))
            close_px = last_px
            volume = int(data.get("volume", 0))

            # Approximate baseline metrics from live quote for immediate evaluation
            bar = FiveMinuteBar(
                symbol=symbol,
                timestamp=datetime.now(),
                open=open_px,
                high=high_px,
                low=low_px,
                close=close_px,
                volume=volume,
                vol_sma_20=max(1.0, float(volume) / 2.0), # Live proxy if historical not cached
                sma_20=open_px
            )
            sig = evaluate_5m_trigger(bar)
            if sig.triggered:
                signals.append(sig)

        return signals
