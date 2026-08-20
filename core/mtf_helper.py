import logging
from datetime import datetime, timedelta
import pytz

def fetch_15m_structure(kite, symbol, target_utc):
    """
    Fetches the 15-minute structural data for the given symbol, strictly covering
    the hour leading up to `target_utc`.
    
    Returns a dictionary of the final 15m candle metrics:
    {
        "final_15m_vol": int,
        "hourly_vol": int,
        "final_15m_close": float,
        "final_15m_open": float,
        "final_15m_high": float,
        "final_15m_low": float
    }
    """
    try:
        # Get instrument token
        instruments = kite.instruments("NSE")
        token = next((i['instrument_token'] for i in instruments if i['tradingsymbol'] == symbol), None)
        
        if not token:
            logging.error(f"Could not find token for {symbol}")
            return None
            
        # Target UTC is the start of the completed 60m candle. We want the 15m candles from start_ist to start_ist + 1 hour.
        # Convert UTC back to IST for Kite API
        ist_tz = pytz.timezone('Asia/Kolkata')
        start_ist = target_utc.astimezone(ist_tz)
        target_ist = start_ist + timedelta(hours=1)
        
        # Kite interval "15minute"
        hist = kite.historical_data(
            instrument_token=token,
            from_date=start_ist.strftime("%Y-%m-%d %H:%M:%S"),
            to_date=target_ist.strftime("%Y-%m-%d %H:%M:%S"),
            interval="15minute"
        )
        
        if not hist or len(hist) < 4:
            logging.warning(f"Insufficient 15m data for {symbol}. (Got {len(hist)} candles)")
            return None
            
        # The last candle in this 1-hour block is the final 15m candle
        final_candle = hist[-1]
        # Analyze the internal structure of the 4 candles
        highs = [c['high'] for c in hist]
        volumes = [c['volume'] for c in hist]
        
        # Count higher highs (consecutive increases in high price)
        higher_high_count = 0
        for i in range(1, len(highs)):
            if highs[i] > highs[i-1]:
                higher_high_count += 1
                
        has_consecutive_higher_highs = higher_high_count >= 2
        is_vol_climax = volumes[-1] == max(volumes) if len(volumes) > 0 else False
        hourly_vol = sum(volumes)
        
        return {
            "final_15m_vol": final_candle['volume'],
            "hourly_vol": hourly_vol,
            "final_15m_close": final_candle['close'],
            "final_15m_open": final_candle['open'],
            "final_15m_high": final_candle['high'],
            "final_15m_low": final_candle['low'],
            "has_consecutive_higher_highs": has_consecutive_higher_highs,
            "is_vol_climax": is_vol_climax
        }
        
    except Exception as e:
        logging.error(f"Error fetching 15m data for {symbol}: {e}")
        return None
