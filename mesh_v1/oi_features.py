"""
OI Buildup feature computation (V9). For a given F&O symbol and historical
date, finds the strike that was closest-to-ATM on that date among CURRENTLY
LISTED contracts (Kite's instruments() dump only has non-expired contracts -
already-expired May/June series are gone, so this only works for contracts
still listed today with OI history reaching back far enough), then classifies
the Price x OI Buildup type for that option on that date vs the prior
trading day:
  Long Buildup:   option price up,  OI up   (fresh bullish conviction)
  Short Buildup:  option price down, OI up  (fresh bearish/writer conviction)
  Long Unwinding: option price down, OI down
  Short Covering: option price up,  OI down
"""
import os
import time
from datetime import date, timedelta
from dotenv import load_dotenv
from kiteconnect import KiteConnect

load_dotenv()
_kite = None


def kite():
    global _kite
    if _kite is None:
        _kite = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
        _kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN").strip("'"))
    return _kite


_instrument_cache = {}
_all_nfo_cache = None


def _get_all_nfo():
    global _all_nfo_cache
    if _all_nfo_cache is None:
        _all_nfo_cache = kite().instruments("NFO")
    return _all_nfo_cache


def _get_option_chain(underlying_symbol):
    if underlying_symbol in _instrument_cache:
        return _instrument_cache[underlying_symbol]
    all_nfo = _get_all_nfo()
    chain = [i for i in all_nfo if i["name"] == underlying_symbol and i["instrument_type"] in ("CE", "PE")]
    _instrument_cache[underlying_symbol] = chain
    return chain


def find_nearest_expiry_chain(underlying_symbol, as_of_date):
    """Picks the nearest-expiry contract series that was still listed as-of
    as_of_date (i.e. expiry >= as_of_date), since that's the closest proxy to
    what would have been the front-month series trading back then. Returns
    the list of instruments for that expiry."""
    chain = _get_option_chain(underlying_symbol)
    if not chain:
        return None, []
    expiries = sorted(set(i["expiry"] for i in chain if i["expiry"] >= as_of_date))
    if not expiries:
        return None, []
    nearest = expiries[0]
    return nearest, [i for i in chain if i["expiry"] == nearest]


def find_atm_strike_instrument(underlying_symbol, spot_price, as_of_date, opt_type="CE"):
    expiry, contracts = find_nearest_expiry_chain(underlying_symbol, as_of_date)
    if not contracts:
        return None
    same_type = [c for c in contracts if c["instrument_type"] == opt_type]
    if not same_type:
        return None
    atm = min(same_type, key=lambda c: abs(c["strike"] - spot_price))
    return atm


def get_oi_history(instrument_token, from_date, to_date, retries=3):
    for attempt in range(retries):
        try:
            hist = kite().historical_data(
                instrument_token,
                from_date.strftime("%Y-%m-%d 09:00:00"),
                to_date.strftime("%Y-%m-%d 15:30:00"),
                "day", oi=True,
            )
            return hist
        except Exception as e:
            if attempt == retries - 1:
                return []
            time.sleep(1.0)
    return []


def classify_buildup(price_chg, oi_chg):
    if price_chg > 0 and oi_chg > 0:
        return "LONG_BUILDUP"
    if price_chg < 0 and oi_chg > 0:
        return "SHORT_BUILDUP"
    if price_chg < 0 and oi_chg < 0:
        return "LONG_UNWINDING"
    if price_chg > 0 and oi_chg < 0:
        return "SHORT_COVERING"
    return "FLAT"


def buildup_signal_for(underlying_symbol, spot_price_asof, as_of_date, opt_type="CE"):
    """Returns the Buildup classification on the LAST available trading day
    strictly before as_of_date, for the strike that was ATM as of that prior
    day's spot (spot_price_asof should be the prior day's close). None if
    contract/data unavailable."""
    inst = find_atm_strike_instrument(underlying_symbol, spot_price_asof, as_of_date, opt_type)
    if not inst:
        return None
    hist = get_oi_history(inst["instrument_token"], as_of_date - timedelta(days=10), as_of_date)
    hist = [h for h in hist if h["date"].date() < as_of_date]
    if len(hist) < 2:
        return None
    prev, last = hist[-2], hist[-1]
    if not prev["oi"] or prev["close"] == 0:
        return None
    price_chg = (last["close"] - prev["close"]) / prev["close"] * 100
    oi_chg = (last["oi"] - prev["oi"]) / prev["oi"] * 100 if prev["oi"] else 0
    return {
        "strike": inst["strike"], "buildup": classify_buildup(price_chg, oi_chg),
        "price_chg_pct": round(price_chg, 2), "oi_chg_pct": round(oi_chg, 2),
        "signal_date": last["date"].date().isoformat(),
    }
