import os
import logging
from dotenv import load_dotenv
from kiteconnect import KiteConnect
import math
from datetime import date, datetime
from scipy.stats import norm
from core.state import SovereignState

load_dotenv()

def calculate_bs_call_delta(spot: float, strike: float, dte_days: float, r: float = 0.07, iv: float = 0.30) -> float:
    """Calculates Black-Scholes call option delta."""
    if dte_days <= 0 or spot <= 0 or strike <= 0 or iv <= 0:
        return 0.50 if spot >= strike else 0.0
    t = dte_days / 365.0
    d1 = (math.log(spot / strike) + (r + 0.5 * iv ** 2) * t) / (iv * math.sqrt(t))
    return float(norm.cdf(d1))

def calculate_bs_put_delta(spot: float, strike: float, dte_days: float, r: float = 0.07, iv: float = 0.30) -> float:
    """Calculates Black-Scholes put option delta (returns negative delta, e.g. -0.65 for ITM put)."""
    if dte_days <= 0 or spot <= 0 or strike <= 0 or iv <= 0:
        return -0.50 if spot <= strike else 0.0
    t = dte_days / 365.0
    d1 = (math.log(spot / strike) + (r + 0.5 * iv ** 2) * t) / (iv * math.sqrt(t))
    return float(norm.cdf(d1) - 1.0)

def select_optimal_strike(fno_insts: list, entry_price: float, today_dt: date = None, direction: str = "LONG", instrument_type: str = "CE"):
    """
    High-Delta Strike Selection Algorithm (v3.3.0-PROD) - Bi-Directional
    1. Filter expiries: require DTE >= 4 calendar days (roll to next monthly if DTE < 4).
    2. Compute Black-Scholes delta for each contract matching instrument_type (CE or PE).
    3. Target ITM Option:
       - CE (Long): Delta in [0.58, 0.75] (ideal target Delta* = 0.65, Strike < Spot).
       - PE (Short): |Delta| in [0.58, 0.75] (ideal target Delta* = -0.65, Strike > Spot).
    4. Fallback hierarchy: ITM -> ATM (|Delta| 0.50-0.58) -> None (fallback to Cash EQUITY).
    5. ABSOLUTE INVARIANT: Never buy OTM (|Delta| < 0.50).
    """
    if not today_dt:
        today_dt = date.today()

    target_inst_type = "PE" if (direction == "SHORT" or instrument_type == "PE") else "CE"

    expiries = sorted(list(set([
        i['expiry'] for i in fno_insts 
        if i.get('expiry') is not None and i.get('instrument_type') == target_inst_type
    ])))
    
    valid_expiries = [exp for exp in expiries if (exp - today_dt).days >= 4]
    if not valid_expiries:
        # Invariant: Never trade options with DTE < 4 days (extreme theta decay trap).
        # Fall back to Cash Equity route.
        return None, None, "EQUITY"

    target_expiry = valid_expiries[0]
    dte_days = max(1, (target_expiry - today_dt).days)
    target_expiry_str = target_expiry.strftime("%Y-%m-%d")

    matching_contracts = [
        i for i in fno_insts 
        if i.get('expiry') == target_expiry and i.get('instrument_type') == target_inst_type
    ]
    if not matching_contracts:
        return None, None, "EQUITY"

    contracts_with_delta = []
    for c in matching_contracts:
        try:
            strike = float(c['strike'])
            if target_inst_type == "CE":
                delta = calculate_bs_call_delta(entry_price, strike, dte_days)
            else:
                delta = calculate_bs_put_delta(entry_price, strike, dte_days)
            contracts_with_delta.append((c, delta))
        except (ValueError, TypeError):
            continue

    # Tier 1: Target In-The-Money (ITM) Delta centered around magnitude 0.65 (acceptable band 0.58 to 0.75)
    itm_candidates = [
        (c, delta) for c, delta in contracts_with_delta 
        if 0.58 <= abs(delta) <= 0.75
    ]
    if itm_candidates:
        best_c, best_delta = min(itm_candidates, key=lambda x: abs(abs(x[1]) - 0.65))
        return best_c, target_expiry_str, "NFO_OPTION"

    # Tier 2: Fallback to ATM (|Delta| 0.50 - 0.58)
    atm_candidates = [
        (c, delta) for c, delta in contracts_with_delta 
        if 0.50 <= abs(delta) < 0.58
    ]
    if atm_candidates:
        best_c, best_delta = min(atm_candidates, key=lambda x: abs(abs(x[1]) - 0.55))
        return best_c, target_expiry_str, "NFO_OPTION"

    # Tier 3: Absolute Invariant: Never buy OTM (|Delta| < 0.50). Fallback to Cash Equity.
    return None, target_expiry_str, "EQUITY"

def run_derivatives_routing_agent(state: SovereignState) -> dict:
    """
    LangGraph Node: Derivatives Routing Agent
    Runs after Risk & Position Sizing.
    Evaluates all approved allocations to determine if they can be traded via NFO Options.
    Mutates the approved_allocations to specify 'instrument_type' ('NFO_OPTION' or 'EQUITY').
    """
    logging.info("--- PHASE 3.5: DERIVATIVES ROUTING AGENT ---")
    
    approved = state.get("approved_allocations", {})
    if not approved:
        logging.info("No approved allocations to route. Skipping.")
        return {}

    api_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    access_token = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")
    
    if not api_key or not access_token:
        logging.warning("Zerodha API credentials missing. Defaulting all routes to EQUITY.")
        for ticker in approved:
            approved[ticker]["suggested_instrument"] = "EQUITY"
        return {"approved_allocations": approved}

    try:
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        logging.info(f"Fetching NFO instruments for {len(approved)} allocations...")
        instruments = kite.instruments(exchange="NFO")
    except Exception as e:
        logging.error(f"Failed to fetch NFO instruments: {e}")
        for ticker in approved:
            approved[ticker]["suggested_instrument"] = "EQUITY"
        return {"approved_allocations": approved}

    updated_allocations = {}
    
    for ticker, details in approved.items():
        updated_allocations[ticker] = details.copy()
        entry_price = float(details.get("entry", 0.0))
        
        # Check if underlying ticker exists in NFO segment
        fno_insts = [i for i in instruments if i.get('name') == ticker]
        
        if not fno_insts:
            logging.info(f"Derivatives Router: {ticker} -> Routed to EQUITY (No NFO Options Available)")
            updated_allocations[ticker]["suggested_instrument"] = "EQUITY"
            continue
            
        selected_ce, expiry_str, route_type = select_optimal_strike(fno_insts, entry_price)
        
        if route_type == "NFO_OPTION" and selected_ce:
            option_symbol = selected_ce['tradingsymbol']
            strike = float(selected_ce['strike'])
            lot_size = selected_ce['lot_size']
            
            logging.info(f"Derivatives Router: {ticker} -> Routed to NFO_OPTION (Symbol: {option_symbol}, Strike: {strike}, Lot Size: {lot_size}, Expiry: {expiry_str})")
            updated_allocations[ticker]["suggested_instrument"] = "NFO_OPTION"
            updated_allocations[ticker]["lot_size"] = lot_size
            updated_allocations[ticker]["nearest_expiry"] = expiry_str
            updated_allocations[ticker]["option_symbol"] = option_symbol
            updated_allocations[ticker]["strike"] = strike
        else:
            logging.info(f"Derivatives Router: {ticker} -> Routed to EQUITY (No ITM/ATM contract passing Delta & DTE filters)")
            updated_allocations[ticker]["suggested_instrument"] = "EQUITY"

    return {"approved_allocations": updated_allocations}
