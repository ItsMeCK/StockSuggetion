import os
import logging
from dotenv import load_dotenv
from kiteconnect import KiteConnect
from core.state import SovereignState

load_dotenv()

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
        
        # Check if the underlying ticker exists in the NFO segment
        fno_insts = [i for i in instruments if i['name'] == ticker]
        
        if not fno_insts:
            logging.info(f"Derivatives Router: {ticker} -> Routed to EQUITY (No NFO Options Available)")
            updated_allocations[ticker]["suggested_instrument"] = "EQUITY"
            continue
            
        # Find unique expiries
        expiries = sorted(list(set([i['expiry'] for i in fno_insts if i['expiry'] is not None and i['instrument_type'] in ['CE', 'PE']])))
        
        if expiries:
            nearest_expiry_date = expiries[0]
            nearest_expiry_str = nearest_expiry_date.strftime("%Y-%m-%d")
            
            # Find the CE option with the strike closest to the entry price
            ce_options = [i for i in fno_insts if i['expiry'] == nearest_expiry_date and i['instrument_type'] == 'CE']
            
            if ce_options:
                entry_price = details.get("entry", 0.0)
                # Sort CE options by proximity of strike to entry price
                ce_options.sort(key=lambda x: abs(float(x['strike']) - entry_price))
                selected_ce = ce_options[0]
                
                option_symbol = selected_ce['tradingsymbol']
                strike = float(selected_ce['strike'])
                lot_size = selected_ce['lot_size']
                
                logging.info(f"Derivatives Router: {ticker} -> Routed to NFO_OPTION (Symbol: {option_symbol}, Strike: {strike}, Lot Size: {lot_size}, Expiry: {nearest_expiry_str})")
                updated_allocations[ticker]["suggested_instrument"] = "NFO_OPTION"
                updated_allocations[ticker]["lot_size"] = lot_size
                updated_allocations[ticker]["nearest_expiry"] = nearest_expiry_str
                updated_allocations[ticker]["option_symbol"] = option_symbol
                updated_allocations[ticker]["strike"] = strike
            else:
                logging.info(f"Derivatives Router: {ticker} -> Routed to EQUITY (No CE contracts found for nearest expiry)")
                updated_allocations[ticker]["suggested_instrument"] = "EQUITY"
        else:
            logging.info(f"Derivatives Router: {ticker} -> Routed to EQUITY (No CE/PE found in NFO)")
            updated_allocations[ticker]["suggested_instrument"] = "EQUITY"

    return {"approved_allocations": updated_allocations}
