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

    # --- Conviction Router integration ---
    # When the Conviction Router has tagged routes, respect them:
    #  - dropped / NO_EDGE entries stay dropped
    #  - EQUITY_CONTINUATION stays equity (no NFO lookup)
    #  - OPTIONS_IGNITION requires an NFO contract; if unavailable the trade is
    #    DROPPED, not downgraded (ignition setups on a 2-day equity hold showed
    #    a 45% win rate in forensics - worse than not trading).
    router_tagged = any("route" in a for a in approved.values())
    if router_tagged:
        needs_nfo = {t: a for t, a in approved.items()
                     if a.get("route") == "OPTIONS_IGNITION" and not a.get("dropped")}
        needs_nfo_pe = {t: a for t, a in approved.items()
                        if a.get("route") == "PE_BEARISH_DIVERGENCE" and not a.get("dropped")}
        if not needs_nfo and not needs_nfo_pe:
            return {"approved_allocations": approved}
        instruments = None
        if api_key and access_token:
            try:
                kite = KiteConnect(api_key=api_key)
                kite.set_access_token(access_token)
                instruments = kite.instruments(exchange="NFO")
            except Exception as e:
                logging.error(f"NFO instrument fetch failed: {e}")

        def _resolve(ticker, details, opt_type):
            if instruments is None:
                logging.warning(f"Derivatives Router: no NFO data; DROPPING setup {ticker} (no equity fallback).")
                approved[ticker]["dropped"] = True
                approved[ticker]["suggested_instrument"] = "NONE"
                return
            fno = [i for i in instruments
                   if i['name'] == ticker and i['instrument_type'] == opt_type and i['expiry'] is not None]
            if not fno:
                logging.info(f"Derivatives Router: {ticker} setup has no {opt_type} NFO options -> DROPPED.")
                approved[ticker]["dropped"] = True
                approved[ticker]["suggested_instrument"] = "NONE"
                return
            nearest = sorted(set(i['expiry'] for i in fno))[0]
            opts = [i for i in fno if i['expiry'] == nearest]
            entry_price = details.get("entry", 0.0)
            opts.sort(key=lambda x: abs(float(x['strike']) - entry_price))
            sel = opts[0]
            approved[ticker].update({
                "suggested_instrument": "NFO_OPTION",
                "lot_size": sel['lot_size'],
                "nearest_expiry": nearest.strftime("%Y-%m-%d"),
                "option_symbol": sel['tradingsymbol'],
                "strike": float(sel['strike']),
            })
            logging.info(f"Derivatives Router: {ticker} -> {sel['tradingsymbol']} (lot {sel['lot_size']}, expiry {nearest})")

        for ticker, details in needs_nfo.items():
            _resolve(ticker, details, 'CE')
        for ticker, details in needs_nfo_pe.items():
            _resolve(ticker, details, 'PE')
        return {"approved_allocations": approved}

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
