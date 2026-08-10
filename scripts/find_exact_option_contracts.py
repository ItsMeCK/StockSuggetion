"""
Finds the exact NFO Call Option contract symbols and EOD premium prices
for LODHA and HDFCBANK for tomorrow's trade execution.
"""
import os
import datetime
from dotenv import load_dotenv
from kiteconnect import KiteConnect

load_dotenv()

def main():
    api_key = os.getenv("KITE_API_KEY")
    access_token = os.getenv("KITE_ACCESS_TOKEN")
    
    if not api_key or not access_token:
        print("Kite credentials missing")
        return
        
    try:
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token.strip("'"))
        
        # 1. Fetch NFO instruments to locate the exact July Call Option contracts
        print("Scanning NFO instruments...")
        all_nfo = kite.instruments("NFO")
        
        # Current stock closes: LODHA = 1096.15, HDFCBANK = 829.65
        targets = {
            "LODHA": {"strike": 1100.0, "type": "CE"},
            "HDFCBANK": {"strike": 830.0, "type": "CE"}
        }
        
        found_instruments = []
        
        # Filter for July expiry option contracts
        for inst in all_nfo:
            name = inst["name"]
            if name in targets:
                target = targets[name]
                # Expiry must be July 2026 (usually 2026-07-28 or 2026-07-30 depending on contract)
                expiry = inst["expiry"]
                if expiry and expiry.year == 2026 and expiry.month == 7:
                    if inst["strike"] == target["strike"] and inst["instrument_type"] == target["type"]:
                        found_instruments.append(inst)
                        
        print("\n=========================================================================")
        print("EXACT CALL OPTION CONTRACTS FOR TUESDAY OPEN")
        print("=========================================================================")
        
        # Fetch EOD premiums for the found contracts
        if not found_instruments:
            print("No matching July options found in database.")
            return
            
        trading_symbols = [f"NFO:{inst['tradingsymbol']}" for inst in found_instruments]
        ltp_data = kite.ltp(trading_symbols)
        
        for inst in found_instruments:
            sym = inst["tradingsymbol"]
            token = f"NFO:{sym}"
            ltp = ltp_data.get(token, {}).get("last_price")
            
            # Stop loss calculation (orthodox 40% premium risk cushion)
            stop_loss = ltp * 0.60 if ltp else 0.0
            
            print(f"Stock:        {inst['name']}")
            print(f"  Contract:   {sym}")
            print(f"  Expiry:     {inst['expiry'].strftime('%Y-%m-%d')}")
            print(f"  Strike:     ₹{inst['strike']}")
            print(f"  Lot Size:   {inst['lot_size']}")
            print(f"  EOD Close:  ₹{ltp:.2f} (Suggested Limit Entry)")
            print(f"  Stop-Loss:  ₹{stop_loss:.2f} (GTT SL)")
            print("-" * 50)
            
        print("=========================================================================\n")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
