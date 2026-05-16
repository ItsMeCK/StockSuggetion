import os
import sys
import logging

# Ensure we can import from the current directory
sys.path.append(os.getcwd())

from pipeline.ingestion import ZerodhaIngestionEngine
from datetime import datetime

# List of symbols to repair
symbols = ["ADANIENT","AIROLAM","AUROPHARMA","BAIDFIN","BHARTIARTL","CHALET","CHAMBLFERT","CIGNITITEC","CNL","CROMPTON","EASEMYTRIP","ESTER","GLAXO","GMMPFAUDLR","GOLDADD","GOLDBETA","GOLDTECH","GROWWGOLD","IDEA","IIFL","INCREDIBLE","ITC","LICMFGOLD","MANUGRAPH","MEDICO","MIDCAP","NILASPACES","NKIND","NORTHARC","NUVOCO","OBCL","PFIZER","RALLIS","RAYMONDLSL","ROML","SBILIFE","SKFINDIA","SOLARINDS","STARTECK","TMPV","V2RETAIL","VENTIVE","ZYDUSLIFE"]

def repair():
    engine = ZerodhaIngestionEngine()
    try:
        engine.connect_db()
        
        # Get tokens for these symbols
        print("Fetching NSE instruments...")
        all_instruments = engine.kite.instruments("NSE")
        lookup = {inst['tradingsymbol']: inst['instrument_token'] for inst in all_instruments}
        
        from_date = "2026-05-10"
        to_date = "2026-05-18"
        
        for symbol in symbols:
            token = lookup.get(symbol)
            if token:
                data = engine.fetch_historical_data(token, from_date, to_date)
                if data:
                    engine.bulk_insert_ohlcv(symbol, data)
                    print(f"✅ Fixed {symbol}")
                else:
                    print(f"❌ No data for {symbol}")
            else:
                print(f"⚠️ Token not found for {symbol}")
                
            # Respect rate limit
            import time
            time.sleep(0.35)
            
    except Exception as e:
        print(f"🔥 Repair failed: {e}")
    finally:
        engine.close_db()

if __name__ == "__main__":
    repair()
