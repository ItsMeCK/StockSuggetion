import os
from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv()
kite = KiteConnect(api_key=os.getenv("KITE_API_KEY", "").strip("'\""))
kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN", "").strip("'\""))

instruments = kite.instruments("NFO")
fno_symbols = sorted(list(set([inst['name'] for inst in instruments if inst['instrument_type'] in ['CE', 'PE']])))
print(f"Total FNO Stocks: {len(fno_symbols)}")
print(fno_symbols[:10])
