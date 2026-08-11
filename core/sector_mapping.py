# core/sector_mapping.py

SECTOR_MAP = {
    # Pharma
    "ZYDUSLIFE": "NIFTY PHARMA",
    "DRREDDY": "NIFTY PHARMA",
    "SUNPHARMA": "NIFTY PHARMA",
    "CIPLA": "NIFTY PHARMA",
    "LUPIN": "NIFTY PHARMA",
    "DIVISLAB": "NIFTY PHARMA",
    "AUROPHARMA": "NIFTY PHARMA",
    "BIOCON": "NIFTY PHARMA",
    
    # IT
    "TCS": "NIFTY IT",
    "INFY": "NIFTY IT",
    "HCLTECH": "NIFTY IT",
    "WIPRO": "NIFTY IT",
    "TECHM": "NIFTY IT",
    "LTIM": "NIFTY IT",
    "COFORGE": "NIFTY IT",
    "PERSISTENT": "NIFTY IT",
    
    # Bank
    "HDFCBANK": "NIFTY BANK",
    "ICICIBANK": "NIFTY BANK",
    "SBIN": "NIFTY BANK",
    "AXISBANK": "NIFTY BANK",
    "KOTAKBANK": "NIFTY BANK",
    "INDUSINDBK": "NIFTY BANK",
    "PNB": "NIFTY BANK",
    "BANKBARODA": "NIFTY BANK",
    
    # Auto
    "TATAMOTORS": "NIFTY AUTO",
    "M&M": "NIFTY AUTO",
    "MARUTI": "NIFTY AUTO",
    "BAJAJ-AUTO": "NIFTY AUTO",
    "HEROMOTOCO": "NIFTY AUTO",
    "EICHERMOT": "NIFTY AUTO",
    "TVSMOTOR": "NIFTY AUTO",
    "ASHOKLEY": "NIFTY AUTO",
    
    # FMCG
    "ITC": "NIFTY FMCG",
    "HINDUNILVR": "NIFTY FMCG",
    "NESTLEIND": "NIFTY FMCG",
    "BRITANNIA": "NIFTY FMCG",
    "TATACONSUM": "NIFTY FMCG",
    "DABUR": "NIFTY FMCG",
    "GODREJCP": "NIFTY FMCG",
    "MARICO": "NIFTY FMCG",
    "PATANJALI": "NIFTY FMCG",
    
    # Metal
    "TATASTEEL": "NIFTY METAL",
    "HINDALCO": "NIFTY METAL",
    "JSWSTEEL": "NIFTY METAL",
    "JINDALSTEL": "NIFTY METAL",
    "VEDL": "NIFTY METAL",
    "NATIONALUM": "NIFTY METAL",
    "SAIL": "NIFTY METAL",
    "NMDC": "NIFTY METAL",
    
    # Realty
    "DLF": "NIFTY REALTY",
    "GODREJPROP": "NIFTY REALTY",
    "LODHA": "NIFTY REALTY",
    "OBEROIRLTY": "NIFTY REALTY",
    "PRESTIGE": "NIFTY REALTY",
    "PHOENIXLTD": "NIFTY REALTY",
    
    # Energy / Oil & Gas
    "RELIANCE": "NIFTY ENERGY",
    "ONGC": "NIFTY ENERGY",
    "NTPC": "NIFTY ENERGY",
    "POWERGRID": "NIFTY ENERGY",
    "COALINDIA": "NIFTY ENERGY",
    "BPCL": "NIFTY ENERGY",
    "IOC": "NIFTY ENERGY",
    "TATAPOWER": "NIFTY ENERGY",
}

def get_sector_for_symbol(symbol: str) -> str:
    """Returns the NSE Sector Index name for a given FNO symbol. Defaults to NIFTY 50."""
    return SECTOR_MAP.get(symbol, "NIFTY 50")
