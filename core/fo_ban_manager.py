import os
import csv
from datetime import date
from typing import Set

def get_banned_fo_symbols(today_dt: date = None) -> Set[str]:
    """
    NSE F&O Ban Period Manager (Claude Requirement D5)
    When a security crosses 95% of its Market-Wide Position Limit (MWPL),
    NSE places it in the ban period. Entering new derivative contracts is
    prohibited and carries severe exchange penalties and broker order rejections.
    """
    banned_symbols: Set[str] = set()
    ban_file = "data/fo_secban.csv"

    if os.path.exists(ban_file):
        try:
            with open(ban_file, "r") as f:
                reader = csv.reader(f)
                for row in reader:
                    if row and row[0].strip():
                        sym = row[0].strip().upper()
                        # Skip header if present
                        if sym not in ("SYMBOL", "SECURITY", "NAME"):
                            banned_symbols.add(sym)
        except Exception as e:
            print(f"Warning: Could not read F&O ban file {ban_file}: {e}")

    return banned_symbols

def is_symbol_banned(symbol: str, banned_set: Set[str] = None) -> bool:
    """Checks if a given symbol is currently in the NSE F&O ban list."""
    if banned_set is None:
        banned_set = get_banned_fo_symbols()
    return symbol.upper().strip() in banned_set
