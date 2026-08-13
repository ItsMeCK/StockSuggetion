import os
from datetime import datetime
from dotenv import load_dotenv
from core.db_manager import save_active_position

load_dotenv()

position = {
    "order_id": "MANUAL_OBEROI",
    "sl_order_id": "FAILED",
    "status": "LIVE_TRADED",
    "entry_time": datetime.now().isoformat(),
    "symbol": "OBEROIRLTY",
    "option_symbol": "OBEROIRLTY26AUG1840CE",
    "option_token": 0,
    "spot_entry": 1822.4,
    "entry_premium": 28.1,
    "qty": 350,
    "score": 95.0,
    "catalyst": "Manual entry injection for monitoring.",
    "current_sl": 14.05,
    "highest_high": 28.1,
    "trailing_active": False,
    "initial_oi": 384300,
    "highest_oi": 384300,
    "initial_volume": 307650
}

save_active_position(position)
print("✅ Successfully injected OBEROIRLTY into active_positions!")
