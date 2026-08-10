from dotenv import load_dotenv
from core.live_trading import get_kite_exec_client

load_dotenv()
try:
    kite_exec = get_kite_exec_client()
    
    orders = kite_exec.orders()
    open_orders = [o for o in orders if o['status'] in ['OPEN', 'TRIGGER PENDING']]
    
    if open_orders:
        print('⚠️ OPEN ORDERS FOUND:')
        for o in open_orders:
            print(f"- {o['tradingsymbol']}: {o['transaction_type']} {o['quantity']} at {o.get('price', o.get('trigger_price', 'Market'))} ({o['status']})")
    else:
        print('✅ NO OPEN ORDERS.')
        
    positions = kite_exec.positions().get('net', [])
    open_positions = [p for p in positions if p['quantity'] != 0]
    
    if open_positions:
        print('\n⚠️ OPEN POSITIONS FOUND:')
        for p in open_positions:
            print(f"- {p['tradingsymbol']}: {p['quantity']} qty")
    else:
        print('\n✅ NO OPEN POSITIONS.')
        
except Exception as e:
    print(f'Error connecting to Zerodha: {e}')
