#!/usr/bin/env python3
import os
from dotenv import load_dotenv
from kiteconnect import KiteConnect

load_dotenv()

def audit_account(label, api_key, access_token):
    print(f"\n==========================================")
    print(f"📊 AUDITING ZERODHA ACCOUNT: {label}")
    print(f"==========================================")
    if not api_key or not access_token:
        print("Missing credentials.")
        return
    
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    
    try:
        prof = kite.profile()
        print(f"User: {prof.get('user_id')} - {prof.get('user_name')}")
    except Exception as e:
        print("Error fetching profile:", e)
        return
        
    try:
        margins = kite.margins(segment="equity")
        avail = margins.get("available", {})
        print(f"Margins -> Net: ₹{margins.get('net')}, Live Balance: ₹{avail.get('live_balance')}, Cash: ₹{avail.get('cash')}")
    except Exception as e:
        print("Error fetching margins:", e)
        
    # Holdings
    try:
        holdings = kite.holdings()
        tot_holdings_pnl = sum(float(h.get("pnl", 0.0) or 0.0) for h in holdings)
        tot_holdings_val = sum(float(h.get("last_price", 0.0) or 0.0) * int(h.get("quantity", 0) or 0) for h in holdings)
        print(f"\nHOLDINGS ({len(holdings)} stocks) -> Total Value: ₹{tot_holdings_val:.2f}, Total PnL: ₹{tot_holdings_pnl:.2f}")
        for h in holdings:
            sym = h.get("tradingsymbol")
            qty = h.get("quantity")
            avg = h.get("average_price")
            ltp = h.get("last_price")
            pnl = h.get("pnl")
            print(f"  {sym:<15} | Qty: {qty:<6} | Avg: ₹{avg:<8.2f} | LTP: ₹{ltp:<8.2f} | PnL: ₹{pnl:+.2f}")
    except Exception as e:
        print("Error fetching holdings:", e)

    # Positions
    try:
        positions = kite.positions()
        net = positions.get("net", [])
        tot_net_pnl = sum(float(p.get("pnl", 0.0) or 0.0) for p in net)
        print(f"\nPOSITIONS ({len(net)} contracts) -> Total Net PnL: ₹{tot_net_pnl:.2f}")
        for p in net:
            sym = p.get("tradingsymbol")
            qty = p.get("quantity")
            bval = p.get("buy_value")
            sval = p.get("sell_value")
            pnl = p.get("pnl")
            print(f"  {sym:<22} | Qty: {qty:<6} | BuyVal: ₹{bval:<10.2f} | SellVal: ₹{sval:<10.2f} | PnL: ₹{pnl:+.2f}")
    except Exception as e:
        print("Error fetching positions:", e)

def main():
    exec_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
    exec_tok = os.getenv("EXEC_KITE_ACCESS_TOKEN", "").strip("'\"")
    audit_account("EXEC ACCOUNT (HU0092)", exec_key, exec_tok)

    data_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    data_tok = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")
    audit_account("DATA ACCOUNT (YF4472)", data_key, data_tok)

if __name__ == "__main__":
    main()
