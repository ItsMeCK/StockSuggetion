import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import argparse
from datetime import datetime, time
import pytz
from dotenv import load_dotenv

from core.db_manager import get_all_active_positions, close_position
from core.live_trading import get_kite_data_client, get_kite_exec_client
from alerts.email_notifier import SovereignEmailer

def round_to_tick(price: float, tick: float = 0.05) -> float:
    """Rounds price to nearest valid NSE tick size (default 0.05)."""
    return round(round(price / tick) * tick, 2)

def is_btst_position(pos: dict) -> bool:
    """
    Identifies whether a position belongs to Engine 2 (Type C BTST / Anticipatory).
    Checks explicit holding_type tag or entry timestamp in 15:00-15:30 window.
    """
    holding_type = pos.get("holding_type")
    if holding_type == "TYPE_C_BTST":
        return True
    
    # Fallback to entry timestamp / catalyst heuristics if holding_type not stamped
    entry_time = pos.get("entry_time")
    if entry_time:
        if isinstance(entry_time, str):
            try:
                entry_dt = datetime.fromisoformat(entry_time)
            except Exception:
                entry_dt = None
        else:
            entry_dt = entry_time
            
        if entry_dt and entry_dt.hour == 15 and entry_dt.minute >= 10:
            return True
            
    catalyst = str(pos.get("catalyst", "")).lower()
    if "insider" in catalyst or "anticipatory" in catalyst or "btst" in catalyst:
        return True
        
    return False

def run_btst_liquidator(now: datetime = None, force_exit: bool = False, dry_run: bool = False) -> list:
    """
    Evaluates active BTST positions during the 09:15 - 09:30 AM market open window on T+1.
    Empirical Basis:
    - Claude 300-day F&O study confirmed overnight gap edge (BH-adjusted p = 0.0121, +0.188%).
    - Edge completely decays by next-day close (p = 0.9668, mean ≈ -0.0024%).
    - Mandatory exit at market open prevents catastrophic multi-day bleed into expiry.
    """
    load_dotenv()
    ist = pytz.timezone("Asia/Kolkata")
    if now is None:
        now = datetime.now(ist)
    elif now.tzinfo is None:
        now = ist.localize(now)

    print(f"\n=======================================================")
    print(f"🌅 RUNNING BTST T+1 MORNING LIQUIDATOR AT: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"=======================================================")

    live_buy_flag = os.getenv("LIVE_BUY", "False").lower() == "true" and not dry_run
    
    # Check execution window: 09:15 to 09:45 IST on weekdays
    current_time = now.time()
    in_morning_window = time(9, 15) <= current_time <= time(9, 45)
    
    if not in_morning_window and not force_exit:
        print(f"ℹ️ Current time {current_time} is outside the 09:15 - 09:45 AM window. Skipping liquidator run.")
        return []

    try:
        kite_data = get_kite_data_client()
        kite_exec = get_kite_exec_client()
        emailer = SovereignEmailer()
    except Exception as e:
        print(f"❌ Error initializing Kite/Emailer clients: {e}")
        return []

    active_positions = get_all_active_positions()
    if not active_positions:
        print("ℹ️ No active positions found in database.")
        return []

    btst_positions = [p for p in active_positions if is_btst_position(p)]
    if not btst_positions:
        print(f"ℹ️ Found {len(active_positions)} active position(s), but none tagged as BTST/Engine 2.")
        return []

    print(f"🎯 Found {len(btst_positions)} active BTST position(s) to evaluate.")

    # Fetch live exchange positions for broker reconciliation
    net_positions = {}
    try:
        exchange_positions = kite_exec.positions()
        net_positions = {p['tradingsymbol']: p['quantity'] for p in exchange_positions.get('net', [])}
    except Exception as e:
        print(f"⚠️ Warning: Could not fetch broker net positions: {e}")

    exited_trades = []

    for pos in btst_positions:
        opt_symbol = pos["option_symbol"]
        order_id = pos["order_id"]
        sl_order_id = pos.get("sl_order_id")
        entry_premium = pos["entry_premium"]
        qty = pos["qty"]

        # 1. Broker Reconciliation: Check if already exited manually
        if net_positions and net_positions.get(opt_symbol, 0) <= 0:
            print(f"👁️ Broker Reconciliation: {opt_symbol} already exited on exchange (qty={net_positions.get(opt_symbol, 0)}).")
            if sl_order_id and sl_order_id != "FAILED":
                try:
                    kite_exec.cancel_order(variety=kite_exec.VARIETY_REGULAR, order_id=sl_order_id)
                except Exception:
                    pass
            close_position(pos, entry_premium, 0.0, "CLOSED_MANUAL_BROKER_EXIT")
            continue

        # 2. Fetch live Option Quote
        try:
            quote_resp = kite_data.quote([f"NFO:{opt_symbol}"])
            if f"NFO:{opt_symbol}" not in quote_resp:
                print(f"⚠️ Could not fetch quote for NFO:{opt_symbol}")
                continue
            ltp = quote_resp[f"NFO:{opt_symbol}"]["last_price"]
        except Exception as e:
            print(f"❌ Error fetching quote for {opt_symbol}: {e}")
            continue

        pnl_pct = ((ltp - entry_premium) / entry_premium) * 100
        print(f"📊 {opt_symbol}: Entry={entry_premium:.2f} | LTP={ltp:.2f} | PnL={pnl_pct:+.2f}%")

        # 3. Evaluate Exit Decision Rules
        should_exit = False
        exit_reason = ""

        # Rule A (Provisional Heuristic): Gap Profit Lock (+15% or higher)
        if pnl_pct >= 15.0:
            should_exit = True
            exit_reason = f"BTST Gap Profit Lock (+{pnl_pct:.1f}%)"

        # Rule B (Provisional Heuristic): Adverse Gap Cut (-10% or worse)
        elif pnl_pct <= -10.0:
            should_exit = True
            exit_reason = f"BTST Adverse Gap Stop ({pnl_pct:.1f}%)"

        # Rule C: Unconditional 09:30 AM Decay Exit (empirical edge flat/decayed)
        elif (now.hour == 9 and now.minute >= 30) or now.hour > 9 or force_exit:
            should_exit = True
            exit_reason = f"BTST 09:30 AM Edge Decay Cut ({pnl_pct:.1f}%)"

        if not should_exit:
            print(f"⏳ Holding {opt_symbol}: Inside 09:15-09:30 window, PnL {pnl_pct:+.2f}% awaiting target or 09:30 cutoff.")
            continue

        print(f"🚨 EXECUTING BTST EXIT: {opt_symbol} -> Reason: {exit_reason}")

        # 4. Cancel resting SL order on exchange
        if sl_order_id and sl_order_id != "FAILED" and live_buy_flag:
            try:
                kite_exec.cancel_order(variety=kite_exec.VARIETY_REGULAR, order_id=sl_order_id)
                print(f"✅ Cancelled resting SL order: {sl_order_id}")
            except Exception as ce:
                print(f"⚠️ Failed to cancel resting SL order {sl_order_id}: {ce}")

        # 5. Place Marketable Limit Exit Order (Sweeps bids without market order rejection)
        # NSE blocks ORDER_TYPE_MARKET for stock options; aggressive limit sell guarantees fill.
        limit_sell_price = round_to_tick(max(0.05, ltp * 0.90), 0.05)
        exit_order_id = f"PAPER_EXIT_{int(now.timestamp())}"

        if live_buy_flag:
            try:
                exit_order_id = kite_exec.place_order(
                    variety=kite_exec.VARIETY_REGULAR,
                    exchange=kite_exec.EXCHANGE_NFO,
                    tradingsymbol=opt_symbol,
                    transaction_type=kite_exec.TRANSACTION_TYPE_SELL,
                    quantity=qty,
                    product=kite_exec.PRODUCT_NRML,
                    order_type=kite_exec.ORDER_TYPE_LIMIT,
                    price=limit_sell_price
                )
                print(f"✅ Live Exit Order Placed! ID: {exit_order_id} @ Limit {limit_sell_price:.2f}")
            except Exception as oe:
                print(f"❌ Failed to place Live Exit Order for {opt_symbol}: {oe}")
                emailer.send_live_alert(
                    f"⚠️ URGENT: BTST Exit Order Failed for {opt_symbol}",
                    f"<p>Liquidator failed to place exit order: {oe}</p><p>Manual intervention required immediately!</p>"
                )
                continue
        else:
            print(f"📝 Paper Simulation: Closed {opt_symbol} @ {ltp:.2f} (Limit: {limit_sell_price:.2f})")

        # 6. Update Database State
        close_position(pos, ltp, pnl_pct, f"CLOSED_{exit_reason[:20]}")
        print(f"💾 Updated position in DB: {order_id} marked as CLOSED")

        exited_trade_info = {
            "symbol": pos["symbol"],
            "option_symbol": opt_symbol,
            "entry_premium": entry_premium,
            "exit_price": ltp,
            "pnl_pct": pnl_pct,
            "exit_reason": exit_reason
        }
        exited_trades.append(exited_trade_info)

        # 7. Dispatch Notification Email
        try:
            emailer.send_live_alert(
                f"🌅 BTST Position Liquidated: {pos['symbol']} ({pnl_pct:+.1f}%)",
                f"<h3>BTST T+1 Morning Liquidation Summary</h3>"
                f"<ul>"
                f"<li><b>Stock:</b> {pos['symbol']}</li>"
                f"<li><b>Contract:</b> {opt_symbol}</li>"
                f"<li><b>Entry Premium:</b> ₹{entry_premium:.2f}</li>"
                f"<li><b>Exit Price:</b> ₹{ltp:.2f}</li>"
                f"<li><b>PnL:</b> <span style='color: {'#238636' if pnl_pct >= 0 else '#da3633'}; font-weight: bold;'>{pnl_pct:+.2f}%</span></li>"
                f"<li><b>Reason:</b> {exit_reason}</li>"
                f"<li><b>Exit Order ID:</b> {exit_order_id}</li>"
                f"</ul>"
            )
        except Exception as ee:
            print(f"⚠️ Email alert error: {ee}")

    print(f"🏁 BTST Liquidator completed. Total positions exited: {len(exited_trades)}")
    return exited_trades

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Midnight Sovereign BTST T+1 Morning Liquidator")
    parser.add_argument("--force", action="store_true", help="Force exit regardless of current time")
    parser.add_argument("--dry-run", action="store_true", help="Dry run without live order placement")
    args = parser.parse_args()

    run_btst_liquidator(force_exit=args.force, dry_run=args.dry_run)
