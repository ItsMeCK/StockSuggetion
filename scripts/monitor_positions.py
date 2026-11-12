import os
import sys
import json
from datetime import datetime
from dotenv import load_dotenv

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from core.db_manager import (
    get_all_active_positions, update_position_sl, close_position,
    update_sl_order_id, update_highest_oi, update_position_profit_locked,
    update_position_qty, update_entry_fill
)
from core.live_trading import get_kite_data_client, get_kite_exec_client
from alerts.email_notifier import SovereignEmailer
from core.dynamic_exit_engine import DynamicOptionExitEngine, round_to_tick

def monitor_positions():
    """
    Minutely Position Monitor & Dynamic Exit Engine (v3.3.0-PROD)
    1. Broker Reconciliation: checks if position was manually exited.
    2. Exchange SL Recovery: restores missing/failed SL orders.
    3. Exchange SL Execution: checks if resting SL was hit on exchange.
    4. Evaluates position against 4-Tier Dynamic Option Exit Engine:
       - Tier 1: Profit Lock (+20% gain -> book 50% lot or ratchet SL to +15%).
       - Tier 2: Breakeven Ratchet (+15% gain -> move SL to cost + 1.5%).
       - Tier 3: Hard Stop-Loss Floor (-15% max premium loss).
       - Tier 4: Time-Stop (90m stagnation cut).
       - Taxonomy Boundaries: Intraday Scalp 15:00 cut, Day Trend 15:15 cut.
    """
    load_dotenv()
    live_buy_flag = os.getenv("LIVE_BUY", "False")
    is_live = (live_buy_flag.lower() == "true")
    try:
        kite_data = get_kite_data_client()
        emailer = SovereignEmailer()
    except Exception as e:
        print(f"Error initializing clients: {e}")
        return

    kite_exec = None
    if is_live:
        try:
            kite_exec = get_kite_exec_client()
        except Exception as e:
            print(f"Failed to initialize Exec client: {e}")
            return

    positions = get_all_active_positions()
    if not positions:
        return

    # Fetch current exchange state
    order_dict = {}
    net_positions = {}
    if is_live and kite_exec:
        try:
            exchange_orders = kite_exec.orders()
            order_dict = {o['order_id']: o for o in exchange_orders}
            exchange_positions = kite_exec.positions()
            net_positions = {p['tradingsymbol']: p['quantity'] for p in exchange_positions.get('net', [])}
        except Exception as e:
            print(f"Failed to fetch exchange data via Exec account: {e}")
            return

    exit_engine = DynamicOptionExitEngine()
    now_dt = datetime.now()

    for pos in positions:
        order_id = pos["order_id"]
        sl_order_id = pos["sl_order_id"]
        opt_symbol = pos["option_symbol"]
        base_symbol = pos["symbol"]

        # 1. Check Entry Buy Order Status (if available in live orderbook)
        buy_order = order_dict.get(order_id)
        buy_status = buy_order.get("status") if buy_order else None

        if buy_status in ["OPEN", "TRIGGER PENDING"]:
            print(f"[{now_dt}] ⏳ Entry Buy Order {order_id} ({opt_symbol}) is still {buy_status}. Awaiting fill...")
            continue
        elif buy_status in ["CANCELLED", "REJECTED"]:
            print(f"[{now_dt}] ⚠️ Entry Buy Order {order_id} ({opt_symbol}) was {buy_status}. Cleaning up position.")
            if sl_order_id and sl_order_id != "FAILED":
                try:
                    kite_exec.cancel_order(variety=kite_exec.VARIETY_REGULAR, order_id=sl_order_id)
                except Exception:
                    pass
            close_position(pos, pos["entry_premium"], 0.0, f"Buy Order {buy_status}")
            continue
        elif buy_status == "COMPLETE":
            avg_px = buy_order.get("average_price")
            if avg_px and avg_px > 0 and abs(pos["entry_premium"] - avg_px) > 0.05:
                print(f"[{now_dt}] 🎯 Broker Fill Reconciled for {opt_symbol}: DB {pos['entry_premium']} -> Broker {avg_px}")
                update_entry_fill(order_id, avg_px)
                pos["entry_premium"] = avg_px

        # Broker Reconciliation: Check if manually squared off (only after fill)
        qty_held = net_positions.get(opt_symbol, 0)
        if not str(order_id).startswith("PAPER_") and qty_held <= 0 and live_buy_flag.lower() == "true":
            print(f"[{now_dt}] 👁️ Broker Reconciliation: {opt_symbol} was manually exited (qty={qty_held}). Marking as CLOSED.")
            if sl_order_id and sl_order_id != "FAILED":
                try:
                    kite_exec.cancel_order(variety=kite_exec.VARIETY_REGULAR, order_id=sl_order_id)
                    print(f"Canceled resting SL {sl_order_id} due to manual exit.")
                except Exception:
                    pass
            close_position(pos, pos["entry_premium"], 0.0, "Manual Exit Detected")
            continue

        # 2. Check if SL is missing/FAILED and Buy is COMPLETE
        if sl_order_id == "FAILED":
            if order_id in order_dict and order_dict[order_id]['status'] == "COMPLETE":
                print(f"[{now_dt}] 🔄 Buy Order {order_id} is COMPLETE. Placing recovery SL for {opt_symbol}...")
                if live_buy_flag.lower() == "true":
                    try:
                        pos_sl = round_to_tick(pos["current_sl"])
                        sl_limit_price = round_to_tick(pos_sl * 0.95)
                        new_sl_id = kite_exec.place_order(
                            variety=kite_exec.VARIETY_REGULAR,
                            exchange=kite_exec.EXCHANGE_NFO,
                            tradingsymbol=opt_symbol,
                            transaction_type=kite_exec.TRANSACTION_TYPE_SELL,
                            quantity=pos["qty"],
                            product=kite_exec.PRODUCT_NRML,
                            order_type=kite_exec.ORDER_TYPE_SL,
                            trigger_price=pos_sl,
                            price=sl_limit_price
                        )
                        print(f"✅ Recovery SL Order Placed! ID: {new_sl_id}")
                        update_sl_order_id(order_id, new_sl_order_id=new_sl_id)
                        pos["sl_order_id"] = new_sl_id
                        sl_order_id = new_sl_id
                    except Exception as e:
                        print(f"❌ Failed to place recovery SL for {opt_symbol}: {e}")
                        emailer.send_live_alert(
                            f"⚠️ URGENT: Margin Failure for {opt_symbol}",
                            f"<p>Failed to place recovery SL for <b>{opt_symbol}</b>: {e}</p>"
                        )

        # 3. Check if resting SL was hit on the exchange
        if sl_order_id in order_dict:
            sl_status = order_dict[sl_order_id]['status']
            if sl_status == "COMPLETE":
                print(f"[{now_dt}] 🛑 SL Order {sl_order_id} is COMPLETE for {opt_symbol}!")
                exit_price = order_dict[sl_order_id]['average_price']
                pnl_pct = ((exit_price - pos["entry_premium"]) / pos["entry_premium"]) * 100
                close_position(pos, exit_price, pnl_pct, "Exchange SL Hit")
                emailer.send_live_alert(
                    f"🛑 STOP LOSS HIT: {opt_symbol}",
                    f"<p>Exchange executed Stop-Loss for <b>{opt_symbol}</b> at ₹{exit_price} (PnL: {pnl_pct:.2f}%).</p>"
                )
                continue

        # 4. Fetch live quotes
        try:
            quote = kite_data.quote([f"NFO:{opt_symbol}", f"NSE:{base_symbol}"])
            if f"NFO:{opt_symbol}" not in quote:
                continue
            ltp = quote[f"NFO:{opt_symbol}"]["last_price"]
            live_oi = quote[f"NFO:{opt_symbol}"].get("oi", 0)
        except Exception as e:
            print(f"Error fetching quote for {opt_symbol} via Data account: {e}")
            continue

        p_entry = pos["entry_premium"]
        highest_oi = max(pos.get("highest_oi") or 0, live_oi)
        if highest_oi > (pos.get("highest_oi") or 0):
            update_highest_oi(order_id, highest_oi)
            pos["highest_oi"] = highest_oi

        highest_high = max(float(pos.get("highest_high") or ltp), ltp)
        pos["highest_high"] = highest_high
        pnl_pct = ((ltp - p_entry) / p_entry) * 100.0

        # 5. Evaluate position through DynamicOptionExitEngine
        decision = exit_engine.evaluate_position(pos, ltp=ltp, now=now_dt)

        # 6. Act on Exit Decision
        if decision.action in ("HARD_STOP", "TIME_STOP", "TAXONOMY_CUT", "EXPIRY_STOP"):
            print(f"[{now_dt}] 🚨 {decision.action} Triggered for {opt_symbol}: {decision.reason}")
            if live_buy_flag.lower() == "true":
                try:
                    # Cancel resting SL first to release margin
                    if sl_order_id and sl_order_id != "FAILED":
                        try:
                            kite_exec.cancel_order(variety=kite_exec.VARIETY_REGULAR, order_id=sl_order_id)
                        except Exception:
                            pass
                    aggressive_limit = round(ltp * 0.90, 1)
                    kite_exec.place_order(
                        variety=kite_exec.VARIETY_REGULAR,
                        exchange=kite_exec.EXCHANGE_NFO,
                        tradingsymbol=opt_symbol,
                        transaction_type=kite_exec.TRANSACTION_TYPE_SELL,
                        quantity=pos["qty"],
                        product=kite_exec.PRODUCT_NRML,
                        order_type=kite_exec.ORDER_TYPE_LIMIT,
                        price=aggressive_limit
                    )
                    emailer.send_live_alert(
                        f"🚨 DYNAMIC EXIT: {opt_symbol} ({decision.action})",
                        f"<p>{decision.reason}</p><p>Exit Price: ₹{ltp} | PnL: {pnl_pct:.2f}%</p>"
                    )
                except Exception as e:
                    print(f"❌ Failed to execute dynamic exit order for {opt_symbol}: {e}")
                    emailer.send_live_alert(
                        f"🚨 URGENT: Exit Order Failed for {opt_symbol}",
                        f"<p>Failed to execute exit: {e}. Manually square off immediately!</p>"
                    )
                    continue
            else:
                print(f"Paper mode: {decision.action} simulated for {opt_symbol} at ₹{ltp} ({pnl_pct:.2f}%)")

            close_position(pos, ltp, pnl_pct, decision.action[:20])
            continue

        elif decision.action == "PROFIT_LOCK":
            print(f"[{now_dt}] 🟢 PROFIT LOCK for {opt_symbol}: {decision.reason} (New SL: ₹{decision.new_sl})")
            # If multi-lot, book 50%
            order_qty = net_positions.get(opt_symbol) or pos["qty"]
            if decision.book_lot_pct > 0.0 and order_qty > 1:
                book_qty = int(order_qty * decision.book_lot_pct)
                if book_qty > 0:
                    if live_buy_flag.lower() == "true":
                        try:
                            # NSE blocks ORDER_TYPE_MARKET for stock options
                            limit_partial_px = max(0.05, round_to_tick(ltp * 0.90))
                            kite_exec.place_order(
                                variety=kite_exec.VARIETY_REGULAR,
                                exchange=kite_exec.EXCHANGE_NFO,
                                tradingsymbol=opt_symbol,
                                transaction_type=kite_exec.TRANSACTION_TYPE_SELL,
                                quantity=book_qty,
                                product=kite_exec.PRODUCT_NRML,
                                order_type=kite_exec.ORDER_TYPE_LIMIT,
                                price=limit_partial_px
                            )
                            print(f"✅ Booked {book_qty} lots at Tier 1 profit target")
                            pos["qty"] = max(0, order_qty - book_qty)
                            update_position_qty(pos["order_id"], pos["qty"])
                            print(f"💾 Persisted reduced quantity {pos['qty']} to database for {order_id}")
                        except Exception as e:
                            print(f"Warning: Failed partial profit booking order: {e}")
                    else:
                        print(f"Paper mode: Simulated partial booking of {book_qty} lots for {opt_symbol}")
                        pos["qty"] = max(0, order_qty - book_qty)
                        update_position_qty(pos["order_id"], pos["qty"])

            # Ratchet SL
            new_sl = round_to_tick(decision.new_sl)
            sl_qty = net_positions.get(opt_symbol) or pos["qty"]
            if live_buy_flag.lower() == "true" and sl_order_id and sl_order_id != "FAILED":
                try:
                    kite_exec.modify_order(
                        variety=kite_exec.VARIETY_REGULAR,
                        order_id=sl_order_id,
                        order_type=kite_exec.ORDER_TYPE_SL,
                        quantity=sl_qty,
                        trigger_price=new_sl,
                        price=round_to_tick(new_sl * 0.95)
                    )
                except Exception as e:
                    print(f"Warning: Failed to modify SL on exchange: {e}")

            pos["current_sl"] = new_sl
            pos["trailing_active"] = True
            pos["profit_locked"] = True
            update_position_sl(pos['order_id'], new_sl, highest_high, trailing_active=True)
            update_position_profit_locked(pos['order_id'], profit_locked=True)
            emailer.send_live_alert(
                f"🟢 PROFIT LOCKED: +20% on {opt_symbol}",
                f"<p>Tier 1 Profit Target Hit! Stop-Loss moved to +15% (₹{new_sl}).</p>"
            )

        elif decision.action in ("MOVE_TO_BREAKEVEN", "UPDATE_TRAILING_SL"):
            new_sl = round_to_tick(decision.new_sl)
            print(f"[{now_dt}] 📈 {decision.action} for {opt_symbol}: Moving SL to ₹{new_sl}")
            sl_qty = net_positions.get(opt_symbol) or pos["qty"]
            if live_buy_flag.lower() == "true" and sl_order_id and sl_order_id != "FAILED":
                try:
                    kite_exec.modify_order(
                        variety=kite_exec.VARIETY_REGULAR,
                        order_id=sl_order_id,
                        order_type=kite_exec.ORDER_TYPE_SL,
                        quantity=sl_qty,
                        trigger_price=new_sl,
                        price=round_to_tick(new_sl * 0.95)
                    )
                except Exception as e:
                    print(f"Warning: Failed to modify SL on exchange: {e}")

            pos["current_sl"] = new_sl
            pos["trailing_active"] = True
            update_position_sl(pos['order_id'], new_sl, highest_high, trailing_active=True)
            if decision.action == "MOVE_TO_BREAKEVEN":
                emailer.send_live_alert(
                    f"🛡️ BREAKEVEN RATCHET: {opt_symbol}",
                    f"<p>Position moved to breakeven stop-loss (₹{new_sl}) covering transaction costs.</p>"
                )

        else: # HOLD
            if highest_high > float(pos.get("highest_high", 0)):
                update_position_sl(pos['order_id'], pos['current_sl'], highest_high, pos.get('trailing_active', False))

            # SL Self-Healing: Reconcile exchange SL trigger with pos['current_sl'] if exchange order lag exists
            if live_buy_flag.lower() == "true" and sl_order_id and sl_order_id in order_dict:
                sl_order = order_dict[sl_order_id]
                if sl_order.get("status") in ("OPEN", "TRIGGER PENDING"):
                    target_sl = round_to_tick(pos["current_sl"])
                    exchange_trigger = round_to_tick(sl_order.get("trigger_price") or 0.0)
                    sl_qty = net_positions.get(opt_symbol) or pos["qty"]
                    if exchange_trigger > 0 and exchange_trigger < target_sl:
                        print(f"[{now_dt}] 🔄 Reconciling SL on exchange: Order trigger ₹{exchange_trigger} < Target SL ₹{target_sl}. Modifying order {sl_order_id}...")
                        try:
                            kite_exec.modify_order(
                                variety=kite_exec.VARIETY_REGULAR,
                                order_id=sl_order_id,
                                order_type=kite_exec.ORDER_TYPE_SL,
                                quantity=sl_qty,
                                trigger_price=target_sl,
                                price=round_to_tick(target_sl * 0.95)
                            )
                            print(f"✅ Exchange SL successfully reconciled to ₹{target_sl}")
                        except Exception as e:
                            print(f"Warning: Failed to reconcile SL on exchange: {e}")

        trail_status = pos.get("trailing_active", False)
        lock_status = pos.get("profit_locked", False)
        print(f"[{now_dt}] 🟢 {opt_symbol} | LTP: {ltp} (PnL: {pnl_pct:+.2f}%) | SL: {pos['current_sl']:.2f} | Trail: {trail_status} | Locked: {lock_status}")

if __name__ == "__main__":
    monitor_positions()
