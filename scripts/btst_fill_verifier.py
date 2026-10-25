#!/usr/bin/env python3
"""
scripts/btst_fill_verifier.py

3:26 PM Operational Safety Net for Engine 2 (Type C BTST / Anticipatory Trades).
Runs daily at 15:26 IST via cron.

Core Objectives:
1. Verify Order Fills: Query Zerodha Kite API to confirm whether the 3:15 PM SL-Limit order filled.
2. Unfilled Order Cancellation: If status is OPEN or TRIGGER PENDING at 15:26, cancel order and close position
   record as CANCELLED_UNFILLED_AT_326 to prevent orphan overnight execution before the 15:30 close.
3. Slippage Audit: Check fill price against expected limit price. If slippage > 1.5%, trigger email alert.
4. Stop-Loss Guarantee: Verify that a resting protective SL order exists. If missing/failed, place it immediately
   at the -15% hard floor before the 15:30 close.
5. Paper Simulation Integrity: Reconcile and audit paper positions with live quotes and ensure SL state is populated.
"""

import os
import sys
import argparse
from datetime import datetime, time
import pytz
from dotenv import load_dotenv

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from core.db_manager import (
    get_all_active_positions,
    close_position,
    update_sl_order_id,
    update_entry_fill
)
from core.live_trading import get_kite_data_client, get_kite_exec_client
from alerts.email_notifier import SovereignEmailer
from scripts.btst_liquidator import is_btst_position, round_to_tick


def get_position_entry_date(pos: dict, default_date=None):
    """Parses entry_time to a date object in IST."""
    entry_time = pos.get("entry_time")
    if not entry_time:
        return default_date
    if isinstance(entry_time, datetime):
        return entry_time.date()
    if isinstance(entry_time, str):
        try:
            return datetime.fromisoformat(entry_time).date()
        except Exception:
            return default_date
    return default_date


def run_btst_fill_verifier(now: datetime = None, force_check: bool = False, dry_run: bool = False) -> list:
    """
    Evaluates today's 3:15 PM BTST positions at 3:26 PM IST.
    """
    load_dotenv()
    ist = pytz.timezone("Asia/Kolkata")
    if now is None:
        now = datetime.now(ist)
    elif now.tzinfo is None:
        now = ist.localize(now)

    print(f"\n=======================================================")
    print(f"🛡️ RUNNING 3:26 PM BTST FILL VERIFIER AT: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"=======================================================")

    current_time = now.time()
    in_check_window = time(15, 25) <= current_time <= time(15, 29)

    if not in_check_window and not force_check:
        print(f"ℹ️ Current time {current_time} is outside the 15:25 - 15:29 PM safety window. Skipping verifier.")
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

    today_date = now.date()
    today_btst_positions = [
        p for p in active_positions 
        if is_btst_position(p) and get_position_entry_date(p, today_date) == today_date
    ]

    if not today_btst_positions:
        print(f"ℹ️ Found {len(active_positions)} active position(s), but none entered today as BTST/Engine 2.")
        return []

    print(f"🎯 Found {len(today_btst_positions)} today's BTST position(s) to verify.")

    verification_results = []

    for pos in today_btst_positions:
        order_id = pos.get("order_id", "")
        opt_symbol = pos.get("option_symbol", "")
        symbol = pos.get("symbol", "")
        status = pos.get("status", "")
        entry_premium = float(pos.get("entry_premium", 0.0))
        qty = int(pos.get("qty", 0))
        sl_order_id = pos.get("sl_order_id")

        is_live = status == "LIVE_TRADED" and not order_id.startswith("PAPER_")

        if is_live:
            print(f"\n🔍 Verifying LIVE order: {order_id} ({opt_symbol})...")
            try:
                history = kite_exec.order_history(order_id)
                latest_order = history[-1] if history else {}
                broker_status = str(latest_order.get("status", "UNKNOWN")).upper()
            except Exception as e:
                print(f"❌ Failed to fetch order history from broker for {order_id}: {e}")
                broker_status = "FETCH_ERROR"
                latest_order = {}

            print(f"   Broker Order Status: {broker_status}")

            if broker_status == "COMPLETE":
                avg_price = float(latest_order.get("average_price") or entry_premium)
                slippage_pct = ((avg_price - entry_premium) / entry_premium) * 100.0 if entry_premium > 0 else 0.0
                print(f"   Filled at: ₹{avg_price:.2f} (Expected Limit: ₹{entry_premium:.2f}, Slippage: {slippage_pct:+.2f}%)")

                # 1. Slippage check
                slippage_alert = False
                if slippage_pct > 1.5:
                    slippage_alert = True
                    print(f"⚠️ HIGH SLIPPAGE ALERT: {slippage_pct:.2f}% exceeds 1.5% threshold!")
                    emailer.send_live_alert(
                        f"⚠️ HIGH SLIPPAGE ALERT: {symbol} (+{slippage_pct:.2f}%)",
                        f"""
                        <p>BTST order for <b>{symbol}</b> ({opt_symbol}) executed with elevated slippage:</p>
                        <ul>
                            <li>Expected Price: ₹{entry_premium:.2f}</li>
                            <li>Actual Fill Price: ₹{avg_price:.2f}</li>
                            <li>Slippage: <b>+{slippage_pct:.2f}%</b> (Threshold: 1.5%)</li>
                        </ul>
                        """
                    )

                # 2. Protective Stop-Loss check
                sl_needed = False
                if not sl_order_id or sl_order_id in ("FAILED", "NONE"):
                    sl_needed = True
                else:
                    # Verify if existing sl_order_id is active on broker
                    try:
                        sl_hist = kite_exec.order_history(sl_order_id)
                        sl_stat = str(sl_hist[-1].get("status", "")).upper() if sl_hist else ""
                        if sl_stat in ("CANCELLED", "REJECTED"):
                            print(f"⚠️ Existing SL order {sl_order_id} is {sl_stat}. Needs fresh SL order.")
                            sl_needed = True
                    except Exception:
                        sl_needed = True

                new_sl_order = None
                initial_sl_price = round_to_tick(avg_price * 0.85)  # -15% hard floor
                if sl_needed:
                    sl_limit_price = round_to_tick(initial_sl_price * 0.95)
                    print(f"   🛡️ Placing Protective SL-M Order: Trigger ₹{initial_sl_price}, Limit ₹{sl_limit_price}")
                    if not dry_run:
                        try:
                            new_sl_order = kite_exec.place_order(
                                variety=kite_exec.VARIETY_REGULAR,
                                exchange=kite_exec.EXCHANGE_NFO,
                                tradingsymbol=opt_symbol,
                                transaction_type=kite_exec.TRANSACTION_TYPE_SELL,
                                quantity=qty,
                                product=kite_exec.PRODUCT_NRML,
                                order_type=kite_exec.ORDER_TYPE_SL,
                                trigger_price=initial_sl_price,
                                price=sl_limit_price
                            )
                            print(f"   ✅ Protective SL Placed! ID: {new_sl_order}")
                            update_entry_fill(order_id, fill_price=avg_price, new_sl=initial_sl_price, new_sl_order_id=new_sl_order)
                            emailer.send_live_alert(
                                f"🛡️ PROTECTIVE SL PLACED: {symbol} at ₹{initial_sl_price}",
                                f"<p>Placed protective resting SL order {new_sl_order} for {opt_symbol} at ₹{initial_sl_price} (-15% stop loss floor).</p>"
                            )
                        except Exception as e:
                            print(f"   ❌ Failed to place protective SL order: {e}")
                    else:
                        print("   [DRY RUN] Would place SL order.")
                else:
                    if not dry_run and avg_price != entry_premium:
                        update_entry_fill(order_id, fill_price=avg_price)

                verification_results.append({
                    "symbol": symbol,
                    "order_id": order_id,
                    "action": "VERIFIED_FILLED",
                    "fill_price": avg_price,
                    "slippage_pct": slippage_pct,
                    "sl_order_id": new_sl_order or sl_order_id
                })

            elif broker_status in ("OPEN", "TRIGGER PENDING"):
                # Unfilled order at 3:26 PM -> Cancel order and close DB position
                print(f"🚨 UNFILLED ORDER AT 3:26 PM: Cancelling {order_id} ({opt_symbol}) to eliminate overnight risk.")
                if not dry_run:
                    try:
                        kite_exec.cancel_order(variety=kite_exec.VARIETY_REGULAR, order_id=order_id)
                        print(f"   ✅ Cancelled broker order {order_id}")
                    except Exception as e:
                        print(f"   ⚠️ Warning: Cancel order failed on broker: {e}")
                    close_position(pos, exit_price=entry_premium, pnl_pct=0.0, status_override="CANCELLED_UNFILLED_AT_326")

                emailer.send_live_alert(
                    f"🚨 BTST ORDER UNFILLED AT 3:26 PM - CANCELLED: {symbol}",
                    f"""
                    <p>BTST buy order for <b>{symbol}</b> ({opt_symbol}) remained <b>{broker_status}</b> at 15:26 IST.</p>
                    <p>The order was automatically cancelled to eliminate unmanaged overnight exposure into 15:30 close.</p>
                    """
                )

                verification_results.append({
                    "symbol": symbol,
                    "order_id": order_id,
                    "action": "CANCELLED_UNFILLED",
                    "broker_status": broker_status
                })

            elif broker_status in ("CANCELLED", "REJECTED"):
                print(f"⚠️ Order {order_id} was already {broker_status} on broker. Updating database record.")
                if not dry_run:
                    close_position(pos, exit_price=entry_premium, pnl_pct=0.0, status_override=f"ORDER_{broker_status}")
                emailer.send_live_alert(
                    f"⚠️ BTST ORDER {broker_status}: {symbol}",
                    f"<p>Order {order_id} for {symbol} ({opt_symbol}) was marked {broker_status} by exchange.</p>"
                )
                verification_results.append({
                    "symbol": symbol,
                    "order_id": order_id,
                    "action": f"RECORD_{broker_status}",
                    "broker_status": broker_status
                })

        else:
            # Paper trade reconciliation
            print(f"\n📝 Verifying PAPER BTST order: {order_id} ({opt_symbol})...")
            ltp = entry_premium
            try:
                quote = kite_data.quote([f"NFO:{opt_symbol}"])
                opt_data = quote.get(f"NFO:{opt_symbol}", {})
                if "last_price" in opt_data:
                    ltp = float(opt_data["last_price"])
            except Exception as e:
                print(f"   ⚠️ Could not fetch quote for paper option: {e}")

            # Ensure paper current_sl exists
            current_sl = float(pos.get("current_sl") or 0.0)
            if current_sl <= 0:
                current_sl = round_to_tick(entry_premium * 0.85)
                if not dry_run:
                    update_entry_fill(order_id, fill_price=entry_premium, new_sl=current_sl)

            print(f"   ✅ Paper verification complete. Entry: ₹{entry_premium:.2f}, LTP: ₹{ltp:.2f}, SL: ₹{current_sl:.2f}")
            verification_results.append({
                "symbol": symbol,
                "order_id": order_id,
                "action": "VERIFIED_PAPER",
                "ltp": ltp,
                "current_sl": current_sl
            })

    print(f"\n🏁 Finished BTST fill verification. Evaluated {len(verification_results)} position(s).")
    return verification_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="3:26 PM BTST Fill Verifier and Safety Net")
    parser.add_argument("--force", action="store_true", help="Bypass 15:25-15:29 time check")
    parser.add_argument("--dry-run", action="store_true", help="Audit only without sending broker orders or DB updates")
    args = parser.parse_args()

    run_btst_fill_verifier(force_check=args.force, dry_run=args.dry_run)
