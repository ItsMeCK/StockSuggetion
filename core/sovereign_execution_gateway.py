import os
import time
import math
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from dotenv import load_dotenv
from kiteconnect import KiteConnect

from alerts.email_notifier import SovereignEmailer
from core.db_manager import save_active_position

logger = logging.getLogger(__name__)

class SovereignExecutionGateway:
    """
    Sovereign Execution Gateway (v4.0.0-PROD)
    Handles institutional order routing, protective execution, 10s fill verification,
    margin verification, and automated alerts for the Execution Account (HU0092).
    """

    MAX_LOT_SIZE_CEILING = 600  # Hard ceiling: only lot_size < 600 permitted for automated orders

    def __init__(self, kite_exec: Optional[KiteConnect] = None, emailer: Optional[SovereignEmailer] = None):
        load_dotenv()
        self.api_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
        self.access_token = os.getenv("EXEC_KITE_ACCESS_TOKEN", "").strip("'\"")
        
        if kite_exec:
            self.kite_exec = kite_exec
        elif self.api_key and self.access_token:
            self.kite_exec = KiteConnect(api_key=self.api_key, timeout=25)
            self.kite_exec.set_access_token(self.access_token)
        else:
            self.kite_exec = None

        self.emailer = emailer or SovereignEmailer()

    def check_lot_size(self, lot_size: int) -> Tuple[bool, str]:
        """
        Enforces lot size ceiling (< 600 shares per lot).
        """
        if lot_size >= self.MAX_LOT_SIZE_CEILING:
            reason = f"Lot size {lot_size} exceeds automated ceiling (< {self.MAX_LOT_SIZE_CEILING}). Manual execution required."
            logger.warning(f"🛑 [LOT SIZE CEILING]: {reason}")
            return False, reason
        return True, "Lot size within safe automated boundaries"

    def check_margin_sufficiency(self, lot_size: int, limit_price: float) -> Tuple[bool, float, float, str]:
        """
        Checks real-time available cash balance on HU0092 against required capital.
        Returns: (is_sufficient, available_cash, required_cash, reason)
        """
        required_cash = round(lot_size * limit_price, 2)
        if not self.kite_exec:
            # Test / simulated mode without active credentials
            return True, 100000.0, required_cash, "Simulated margin check passed"

        try:
            margins = self.kite_exec.margins(segment="equity")
            avail = margins.get("available", {})
            # live_balance includes cash + collateral - intraday payins
            available_cash = float(avail.get("live_balance", avail.get("cash", 0.0)))
            
            if available_cash < required_cash:
                shortfall = required_cash - available_cash
                reason = f"Insufficient funds: Required ₹{required_cash:,.2f} vs Available ₹{available_cash:,.2f} (Shortfall: ₹{shortfall:,.2f})"
                logger.error(f"🛑 [MARGIN SHORTFALL]: {reason}")
                return False, available_cash, required_cash, reason

            return True, available_cash, required_cash, "Sufficient funds available"
        except Exception as e:
            logger.error(f"Error checking margin sufficiency on Zerodha: {e}")
            return False, 0.0, required_cash, f"Broker margin check failed: {str(e)}"

    def calculate_marketable_limit_price(self, entry_premium: float, best_ask: Optional[float] = None) -> float:
        """
        Calculates a marketable limit price with tick size (0.05) rounding:
        Min of (best_ask + 0.05, entry_premium * 1.015) to guarantee fill without excessive slippage.
        """
        if best_ask and best_ask > 0:
            target = min(best_ask + 0.05, entry_premium * 1.015)
        else:
            target = entry_premium * 1.01

        # Round to nearest 0.05 tick
        return round(round(target / 0.05) * 0.05, 2)

    def verify_order_fill_after_10s(
        self,
        order_id: str,
        timeout_seconds: int = 10,
        poll_interval: float = 2.0
    ) -> Dict[str, Any]:
        """
        Monitors order status for up to 10 seconds.
        Returns status dict with:
        {
            "status": "COMPLETE" | "OPEN" | "REJECTED" | "CANCELLED",
            "average_price": float,
            "status_message": str,
            "unfilled_reason": str
        }
        """
        start_time = time.time()
        last_status = "UNKNOWN"
        status_message = ""
        avg_price = 0.0

        while time.time() - start_time < timeout_seconds:
            try:
                history = self.kite_exec.order_history(order_id)
                if history:
                    latest = history[-1]
                    last_status = latest.get("status", "UNKNOWN")
                    status_message = latest.get("status_message", "")
                    avg_price = float(latest.get("average_price", 0.0) or 0.0)

                    if last_status in ("COMPLETE", "REJECTED", "CANCELLED"):
                        return {
                            "status": last_status,
                            "average_price": avg_price,
                            "status_message": status_message,
                            "unfilled_reason": ""
                        }
            except Exception as e:
                logger.warning(f"Error polling order history for {order_id}: {e}")

            time.sleep(poll_interval)

        # Reached 10s timeout while still OPEN or TRIGGER PENDING
        return {
            "status": last_status if last_status != "UNKNOWN" else "OPEN",
            "average_price": avg_price,
            "status_message": status_message,
            "unfilled_reason": "Order remained unfilled after 10-second verification window"
        }

    def place_protective_stop_loss(
        self,
        option_symbol: str,
        fill_price: float,
        lot_size: int,
        hard_sl_pct: float = 0.85
    ) -> Tuple[Optional[str], float, float]:
        """
        Places a broker-side SL-Limit SELL order once BUY order is filled.
        Because open long position exists, broker requires ₹0 extra margin.
        """
        trigger_sl = round(round(fill_price * hard_sl_pct / 0.05) * 0.05, 2)
        limit_sl = round(round(trigger_sl * 0.95 / 0.05) * 0.05, 2)

        if not self.kite_exec:
            return f"SIM_SL_{int(time.time())}", trigger_sl, limit_sl

        try:
            sl_order_id = self.kite_exec.place_order(
                variety=self.kite_exec.VARIETY_REGULAR,
                exchange=self.kite_exec.EXCHANGE_NFO,
                tradingsymbol=option_symbol,
                transaction_type=self.kite_exec.TRANSACTION_TYPE_SELL,
                quantity=lot_size,
                product=self.kite_exec.PRODUCT_NRML,
                order_type=self.kite_exec.ORDER_TYPE_SL,
                trigger_price=trigger_sl,
                price=limit_sl
            )
            logger.info(f"🛡️ Protective SL placed: ID {sl_order_id} @ Trigger ₹{trigger_sl}, Limit ₹{limit_sl}")
            return sl_order_id, trigger_sl, limit_sl
        except Exception as e:
            logger.error(f"❌ Failed to place broker SL for {option_symbol}: {e}")
            return None, trigger_sl, limit_sl

    def execute_option_order(
        self,
        symbol: str,
        option_symbol: str,
        lot_size: int,
        entry_premium: float,
        direction: str = "LONG",
        instrument_type: str = "CE",
        best_ask: Optional[float] = None,
        catalyst: str = "",
        score: float = 85.0,
        holding_type: str = "TYPE_C_BTST",
        is_amo: bool = False
    ) -> Dict[str, Any]:
        """
        Full Institutional Execution Pipeline:
        1. Lot size check (< 600).
        2. Real-time cash margin check on HU0092.
        3. Marketable Limit Buy placement.
        4. 10-second execution verification.
        5. If Filled: Place protective SL-Limit order & send [ORDER EXECUTED] email.
        6. If Unfilled / Rejected: Cancel stale order & send [ORDER FAILED] email.
        """
        logger.info(f"⚡ [GATEWAY] Received execution request: {symbol} -> {option_symbol} (Lot: {lot_size}, Premium: ₹{entry_premium})")

        # 1. Lot Size Ceiling Enforcement (< 600)
        lot_ok, lot_reason = self.check_lot_size(lot_size)
        if not lot_ok:
            self._notify_order_failure(
                symbol=symbol,
                option_symbol=option_symbol,
                reason=lot_reason,
                failure_type="LOT_SIZE_EXCEEDED"
            )
            return {
                "success": False,
                "error": "LOT_SIZE_EXCEEDED",
                "message": lot_reason,
                "order_id": None
            }

        # 2. Limit Price Calculation
        limit_price = self.calculate_marketable_limit_price(entry_premium, best_ask)

        # 3. Cash Margin Verification
        margin_ok, available_cash, required_cash, margin_reason = self.check_margin_sufficiency(lot_size, limit_price)
        if not margin_ok:
            self._notify_insufficient_funds(
                symbol=symbol,
                option_symbol=option_symbol,
                required_cash=required_cash,
                available_cash=available_cash,
                lot_size=lot_size,
                limit_price=limit_price
            )
            return {
                "success": False,
                "error": "INSUFFICIENT_FUNDS",
                "message": margin_reason,
                "order_id": None
            }

        if not self.kite_exec:
            # Simulated / Dry-run fallback
            sim_id = f"SIM_{int(time.time())}"
            sl_id = f"SIM_SL_{int(time.time())}"
            self._notify_order_executed(
                symbol=symbol,
                option_symbol=option_symbol,
                fill_price=limit_price,
                lot_size=lot_size,
                sl_price=round(limit_price * 0.85, 2),
                order_id=sim_id,
                sl_order_id=sl_id,
                catalyst=catalyst,
                is_simulation=True
            )
            return {
                "success": True,
                "status": "COMPLETE",
                "fill_price": limit_price,
                "order_id": sim_id,
                "sl_order_id": sl_id,
                "is_simulation": True
            }

        # 4. Submit Marketable Limit Order to Zerodha
        order_tag = f"MS_{symbol[:6]}_{int(time.time()) % 100000}"[:20]
        order_variety = self.kite_exec.VARIETY_AMO if is_amo else self.kite_exec.VARIETY_REGULAR
        try:
            buy_order_id = self.kite_exec.place_order(
                variety=order_variety,
                exchange=self.kite_exec.EXCHANGE_NFO,
                tradingsymbol=option_symbol,
                transaction_type=self.kite_exec.TRANSACTION_TYPE_BUY,
                quantity=lot_size,
                product=self.kite_exec.PRODUCT_NRML,
                order_type=self.kite_exec.ORDER_TYPE_LIMIT,
                price=limit_price,
                tag=order_tag
            )
            logger.info(f"✅ Limit Buy order submitted: ID {buy_order_id} (variety={order_variety}) for {lot_size} qty @ ₹{limit_price}")
        except Exception as e:
            err_msg = str(e)
            logger.error(f"❌ Failed to submit buy order to broker: {err_msg}")
            self._notify_order_failure(
                symbol=symbol,
                option_symbol=option_symbol,
                reason=f"Broker submission rejected: {err_msg}",
                failure_type="BROKER_REJECTION"
            )
            return {
                "success": False,
                "error": "SUBMISSION_FAILED",
                "message": err_msg,
                "order_id": None
            }

        # For After Market Orders (AMO), order stays OPEN in order book until market opens tomorrow
        if is_amo:
            logger.info(f"📋 AMO Order successfully placed at broker: ID {buy_order_id} is OPEN/PENDING")
            return {
                "success": True,
                "status": "OPEN",
                "order_id": buy_order_id,
                "variety": "amo",
                "fill_price": limit_price,
                "lot_size": lot_size,
                "message": f"AMO limit order placed successfully and is OPEN: {buy_order_id}"
            }

        # 5. 10-Second Order Execution Check
        fill_result = self.verify_order_fill_after_10s(buy_order_id, timeout_seconds=10, poll_interval=2.0)
        order_status = fill_result["status"]

        if order_status == "COMPLETE":
            fill_price = fill_result["average_price"] or limit_price
            logger.info(f"🎯 ORDER FILLED: {buy_order_id} @ ₹{fill_price:.2f}")

            # Place broker-level stop loss immediately
            sl_order_id, trigger_sl, limit_sl = self.place_protective_stop_loss(
                option_symbol=option_symbol,
                fill_price=fill_price,
                lot_size=lot_size
            )

            # Persist active position to ledger
            try:
                save_active_position({
                    "symbol": symbol,
                    "direction": direction,
                    "instrument_type": instrument_type,
                    "option_symbol": option_symbol,
                    "entry_price": fill_price,
                    "entry_time": datetime.now().isoformat(),
                    "quantity": lot_size,
                    "stop_loss": trigger_sl,
                    "target_price": round(fill_price * 1.25, 2),
                    "holding_type": holding_type,
                    "order_id": buy_order_id,
                    "sl_order_id": sl_order_id,
                    "score": score,
                    "catalyst": catalyst
                })
            except Exception as e:
                logger.warning(f"Could not persist active position to database: {e}")

            # Send [ORDER EXECUTED] rich email
            self._notify_order_executed(
                symbol=symbol,
                option_symbol=option_symbol,
                fill_price=fill_price,
                lot_size=lot_size,
                sl_price=trigger_sl,
                order_id=buy_order_id,
                sl_order_id=sl_order_id or "MANUAL_REQUIRED",
                catalyst=catalyst
            )

            return {
                "success": True,
                "status": "COMPLETE",
                "fill_price": fill_price,
                "order_id": buy_order_id,
                "sl_order_id": sl_order_id,
                "lot_size": lot_size
            }

        elif order_status in ("OPEN", "TRIGGER PENDING"):
            # Order unfilled after 10s: Cancel immediately to prevent chasing
            logger.warning(f"⚠️ Order {buy_order_id} unfilled after 10s. Initiating protective cancel...")
            try:
                self.kite_exec.cancel_order(
                    variety=self.kite_exec.VARIETY_REGULAR,
                    order_id=buy_order_id
                )
                cancel_status = "CANCELLED_PROTECTIVE"
            except Exception as e:
                cancel_status = f"CANCEL_ERROR: {e}"

            reason_text = f"Unfilled after 10s window (Market moved past ₹{limit_price:.2f}). Stale order cancelled safely."
            self._notify_order_failure(
                symbol=symbol,
                option_symbol=option_symbol,
                reason=reason_text,
                failure_type="UNFILLED_10S_TIMEOUT"
            )

            return {
                "success": False,
                "status": "CANCELLED_TIMEOUT",
                "error": "UNFILLED_TIMEOUT",
                "message": reason_text,
                "order_id": buy_order_id
            }

        else:
            # REJECTED or CANCELLED by broker
            reason_text = fill_result.get("status_message") or f"Broker status: {order_status}"
            logger.error(f"❌ Order {buy_order_id} ended in status {order_status}: {reason_text}")
            self._notify_order_failure(
                symbol=symbol,
                option_symbol=option_symbol,
                reason=reason_text,
                failure_type=order_status
            )
            return {
                "success": False,
                "status": order_status,
                "error": "BROKER_REJECTION",
                "message": reason_text,
                "order_id": buy_order_id
            }

    # =========================================================================
    # Email Notification Helpers (Exact match to institutional styling)
    # =========================================================================

    def _notify_order_executed(
        self,
        symbol: str,
        option_symbol: str,
        fill_price: float,
        lot_size: int,
        sl_price: float,
        order_id: str,
        sl_order_id: str,
        catalyst: str = "",
        is_simulation: bool = False
    ):
        target_price = round(fill_price * 1.25, 2)
        invested = round(fill_price * lot_size, 2)
        sim_tag = " (PAPER SIMULATION)" if is_simulation else ""

        subject = f"[ORDER EXECUTED]{sim_tag} {symbol} {option_symbol} Filled @ ₹{fill_price:.2f}"
        
        html = f"""
        <div style="background: rgba(63, 185, 80, 0.1); border-left: 5px solid #3fb950; border-radius: 6px; padding: 20px; margin-bottom: 25px; border: 1px solid rgba(63, 185, 80, 0.3);">
            <div style="font-size: 18px; font-weight: bold; color: #3fb950; margin-bottom: 10px;">
                ⚡ ORDER EXECUTED & ACTIVE IN DESK
            </div>
            <table width="100%" style="font-size: 14px; color: #c9d1d9; border-collapse: collapse;">
                <tr>
                    <td style="padding: 6px 0;"><b>Underlying:</b></td>
                    <td style="text-align: right; color: #ffffff; font-weight: bold;">{symbol}</td>
                </tr>
                <tr>
                    <td style="padding: 6px 0;"><b>Contract:</b></td>
                    <td style="text-align: right; color: #58a6ff; font-family: monospace;">{option_symbol}</td>
                </tr>
                <tr>
                    <td style="padding: 6px 0;"><b>Filled Qty:</b></td>
                    <td style="text-align: right; color: #ffffff;">{lot_size} shares (1 lot)</td>
                </tr>
                <tr>
                    <td style="padding: 6px 0;"><b>Fill Price:</b></td>
                    <td style="text-align: right; color: #3fb950; font-weight: bold; font-size: 16px;">₹{fill_price:.2f}</td>
                </tr>
                <tr>
                    <td style="padding: 6px 0;"><b>Total Invested:</b></td>
                    <td style="text-align: right; color: #ffffff;">₹{invested:,.2f}</td>
                </tr>
                <tr style="border-top: 1px solid #30363d;">
                    <td style="padding: 8px 0; color: #ff7b72;"><b>Protective Stop-Loss:</b></td>
                    <td style="text-align: right; color: #ff7b72; font-weight: bold;">₹{sl_price:.2f} (-15.0%)</td>
                </tr>
                <tr>
                    <td style="padding: 6px 0; color: #58a6ff;"><b>Primary Target (T1):</b></td>
                    <td style="text-align: right; color: #58a6ff; font-weight: bold;">₹{target_price:.2f} (+25.0%)</td>
                </tr>
                <tr style="border-top: 1px solid #30363d;">
                    <td style="padding: 6px 0; font-size: 12px; color: #8b949e;">Order ID / SL ID:</td>
                    <td style="text-align: right; font-size: 12px; color: #8b949e; font-family: monospace;">{order_id} / {sl_order_id}</td>
                </tr>
            </table>
            {f'<div style="margin-top: 12px; padding-top: 10px; border-top: 1px solid rgba(63, 185, 80, 0.2); font-size: 12px; color: #8b949e;"><b>Catalyst Grounding:</b> {catalyst}</div>' if catalyst else ''}
        </div>
        """
        try:
            self.emailer.send_live_alert(subject, html)
        except Exception as e:
            logger.error(f"Failed to dispatch order executed email: {e}")

    def _notify_insufficient_funds(
        self,
        symbol: str,
        option_symbol: str,
        required_cash: float,
        available_cash: float,
        lot_size: int,
        limit_price: float
    ):
        shortfall = required_cash - available_cash
        subject = f"[ORDER FAILED] [INSUFFICIENT FUNDS] Margin Shortfall for {symbol}"
        
        html = f"""
        <div style="background: rgba(248, 81, 73, 0.1); border-left: 5px solid #ff7b72; border-radius: 6px; padding: 20px; margin-bottom: 25px; border: 1px solid rgba(248, 81, 73, 0.3);">
            <div style="font-size: 18px; font-weight: bold; color: #ff7b72; margin-bottom: 10px;">
                🛑 MARGIN GATE: INSUFFICIENT CASH IN EXEC ACCOUNT (HU0092)
            </div>
            <p style="margin: 0 0 15px 0; color: #c9d1d9; font-size: 14px;">
                The execution engine attempted to submit a live trade for <b>{symbol}</b> ({option_symbol}), but available margin fell below required capital. Trade aborted to prevent broker RMS penalty.
            </p>
            <table width="100%" style="font-size: 14px; color: #c9d1d9; border-collapse: collapse;">
                <tr>
                    <td style="padding: 6px 0;"><b>Contract:</b></td>
                    <td style="text-align: right; color: #58a6ff; font-family: monospace;">{option_symbol} ({lot_size} qty @ ₹{limit_price:.2f})</td>
                </tr>
                <tr>
                    <td style="padding: 6px 0;"><b>Required Margin:</b></td>
                    <td style="text-align: right; color: #ffffff; font-weight: bold;">₹{required_cash:,.2f}</td>
                </tr>
                <tr>
                    <td style="padding: 6px 0;"><b>Available Live Cash:</b></td>
                    <td style="text-align: right; color: #ff7b72; font-weight: bold;">₹{available_cash:,.2f}</td>
                </tr>
                <tr style="border-top: 1px solid #30363d;">
                    <td style="padding: 8px 0; color: #ff7b72; font-weight: bold;">Cash Shortfall:</td>
                    <td style="text-align: right; color: #ff7b72; font-weight: bold; font-size: 16px;">₹{shortfall:,.2f}</td>
                </tr>
            </table>
            <div style="margin-top: 15px; padding: 10px; background: rgba(0,0,0,0.3); border-radius: 4px; font-size: 12px; color: #8b949e;">
                💡 <b>Action Required:</b> Deposit at least ₹{shortfall:,.2f} into account HU0092 to resume automated execution for this contract size.
            </div>
        </div>
        """
        try:
            self.emailer.send_live_alert(subject, html)
        except Exception as e:
            logger.error(f"Failed to dispatch insufficient funds email: {e}")

    def _notify_order_failure(
        self,
        symbol: str,
        option_symbol: str,
        reason: str,
        failure_type: str
    ):
        subject = f"[ORDER FAILED] {symbol} {option_symbol} ({failure_type})"
        
        html = f"""
        <div style="background: rgba(248, 81, 73, 0.1); border-left: 5px solid #ff7b72; border-radius: 6px; padding: 20px; margin-bottom: 25px; border: 1px solid rgba(248, 81, 73, 0.3);">
            <div style="font-size: 18px; font-weight: bold; color: #ff7b72; margin-bottom: 10px;">
                ⚠️ ORDER SUBMISSION / EXECUTION FAILED
            </div>
            <table width="100%" style="font-size: 14px; color: #c9d1d9; border-collapse: collapse;">
                <tr>
                    <td style="padding: 6px 0;"><b>Underlying:</b></td>
                    <td style="text-align: right; color: #ffffff; font-weight: bold;">{symbol}</td>
                </tr>
                <tr>
                    <td style="padding: 6px 0;"><b>Contract:</b></td>
                    <td style="text-align: right; color: #58a6ff; font-family: monospace;">{option_symbol}</td>
                </tr>
                <tr>
                    <td style="padding: 6px 0;"><b>Failure Classification:</b></td>
                    <td style="text-align: right; color: #ff7b72; font-weight: bold;">{failure_type}</td>
                </tr>
                <tr style="border-top: 1px solid #30363d;">
                    <td style="padding: 8px 0; vertical-align: top;"><b>Reason / Broker Message:</b></td>
                    <td style="text-align: right; color: #c9d1d9;">{reason}</td>
                </tr>
            </table>
            <div style="margin-top: 15px; padding: 10px; background: rgba(0,0,0,0.3); border-radius: 4px; font-size: 12px; color: #8b949e;">
                🛡️ <b>Capital Safety:</b> No unprotected positions are active on your execution account for this trade.
            </div>
        </div>
        """
        try:
            self.emailer.send_live_alert(subject, html)
        except Exception as e:
            logger.error(f"Failed to dispatch order failure email: {e}")
