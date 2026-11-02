import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from kiteconnect import KiteConnect

from core.db_manager import get_all_active_positions, get_active_daily_focus_watchlist
from core.position_guardian import PositionGuardian, CandleMetrics, TrapType, HoldAction
from alerts.email_notifier import SovereignEmailer

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_hourly_guardian_audit(send_mail: bool = True):
    """
    Executes the Hourly Live Position Guardian Audit:
    - Scans active open trades and the Daily Focus Watchlist.
    - Evaluates institutional trap risk (climax wicks, absorption, volume exhaustion).
    - Computes Multi-Day Hold Conviction Score (0-100).
    - Gives clear directive: HOLD_MULTI_DAY (BTST) vs HOLD_INTRADAY vs EXIT_OR_LOCK_PROFIT.
    - Sends an email advisory to the user every hour.
    """
    now = datetime.now()
    logger.info(f"🛡️ Running Position Guardian & Anti-Trap Audit at {now.strftime('%H:%M:%S IST')}...")

    api_key = os.getenv("KITE_API_KEY")
    access_token = os.getenv("KITE_ACCESS_TOKEN")
    if not api_key or not access_token:
        logger.error("Missing Zerodha Kite credentials in .env")
        return

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    guardian = PositionGuardian()

    # 1. Fetch active open positions
    open_positions = get_all_active_positions()
    focus_items = get_active_daily_focus_watchlist()
    
    # We want to audit all open trades, plus top focus stocks
    audit_symbols = {}
    for pos in open_positions:
        sym = pos.get("symbol")
        if sym:
            audit_symbols[sym] = {
                "direction": pos.get("direction", "LONG"),
                "entry_price": float(pos.get("entry_price", 0.0)),
                "is_active_trade": True,
                "engine": pos.get("engine_type", "UNKNOWN"),
                "instrument": pos.get("instrument_token", sym)
            }

    for item in focus_items:
        sym = item.get("symbol")
        if sym and sym not in audit_symbols:
            audit_symbols[sym] = {
                "direction": item.get("direction", "LONG"),
                "entry_price": 0.0,
                "is_active_trade": False,
                "engine": "FOCUS_LIST",
                "instrument": sym
            }

    if not audit_symbols:
        logger.info("No active positions or focus symbols to evaluate.")
        return

    # 2. Fetch quotes
    quote_keys = [f"NSE:{s}" for s in audit_symbols.keys()]
    try:
        quotes = kite.quote(quote_keys)
    except Exception as e:
        logger.error(f"Error fetching quotes: {e}")
        quotes = {}

    from_date = (now - timedelta(days=4)).strftime("%Y-%m-%d")
    to_date = now.strftime("%Y-%m-%d")

    assessments = []

    for sym, meta in audit_symbols.items():
        try:
            q_key = f"NSE:{sym}"
            q_info = quotes.get(q_key, {})
            inst_token = q_info.get("instrument_token")
            ltp = float(q_info.get("last_price", 0.0))
            entry_px = meta["entry_price"] if meta["entry_price"] > 0 else ltp

            if not inst_token:
                continue

            # Fetch hourly / 15m candles
            candles = kite.historical_data(inst_token, from_date, to_date, "60minute")
            if len(candles) < 5:
                # fallback to 15m
                candles = kite.historical_data(inst_token, from_date, to_date, "15minute")
                if len(candles) < 5:
                    continue

            last_bar = candles[-1]
            prev_bars = candles[-21:-1] if len(candles) >= 21 else candles[:-1]
            vol_sma = sum(b["volume"] for b in prev_bars) / len(prev_bars) if prev_bars else last_bar["volume"]
            
            # Simple VWAP estimate from current day's bars
            today_bars = [b for b in candles if b["date"].date() == now.date()]
            if today_bars:
                tot_vp = sum(((b["high"] + b["low"] + b["close"]) / 3.0) * b["volume"] for b in today_bars)
                tot_v = sum(b["volume"] for b in today_bars)
                vwap = (tot_vp / tot_v) if tot_v > 0 else ltp
            else:
                vwap = ltp

            c_metric = CandleMetrics(
                symbol=sym,
                open=float(last_bar["open"]),
                high=float(last_bar["high"]),
                low=float(last_bar["low"]),
                close=float(last_bar["close"]),
                volume=int(last_bar["volume"]),
                vol_sma=vol_sma,
                vwap=vwap,
                current_time=now
            )

            res = guardian.evaluate_position(
                symbol=sym,
                direction=meta["direction"],
                entry_price=entry_px,
                current_price=ltp,
                candle=c_metric
            )
            assessments.append((meta, res))
            logger.info(f"   ↳ {sym} ({meta['direction']}): Score {res.conviction_score}/100 | Action: {res.hold_action.value} | Trap: {res.trap_type.value}")

        except Exception as e:
            logger.warning(f"Could not audit {sym}: {e}")

    # 3. Format and dispatch hourly advisory email if active trades or alerts
    if send_mail and assessments:
        subject = f"🛡️ Hourly Live Position Guardian Briefing ({now.strftime('%H:%M IST')})"
        
        rows = ""
        for meta, a in assessments:
            trade_tag = "🔴 ACTIVE TRADE" if meta["is_active_trade"] else "⚪ FOCUS WATCHLIST"
            action_badge = ""
            if a.hold_action == HoldAction.HOLD_MULTI_DAY:
                action_badge = "<b style='color:#16a34a;'>🔥 HOLD MULTI-DAY (BTST)</b>"
            elif a.hold_action == HoldAction.HOLD_INTRADAY:
                action_badge = "<b style='color:#ca8a04;'>⚡ INTRADAY ONLY (Trail SL)</b>"
            else:
                action_badge = "<b style='color:#dc2626;'>🚨 EXIT / LOCK PROFIT (Trap Risk)</b>"

            reasons_txt = "<br>".join([f"• {r}" for r in a.reasons])
            pnl_txt = f"{a.pnl_pct:+.2f}%" if meta["is_active_trade"] else "N/A"

            rows += f"""
            <tr style='border-bottom: 1px solid #e5e7eb;'>
                <td style='padding:10px;'><b>{a.symbol}</b><br><small>{trade_tag}</small></td>
                <td style='padding:10px;'>{a.direction}</td>
                <td style='padding:10px;'>₹{a.current_price:.2f}</td>
                <td style='padding:10px;'><b>{pnl_txt}</b></td>
                <td style='padding:10px; text-align:center;'><b>{a.conviction_score}/100</b></td>
                <td style='padding:10px;'>{action_badge}</td>
                <td style='padding:10px;'><small>{reasons_txt}</small></td>
            </tr>
            """

        html_body = f"""
        <html>
        <body style='font-family: Arial, sans-serif; line-height: 1.5; color: #1f2937; padding: 20px;'>
            <h2 style='color: #0f172a; border-bottom: 2px solid #3b82f6; padding-bottom: 8px;'>
                🛡️ Hourly Position Guardian & Anti-Trap Advisory ({now.strftime('%I:%M %p IST')})
            </h2>
            <p>Real-time institutional volume, exhaustion wick analysis, and Multi-Day BTST Conviction scoring.</p>
            
            <table style='width:100%; border-collapse: collapse; text-align: left; font-size: 14px;'>
                <thead>
                    <tr style='background-color: #f1f5f9; border-bottom: 2px solid #cbd5e1;'>
                        <th style='padding:10px;'>Symbol</th>
                        <th style='padding:10px;'>Side</th>
                        <th style='padding:10px;'>LTP</th>
                        <th style='padding:10px;'>PnL</th>
                        <th style='padding:10px; text-align:center;'>Conviction</th>
                        <th style='padding:10px;'>Hourly Directive</th>
                        <th style='padding:10px;'>Trap & Structure Analysis</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
            <br>
            <div style='background-color: #f8fafc; border-left: 4px solid #3b82f6; padding: 12px; font-size: 13px;'>
                <b>Guardian Holding Framework:</b><br>
                • <b>Score ≥ 80 (HOLD MULTI-DAY)</b>: Volume expansion, no upper/lower wicks, above VWAP. Keep overnight for BTST alpha.<br>
                • <b>Score 50-79 (INTRADAY ONLY)</b>: Move SL to breakeven cost. Book profits by 15:20 PM.<br>
                • <b>Score &lt; 50 (EXIT / LOCK)</b>: Heavy exhaustion wicks or institutional absorption detected. High risk of reversal.
            </div>
        </body>
        </html>
        """
        try:
            emailer = SovereignEmailer()
            emailer.send_live_alert(subject, html_body)
            logger.info("✅ Hourly Position Guardian briefing email dispatched.")
        except Exception as e:
            logger.error(f"Failed to dispatch email: {e}")

if __name__ == "__main__":
    run_hourly_guardian_audit(send_mail=True)
