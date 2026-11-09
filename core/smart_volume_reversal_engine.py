import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dotenv import load_dotenv
from kiteconnect import KiteConnect

from alerts.email_notifier import SovereignEmailer
from core.db_manager import get_all_active_positions, close_position

logger = logging.getLogger(__name__)

class SmartVolumeReversalEngine:
    """
    Smart 20-Minute Volume Reversal Engine (v4.0.0-PROD)
    Monitors active F&O positions every 20 minutes:
    1. Quantitative Volume & VWAP Microstructure Diagnostics.
    2. Deep LLM Arbiter (Gemini 2.5/Flash) to detect institutional volume reversals and traps.
    3. Autonomous Liquidation with protective broker execution and instant alert dispatch.
    """

    def __init__(
        self,
        kite_data: Optional[KiteConnect] = None,
        kite_exec: Optional[KiteConnect] = None,
        emailer: Optional[SovereignEmailer] = None,
        gemini_client: Optional[Any] = None
    ):
        load_dotenv()
        
        # 1. Kite Data Client (for underlying market data quotes and historical bars)
        if kite_data:
            self.kite_data = kite_data
        else:
            api_key = os.getenv("KITE_API_KEY", "").strip("'\"")
            access_token = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")
            self.kite_data = KiteConnect(api_key=api_key, timeout=25) if api_key and access_token else None
            if self.kite_data and access_token:
                self.kite_data.set_access_token(access_token)

        # 2. Kite Exec Client (for live order placement & position queries on HU0092)
        if kite_exec:
            self.kite_exec = kite_exec
        else:
            exec_api_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
            exec_access_token = os.getenv("EXEC_KITE_ACCESS_TOKEN", "").strip("'\"")
            self.kite_exec = KiteConnect(api_key=exec_api_key, timeout=25) if exec_api_key and exec_access_token else None
            if self.kite_exec and exec_access_token:
                self.kite_exec.set_access_token(exec_access_token)

        self.emailer = emailer or SovereignEmailer()

        # 3. Gemini LLM Client
        if gemini_client:
            self.gemini_client = gemini_client
        else:
            try:
                from google import genai
                gemini_api_key = os.getenv("GEMINI_API_KEY", "").strip("'\"")
                self.gemini_client = genai.Client(api_key=gemini_api_key) if gemini_api_key else None
            except Exception as e:
                logger.warning(f"Could not initialize google.genai: {e}")
                self.gemini_client = None

    def get_live_open_positions(self) -> List[Dict[str, Any]]:
        """
        Retrieves active open positions from the broker (HU0092) and reconciles with DB ledger.
        """
        open_positions = []
        try:
            db_positions_map = {p.get("option_symbol"): p for p in get_all_active_positions() if p.get("option_symbol")}
        except Exception:
            db_positions_map = {}

        if self.kite_exec:
            try:
                net_pos = self.kite_exec.positions().get("net", [])
                for pos in net_pos:
                    qty = int(pos.get("quantity", 0))
                    if qty != 0:  # Open position
                        opt_sym = pos.get("tradingsymbol")
                        db_p = db_positions_map.get(opt_sym, {})
                        open_positions.append({
                            "option_symbol": opt_sym,
                            "instrument_token": pos.get("instrument_token"),
                            "quantity": abs(qty),
                            "direction": "LONG" if qty > 0 else "SHORT",
                            "entry_price": float(pos.get("average_price", 0.0)),
                            "ltp": float(pos.get("last_price", 0.0)),
                            "m2m": float(pos.get("m2m", 0.0)),
                            "pnl": float(pos.get("pnl", 0.0)),
                            "entry_time": db_p.get("entry_time"),
                            "order_id": db_p.get("order_id", f"BROKER_{opt_sym}")
                        })
            except Exception as e:
                logger.error(f"Error fetching live broker positions: {e}")

        # If broker positions are empty or unavailable, check active DB positions
        if not open_positions:
            db_positions = get_all_active_positions()
            for p in db_positions:
                open_positions.append({
                    "option_symbol": p.get("option_symbol"),
                    "underlying": p.get("symbol"),
                    "quantity": int(p.get("quantity", 0)),
                    "direction": p.get("direction", "LONG"),
                    "entry_price": float(p.get("entry_price", 0.0)),
                    "ltp": float(p.get("current_price", p.get("entry_price", 0.0))),
                    "stop_loss": float(p.get("stop_loss", 0.0)),
                    "target_price": float(p.get("target_price", 0.0))
                })

        return open_positions

    def parse_underlying_and_type(self, option_symbol: str) -> Tuple[str, str, float]:
        """
        Extracts underlying symbol, option type (CE/PE), and strike from tradingsymbol.
        Example: 'BHARATFORG26NOV1720CE' -> ('BHARATFORG', 'CE', 1720.0)
        """
        import re
        match = re.match(r"^([A-Z\-]+)\d{2}[A-Z]{3}(\d+)(CE|PE)$", option_symbol)
        if match:
            return match.group(1), match.group(3), float(match.group(2))
        return option_symbol[:6], "CE", 0.0

    def calculate_microstructure_metrics(self, candles: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Computes volume, VWAP, and institutional price action metrics across 5m bars.
        """
        if not candles or len(candles) < 3:
            return {
                "volume_ratio": 1.0,
                "is_below_vwap": False,
                "selling_volume_spike": False,
                "volume_drying": False,
                "distribution_candle": False,
                "latest_close": 0.0,
                "vwap": 0.0
            }

        # Calculate Intraday VWAP
        cum_vol = 0
        cum_vol_price = 0.0
        for c in candles:
            v = c.get("volume", 0)
            typical = (c["high"] + c["low"] + c["close"]) / 3.0
            cum_vol += v
            cum_vol_price += typical * v

        vwap = round(cum_vol_price / cum_vol, 2) if cum_vol > 0 else candles[-1]["close"]

        # Volume 20-period SMA
        recent_volumes = [c.get("volume", 0) for c in candles[-20:]]
        vol_sma = sum(recent_volumes) / max(1, len(recent_volumes))

        last_bar = candles[-1]
        prev_bar = candles[-2]
        latest_vol = last_bar.get("volume", 0)
        vol_ratio = round(latest_vol / vol_sma, 2) if vol_sma > 0 else 1.0

        candle_range = last_bar["high"] - last_bar["low"]
        upper_wick = last_bar["high"] - max(last_bar["open"], last_bar["close"])
        upper_wick_ratio = (upper_wick / candle_range) if candle_range > 0 else 0.0

        is_red_candle = last_bar["close"] < last_bar["open"]
        selling_volume_spike = is_red_candle and (vol_ratio >= 1.5)
        volume_drying = vol_ratio < 0.45
        distribution_candle = upper_wick_ratio >= 0.40 and (vol_ratio >= 1.2)
        is_below_vwap = last_bar["close"] < vwap

        return {
            "volume_ratio": vol_ratio,
            "is_below_vwap": is_below_vwap,
            "selling_volume_spike": selling_volume_spike,
            "volume_drying": volume_drying,
            "distribution_candle": distribution_candle,
            "upper_wick_ratio": round(upper_wick_ratio, 2),
            "latest_close": last_bar["close"],
            "vwap": vwap,
            "vol_sma": round(vol_sma, 1),
            "latest_vol": latest_vol
        }

    def consult_llm_volume_reversal(
        self,
        symbol: str,
        option_symbol: str,
        option_type: str,
        entry_price: float,
        current_ltp: float,
        pnl_pct: float,
        metrics: Dict[str, Any],
        recent_candles: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Deep LLM Consultation: Evaluates institutional volume reversal vs normal pullback.
        """
        if not self.gemini_client:
            # Deterministic quantitative fallback when LLM is unavailable
            return self._deterministic_fallback_reversal(metrics, pnl_pct)

        prompt = f"""
You are the Chief Quantitative Risk Arbiter at an elite institutional volatility trading desk.
Evaluate whether an open option position has suffered a critical "Institutional Volume Reversal / Momentum Trap" requiring an IMMEDIATE AUTO-EXIT to preserve capital, or if the price action represents normal consolidation that should be HELD.

POSITION CONTEXT:
- Underlying: {symbol}
- Option Contract: {option_symbol} ({option_type})
- Entry Premium: ₹{entry_price:.2f}
- Current Option LTP: ₹{current_ltp:.2f}
- Unrealized P&L: {pnl_pct:+.2f}%

UNDERLYING MICROSTRUCTURE (Last 20-Minute Closed Bars):
- Current Price: ₹{metrics['latest_close']:.2f}
- Intraday VWAP: ₹{metrics['vwap']:.2f} (Posture: {'BELOW VWAP (BEARISH)' if metrics['is_below_vwap'] else 'ABOVE VWAP (BULLISH)'})
- Latest Bar Volume vs 20-SMA: {metrics['volume_ratio']}x
- Upper Wick Rejection Ratio: {metrics['upper_wick_ratio']*100:.1f}%
- Microstructure Signals:
  * Selling Volume Spike: {metrics['selling_volume_spike']}
  * Volume Drying/Stagnation: {metrics['volume_drying']}
  * Institutional Distribution Wick: {metrics['distribution_candle']}

LAST 3 5-MINUTE CANDLES (OHLCV):
{json.dumps([{ 'time': str(c.get('date', 'N/A')), 'open': c['open'], 'high': c['high'], 'low': c['low'], 'close': c['close'], 'volume': c['volume']} for c in recent_candles[-3:]], indent=2)}

INSTRUCTIONS:
1. If this is a CALL (CE) and the underlying has broken below VWAP on heavy selling volume, or if volume dried up while aggressive sellers absorbed the highs (distribution wick), issue an IMMEDIATE EXIT.
2. If this is a PUT (PE) and the underlying has reclaimed above VWAP with strong buying volume, issue an IMMEDIATE EXIT.
3. If this is an orderly low-volume consolidation or bull flag respecting VWAP, vote HOLD.

Respond with strict JSON ONLY using this exact schema:
{{
  "decision": "EXIT" or "HOLD",
  "confidence": <integer from 0 to 100>,
  "volume_reversal_confirmed": <true or false>,
  "reversal_mechanism": "HEAVY_DISTRIBUTION_INTO_RESISTANCE" or "VOLUME_EXHAUSTION_STAGNATION" or "VWAP_BREAKDOWN" or "NORMAL_PULLBACK_CONSOLIDATION",
  "concise_rationale": "<1-2 sentence high-conviction institutional reasoning>"
}}
"""
        from google.genai import types
        models_to_try = ["gemini-3.5-flash-lite", "gemini-flash-latest"]
        for m in models_to_try:
            try:
                response = self.gemini_client.models.generate_content(
                    model=m,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.0,
                        response_mime_type="application/json"
                    )
                )
                data = json.loads(response.text.strip())
                logger.info(f"🧠 [LLM ARBITER ({m})] {option_symbol} -> Verdict: {data.get('decision')} (Conf: {data.get('confidence')}%) | {data.get('concise_rationale')}")
                return data
            except Exception as e:
                logger.warning(f"Model {m} failed for volume reversal consultation: {e}")

        logger.error("All Gemini models failed for volume reversal consultation. Using quantitative fallback.")
        return self._deterministic_fallback_reversal(metrics, pnl_pct)

    def _deterministic_fallback_reversal(self, metrics: Dict[str, Any], pnl_pct: float) -> Dict[str, Any]:
        """
        Deterministic quantitative fallback when LLM API is unavailable.
        """
        # Hard quantitative rules for volume reversal:
        # 1. Price broke below VWAP + Selling Volume Spike (>1.5x)
        # 2. Institutional Distribution wick (>40%) on expanding volume
        # 3. Severe loss (-14% or more)
        if (metrics.get("is_below_vwap") and metrics.get("selling_volume_spike")) or pnl_pct <= -14.0:
            return {
                "decision": "EXIT",
                "confidence": 85,
                "volume_reversal_confirmed": True,
                "reversal_mechanism": "VWAP_BREAKDOWN",
                "concise_rationale": "Underlying broke below VWAP on heavy volume spike; immediate capital protection exit triggered."
            }
        elif metrics.get("distribution_candle") and metrics.get("volume_ratio", 1.0) >= 1.5:
            return {
                "decision": "EXIT",
                "confidence": 78,
                "volume_reversal_confirmed": True,
                "reversal_mechanism": "HEAVY_DISTRIBUTION_INTO_RESISTANCE",
                "concise_rationale": "High upper wick rejection on above-average volume confirms institutional supply overhead."
            }
        else:
            return {
                "decision": "HOLD",
                "confidence": 75,
                "volume_reversal_confirmed": False,
                "reversal_mechanism": "NORMAL_PULLBACK_CONSOLIDATION",
                "concise_rationale": "Microstructure remains within normal consolidation tolerance; volume remains non-threatening."
            }

    def execute_smart_exit(
        self,
        position: Dict[str, Any],
        exit_price: float,
        reason: str,
        mechanism: str
    ) -> bool:
        """
        Executes autonomous liquidation on HU0092 and notifies via email.
        """
        option_symbol = position.get("option_symbol")
        qty = int(position.get("quantity", 0))
        entry_price = float(position.get("entry_price", 0.0))
        pnl = round((exit_price - entry_price) * qty, 2)
        pnl_pct = round(((exit_price - entry_price) / entry_price * 100.0), 2) if entry_price > 0 else 0.0

        logger.info(f"🚨 [AUTO-EXIT INITIATED]: Closing {qty} qty of {option_symbol} @ ~₹{exit_price:.2f} (P&L: ₹{pnl:+,.2f})")

        # 1. Cancel resting SL orders on Zerodha if any
        if self.kite_exec:
            try:
                orders = self.kite_exec.orders() or []
                for o in orders:
                    if o.get("tradingsymbol") == option_symbol and o.get("status") in ("OPEN", "TRIGGER PENDING"):
                        logger.info(f"Cancelling resting SL order {o['order_id']} before liquidation...")
                        self.kite_exec.cancel_order(variety=self.kite_exec.VARIETY_REGULAR, order_id=o["order_id"])
            except Exception as e:
                logger.warning(f"Error clearing resting orders: {e}")

            # 2. Place Market / Marketable Limit Exit Order
            try:
                exit_order_id = self.kite_exec.place_order(
                    variety=self.kite_exec.VARIETY_REGULAR,
                    exchange=self.kite_exec.EXCHANGE_NFO,
                    tradingsymbol=option_symbol,
                    transaction_type=self.kite_exec.TRANSACTION_TYPE_SELL,
                    quantity=qty,
                    product=self.kite_exec.PRODUCT_NRML,
                    order_type=self.kite_exec.ORDER_TYPE_MARKET
                )
                logger.info(f"✅ Market Exit Order Placed: {exit_order_id}")
            except Exception as e:
                logger.error(f"❌ Failed to submit market exit to broker: {e}")
                # Fallback to limit order at 95% of LTP
                try:
                    limit_exit = round(round(exit_price * 0.95 / 0.05) * 0.05, 2)
                    exit_order_id = self.kite_exec.place_order(
                        variety=self.kite_exec.VARIETY_REGULAR,
                        exchange=self.kite_exec.EXCHANGE_NFO,
                        tradingsymbol=option_symbol,
                        transaction_type=self.kite_exec.TRANSACTION_TYPE_SELL,
                        quantity=qty,
                        product=self.kite_exec.PRODUCT_NRML,
                        order_type=self.kite_exec.ORDER_TYPE_LIMIT,
                        price=limit_exit
                    )
                    logger.info(f"✅ Fallback Limit Exit Placed: {exit_order_id} @ ₹{limit_exit}")
                except Exception as e2:
                    logger.critical(f"🛑 CRITICAL: Exit failed on both MARKET and LIMIT: {e2}")
                    return False

        # 3. Update database position
        try:
            close_position(
                position=position,
                exit_price=exit_price,
                pnl_pct=pnl_pct,
                status_override="CLOSED_VOLUME_REVERSAL"
            )
        except Exception as e:
            logger.warning(f"Could not update position in DB: {e}")

        # 4. Dispatch Email Alert
        self._notify_auto_exit(
            option_symbol=option_symbol,
            qty=qty,
            entry_price=entry_price,
            exit_price=exit_price,
            pnl=pnl,
            pnl_pct=pnl_pct,
            mechanism=mechanism,
            reason=reason
        )

        return True

    def _notify_auto_exit(
        self,
        option_symbol: str,
        qty: int,
        entry_price: float,
        exit_price: float,
        pnl: float,
        pnl_pct: float,
        mechanism: str,
        reason: str
    ):
        pnl_color = "#3fb950" if pnl >= 0 else "#ff7b72"
        subject = f"[ORDER EXECUTED] [AUTO-EXIT] {option_symbol} Volume Reversal Exit @ ₹{exit_price:.2f} ({pnl_pct:+.1f}%)"
        
        html = f"""
        <div style="background: rgba(248, 81, 73, 0.08); border-left: 5px solid {pnl_color}; border-radius: 6px; padding: 20px; margin-bottom: 25px; border: 1px solid #30363d;">
            <div style="font-size: 18px; font-weight: bold; color: #ffffff; margin-bottom: 10px;">
                🚨 20-MINUTE REVERSAL ENGINE: POSITION CLOSED
            </div>
            <p style="margin: 0 0 15px 0; color: #c9d1d9; font-size: 14px;">
                The 20-Minute Volume Reversal Engine detected that buying momentum has reversed. Position liquidated autonomously to protect capital.
            </p>
            <table width="100%" style="font-size: 14px; color: #c9d1d9; border-collapse: collapse;">
                <tr>
                    <td style="padding: 6px 0;"><b>Contract:</b></td>
                    <td style="text-align: right; color: #58a6ff; font-family: monospace;">{option_symbol}</td>
                </tr>
                <tr>
                    <td style="padding: 6px 0;"><b>Quantity:</b></td>
                    <td style="text-align: right; color: #ffffff;">{qty} shares</td>
                </tr>
                <tr>
                    <td style="padding: 6px 0;"><b>Entry Premium:</b></td>
                    <td style="text-align: right; color: #ffffff;">₹{entry_price:.2f}</td>
                </tr>
                <tr>
                    <td style="padding: 6px 0;"><b>Exit Price:</b></td>
                    <td style="text-align: right; color: #ffffff; font-weight: bold;">₹{exit_price:.2f}</td>
                </tr>
                <tr style="border-top: 1px solid #30363d;">
                    <td style="padding: 8px 0; color: {pnl_color}; font-weight: bold;">Realized P&L:</td>
                    <td style="text-align: right; color: {pnl_color}; font-weight: bold; font-size: 16px;">₹{pnl:+,.2f} ({pnl_pct:+.2f}%)</td>
                </tr>
                <tr style="border-top: 1px solid #30363d;">
                    <td style="padding: 6px 0; color: #8b949e;"><b>Reversal Mechanism:</b></td>
                    <td style="text-align: right; color: #e1e8ed;">{mechanism}</td>
                </tr>
                <tr>
                    <td style="padding: 6px 0; color: #8b949e; vertical-align: top;"><b>AI Arbiter Rationale:</b></td>
                    <td style="text-align: right; color: #8b949e; font-size: 13px;">{reason}</td>
                </tr>
            </table>
        </div>
        """
        try:
            self.emailer.send_live_alert(subject, html)
        except Exception as e:
            logger.error(f"Failed to send auto-exit notification email: {e}")

    def run_20m_cycle(self, now: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Executes the 20-minute cycle across all open F&O positions.
        """
        current_time = now or datetime.now()
        logger.info(f"🔄 Running 20-Minute Volume Reversal Cycle at {current_time.strftime('%H:%M:%S IST')}...")

        open_positions = self.get_live_open_positions()
        if not open_positions:
            logger.info("Zero active F&O positions on desk. Cycle complete (0 overhead).")
            return []

        logger.info(f"Found {len(open_positions)} active F&O positions to evaluate: {[p.get('option_symbol') for p in open_positions]}")

        results = []
        for pos in open_positions:
            option_symbol = pos.get("option_symbol")
            underlying, opt_type, strike = self.parse_underlying_and_type(option_symbol)
            entry_price = float(pos.get("entry_price", 0.0))

            # Fetch live quote for Option
            current_ltp = float(pos.get("ltp", 0.0))
            if self.kite_data and current_ltp <= 0.0:
                try:
                    q = self.kite_data.quote([f"NFO:{option_symbol}"])
                    current_ltp = float(q.get(f"NFO:{option_symbol}", {}).get("last_price", entry_price))
                except Exception as e:
                    logger.warning(f"Could not fetch live quote for {option_symbol}: {e}")
                    current_ltp = entry_price

            pnl_pct = round(((current_ltp - entry_price) / entry_price * 100.0), 2) if entry_price > 0 else 0.0

            # 15-Minute Gestation Window: Do not liquidate newly opened positions on initial noise
            entry_time = pos.get("entry_time")
            if entry_time:
                age_minutes = (current_time - entry_time).total_seconds() / 60.0
                if age_minutes < 15.0 and pnl_pct > -14.0:
                    logger.info(f"⏳ {option_symbol} is in early gestation window (held {age_minutes:.1f}m < 15m, PnL: {pnl_pct:+.1f}%). Bypassing volume reversal.")
                    results.append({
                        "option_symbol": option_symbol,
                        "underlying": underlying,
                        "current_ltp": current_ltp,
                        "pnl_pct": pnl_pct,
                        "decision": "HOLD",
                        "confidence": 90,
                        "mechanism": "GESTATION_HOLD",
                        "action_taken": "HOLD"
                    })
                    continue

            # Fetch 5-minute historical candles for the underlying
            candles = []
            if self.kite_data:
                try:
                    # Look back 3 days to guarantee sufficient intraday bars
                    from_d = (current_time - timedelta(days=3)).strftime("%Y-%m-%d")
                    to_d = current_time.strftime("%Y-%m-%d")
                    inst_token = None
                    
                    # Try finding token via quote or instrument list
                    quote = self.kite_data.quote([f"NSE:{underlying}"])
                    if f"NSE:{underlying}" in quote:
                        inst_token = quote[f"NSE:{underlying}"]["instrument_token"]
                    
                    if inst_token:
                        candles = self.kite_data.historical_data(
                            instrument_token=inst_token,
                            from_date=from_d,
                            to_date=to_d,
                            interval="5minute"
                        )
                except Exception as e:
                    logger.warning(f"Could not fetch historical data for {underlying}: {e}")

            metrics = self.calculate_microstructure_metrics(candles)

            # Consult LLM Arbiter
            verdict_data = self.consult_llm_volume_reversal(
                symbol=underlying,
                option_symbol=option_symbol,
                option_type=opt_type,
                entry_price=entry_price,
                current_ltp=current_ltp,
                pnl_pct=pnl_pct,
                metrics=metrics,
                recent_candles=candles
            )

            decision = verdict_data.get("decision", "HOLD")
            confidence = int(verdict_data.get("confidence", 50))
            mechanism = verdict_data.get("reversal_mechanism", "NONE")
            rationale = verdict_data.get("concise_rationale", "")

            action_taken = "HOLD"
            # Hard stop or high-confidence reversal exit
            if decision == "EXIT" and confidence >= 70:
                success = self.execute_smart_exit(
                    position=pos,
                    exit_price=current_ltp,
                    reason=rationale,
                    mechanism=mechanism
                )
                action_taken = "AUTO_EXITED" if success else "EXIT_FAILED"

            results.append({
                "option_symbol": option_symbol,
                "underlying": underlying,
                "current_ltp": current_ltp,
                "pnl_pct": pnl_pct,
                "decision": decision,
                "confidence": confidence,
                "mechanism": mechanism,
                "action_taken": action_taken
            })

        return results
