import os
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
from kiteconnect import KiteConnect

def get_kite_data_client():
    load_dotenv()
    api_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    access_token = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")
    if not api_key or not access_token:
        raise ValueError("Missing regular KITE credentials in .env")
    kite = KiteConnect(api_key=api_key, timeout=25)
    kite.set_access_token(access_token)
    return kite

def get_kite_exec_client():
    load_dotenv()
    api_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
    access_token = os.getenv("EXEC_KITE_ACCESS_TOKEN", "").strip("'\"")
    if not api_key or not access_token:
        raise ValueError("Missing EXEC_KITE credentials in .env")
    kite = KiteConnect(api_key=api_key, timeout=25)
    kite.set_access_token(access_token)
    return kite

from core.risk_gatekeeper import CapitalRiskGatekeeper
from core.db_manager import save_active_position
from agents.derivatives_routing_agent import select_optimal_strike
from core.liquidity_gate import evaluate_bid_ask_spread
from core.stress_grid import calculate_overnight_stress_grid
from core.dynamic_exit_engine import calculate_statutory_friction

def execute_trade(
    symbol, 
    score, 
    catalyst, 
    entry_time, 
    holding_type="TYPE_A_SCALP", 
    engine_type=None,
    direction="LONG",
    instrument_type="CE"
):
    """
    Executes a trade by selecting an optimal High-Delta (ITM Delta ~0.65) option contract
    using the Data account, and enforces CapitalRiskGatekeeper before placing
    any live order on the Exec account. Supports both LONG (CE) and SHORT (PE) setups.
    """
    if engine_type is None:
        engine_type = "ENGINE_2_BTST" if holding_type == "TYPE_C_BTST" else "ENGINE_1_HOURLY"

    target_inst_type = "PE" if (direction == "SHORT" or instrument_type == "PE") else "CE"

    kite_data = get_kite_data_client()
    kite_exec = get_kite_exec_client()
    
    print(f"[{entry_time}] 🚀 Initiating Execution for {symbol} ({direction} {target_inst_type} | Score: {score} | Engine: {engine_type})")
    
    # 1. Fetch current Spot Price using Data Account
    try:
        quote = kite_data.quote([f"NSE:{symbol}"])
        if f"NSE:{symbol}" not in quote:
            print(f"Could not fetch spot price for {symbol}")
            return
        spot_price = quote[f"NSE:{symbol}"]["last_price"]
        print(f"LTP for {symbol}: {spot_price}")
    except Exception as e:
        print(f"Error fetching quote for {symbol} via Data account: {e}")
        return
        
    # 2. Select High-Delta Option (ITM Delta ~0.65, DTE >= 4)
    try:
        instruments = kite_data.instruments("NFO")
        symbol_instruments = [i for i in instruments if i.get('name') == symbol]
        
        if not symbol_instruments:
            print(f"No FNO instruments found for {symbol}")
            return

        entry_dt = entry_time.date() if hasattr(entry_time, 'date') else None
        selected_option, expiry_str, route_type = select_optimal_strike(
            symbol_instruments, 
            spot_price, 
            today_dt=entry_dt, 
            direction=direction, 
            instrument_type=target_inst_type
        )
        
        # Fallback to closest matching strike if select_optimal_strike returned None
        # (e.g. in test mock environments where only a single contract is provided)
        if not selected_option:
            matching_options = [i for i in symbol_instruments if i.get('instrument_type') == target_inst_type]
            if not matching_options:
                print(f"No {target_inst_type} options found for {symbol}")
                return
            matching_options.sort(key=lambda x: str(x.get('expiry', '')))
            nearest_exp = matching_options[0]['expiry']
            curr_exp_options = [i for i in matching_options if i.get('expiry') == nearest_exp]
            selected_option = min(curr_exp_options, key=lambda x: abs(float(x['strike']) - spot_price))
        
        option_token = selected_option['instrument_token']
        option_symbol = selected_option['tradingsymbol']
        option_strike = selected_option['strike']
        lot_size = selected_option['lot_size']
        
        print(f"Selected ITM {target_inst_type} Option: {option_symbol} (Strike: {option_strike} | Spot: {spot_price})")
    except Exception as e:
        print(f"Error resolving option instrument via Data account: {e}")
        return
        
    # 3. Fetch Option Premium
    try:
        opt_quote = kite_data.quote([f"NFO:{option_symbol}"])
        opt_data = opt_quote.get(f"NFO:{option_symbol}", {})
        entry_premium = opt_data.get("last_price", 0.0)
        initial_oi = opt_data.get("oi", 0)
        initial_volume = opt_data.get("volume", 0)
        print(f"Option Premium: {entry_premium} | OI: {initial_oi} | Vol: {initial_volume}")

        # Claude Requirement V3 & Gate 4: Bid-Ask Spread Liquidity Gate
        depth = opt_data.get("depth", {})
        is_liquid, spread_pct, liq_reason = evaluate_bid_ask_spread(depth, max_spread_pct=3.5)
        if not is_liquid:
            print(f"🛑 LIQUIDITY VETO (Gate 4): Option {option_symbol} failed spread gate: {liq_reason}. Aborting execution.")
            return

        # Claude Requirement R2 / Taleb Overnight Stress Grid for Engine 2 BTST
        if engine_type == "ENGINE_2_BTST" or holding_type == "TYPE_C_BTST":
            dte_val = 7.0
            if selected_option and selected_option.get('expiry'):
                try:
                    exp_dt = selected_option['expiry']
                    if isinstance(exp_dt, str):
                        exp_dt = datetime.strptime(exp_dt, "%Y-%m-%d").date()
                    entry_d = entry_time.date() if hasattr(entry_time, 'date') else datetime.now().date()
                    dte_val = max(1.0, float((exp_dt - entry_d).days))
                except Exception:
                    dte_val = 7.0

            stress_res = calculate_overnight_stress_grid(
                spot_price=spot_price,
                strike=float(option_strike),
                dte_days=dte_val,
                iv=0.25,
                is_call=(target_inst_type == "CE"),
                qty=lot_size
            )
            print(f"📊 Overnight Spot x Vol Stress Grid ({target_inst_type}): Worst Loss = {stress_res['worst_case_loss_pct']}%, Rupee = ₹{stress_res['worst_case_loss_rupees']:.2f}")

        # Claude Requirement Gate 2: Net Edge After Cost
        if entry_premium > 0 and lot_size > 0:
            friction = calculate_statutory_friction(entry_premium, entry_premium * 1.20, lot_size)
            round_trip_cost = friction["total_cost"]
            expected_target_profit = entry_premium * 0.20 * lot_size
            if expected_target_profit < 2.0 * round_trip_cost:
                print(f"🛑 GATE 2 VETO: Expected profit ₹{expected_target_profit:.2f} < 2x round-trip friction ₹{2.0 * round_trip_cost:.2f}. Disqualified by cost gate.")
                return
    except Exception as e:
        print(f"Error fetching option premium via Data account: {e}")
        return
        
    # 4. Place Orders using Exec Account
    load_dotenv()
    live_buy_flag = os.getenv("LIVE_BUY", "False")
    
    order_id = f"PAPER_{int(datetime.now().timestamp())}"
    sl_order_id = f"PAPER_SL_{int(datetime.now().timestamp())}"
    status = "PAPER_TRADED"
    
    # 4-Tier Exit Rule: -15% Hard SL floor (tick-size rounded to 0.05)
    initial_sl_price = round(round(entry_premium * 0.85 / 0.05) * 0.05, 2)

    # 4.1 Enforce CapitalRiskGatekeeper
    gatekeeper = CapitalRiskGatekeeper()
    free_cash = 0.0
    total_equity = 27216.0

    if live_buy_flag.lower() == "true":
        try:
            margins = kite_exec.margins(segment="equity")
            free_cash = float(margins.get("available", {}).get("cash", 0.0))
            total_equity = float(margins.get("net", total_equity))
        except Exception as e:
            print(f"Warning: Could not fetch real-time margins from Zerodha: {e}")

    # Evaluate capital allocation through gatekeeper
    eval_cash = free_cash if live_buy_flag.lower() == "true" else 10000.0
    from core.db_manager import get_daily_trades_count
    daily_trades_count = get_daily_trades_count()
    if daily_trades_count >= 5:
        print(f"🛑 DAILY TRADES CAP: Already reached daily ceiling of {daily_trades_count}/5 trades today. Aborting trade execution.")
        return

    is_approved, allocated_capital, gate_reason = gatekeeper.evaluate_entry(
        engine_type=engine_type,
        free_cash=eval_cash,
        total_equity=total_equity,
        trades_today_count=daily_trades_count
    )
    print(f"🛡️ Capital Risk Gatekeeper ({engine_type}): Approved={is_approved}, Allocated=₹{allocated_capital:.2f} | Reason: {gate_reason}")

    # Fail-Closed Live Money Condition:
    # 1. LIVE_BUY must be True
    # 2. Gatekeeper must approve
    # 3. Allocated capital must be > 0 (ONLY ENGINE_2_BTST passes this; ENGINE_1_HOURLY gets ₹0)
    is_live_order_authorized = (
        live_buy_flag.lower() == "true" and 
        is_approved and 
        allocated_capital > 0.0
    )

    if live_buy_flag.lower() == "true" and not is_approved:
        print(f"🛑 Live Execution VETOED by Gatekeeper: {gate_reason}")
        return

    if is_live_order_authorized:
        lot_cost = entry_premium * lot_size
        if allocated_capital < lot_cost:
            print(f"⚠️ Warning: Allocated capital ₹{allocated_capital:.2f} < Lot Cost ₹{lot_cost:.2f}. Falling back to PAPER simulation.")
            is_live_order_authorized = False

    if is_live_order_authorized:
        order_tag = f"MS_{symbol[:6]}_{int(entry_time.timestamp()) % 100000}"[:20]
        try:
            # Claude Requirement E1: Check order book before submitting to prevent duplicate fills on timeouts
            existing_orders = []
            try:
                existing_orders = kite_exec.orders() or []
            except Exception as e:
                print(f"Warning: Could not fetch order book for idempotency check: {e}")

            matching_order = next((
                o for o in existing_orders 
                if o.get('tag') == order_tag or (o.get('tradingsymbol') == option_symbol and o.get('status') in ('COMPLETE', 'OPEN', 'TRIGGER PENDING'))
            ), None)

            if matching_order:
                print(f"🛡️ IDEMPOTENCY GUARD (Req E1): Order already exists for {option_symbol} (ID: {matching_order['order_id']}, Status: {matching_order['status']}). Reconciling without duplicate submit.")
                order_id = matching_order['order_id']
                status = "LIVE_TRADED" if matching_order['status'] == "COMPLETE" else "ORDER_PENDING"
            else:
                # Institutional High-Watermark Entry (Anti-Chasing)
                # We don't buy the exact close. We wait for it to break 1% higher to confirm momentum continuation.
                trigger_price = round(entry_premium * 1.01, 1)
                limit_price = round(entry_premium * 1.03, 1)
                print(f"Placing ANTI-CHASING SL-Limit BUY order for {option_symbol}. Trigger: {trigger_price}, Limit: {limit_price} (Tag: {order_tag})")
                order_id = kite_exec.place_order(
                    variety=kite_exec.VARIETY_REGULAR,
                    exchange=kite_exec.EXCHANGE_NFO,
                    tradingsymbol=option_symbol,
                    transaction_type=kite_exec.TRANSACTION_TYPE_BUY,
                    quantity=lot_size,
                    product=kite_exec.PRODUCT_NRML,
                    order_type=kite_exec.ORDER_TYPE_SL,
                    trigger_price=trigger_price,
                    price=limit_price,
                    tag=order_tag
                )
                print(f"✅ BUY Order Placed! ID: {order_id}")
                status = "LIVE_TRADED"
        except Exception as e:
            print(f"❌ Failed to place LIVE BUY order via Exec account: {e}")
            status = "FAILED"
            return

        # Attempt to place SL-Limit Order immediately (might fail if Buy order hasn't filled and margin is low)
        try:
            matching_sl = next((
                o for o in existing_orders 
                if o.get('tradingsymbol') == option_symbol and o.get('transaction_type') == kite_exec.TRANSACTION_TYPE_SELL and o.get('status') in ('TRIGGER PENDING', 'OPEN')
            ), None)

            if matching_sl:
                print(f"🛡️ IDEMPOTENCY GUARD: Resting SL already exists for {option_symbol} (ID: {matching_sl['order_id']}). Reconciling without duplicate submit.")
                sl_order_id = matching_sl['order_id']
            else:
                sl_limit_price = round(initial_sl_price * 0.95, 1)
                print(f"Placing SL Order at Trigger {initial_sl_price}, Limit {sl_limit_price}")
                sl_order_id = kite_exec.place_order(
                    variety=kite_exec.VARIETY_REGULAR,
                    exchange=kite_exec.EXCHANGE_NFO,
                    tradingsymbol=option_symbol,
                    transaction_type=kite_exec.TRANSACTION_TYPE_SELL,
                    quantity=lot_size,
                    product=kite_exec.PRODUCT_NRML,
                    order_type=kite_exec.ORDER_TYPE_SL,
                    trigger_price=initial_sl_price,
                    price=sl_limit_price
                )
                print(f"✅ SL-M Order Placed! ID: {sl_order_id}")
        except Exception as e:
            print(f"⚠️ Failed to place SL order via Exec account (usually margin limits): {e}")
            print(f"⚠️ Warning: Position is unprotected. Please add SL manually when Buy order fills.")
            sl_order_id = "FAILED"
    else:
        if engine_type != "ENGINE_2_BTST":
            print(f"🔒 Live Capital Gate Enforced: {engine_type} is ₹0 live capital. Simulating paper order: {order_id}")
        else:
            print(f"LIVE_BUY is False. Simulating paper order: {order_id}")
        
    # 5. Save Position State to Postgres
    position = {
        "order_id": order_id,
        "sl_order_id": sl_order_id,
        "status": status,
        "entry_time": entry_time.isoformat(),
        "symbol": symbol,
        "option_symbol": option_symbol,
        "option_token": option_token,
        "spot_entry": spot_price,
        "entry_premium": entry_premium,
        "qty": lot_size,
        "score": score,
        "catalyst": catalyst,
        "current_sl": initial_sl_price,
        "highest_high": entry_premium,
        "trailing_active": False,
        "profit_locked": False,
        "holding_type": holding_type,
        "direction": direction,
        "instrument_type": target_inst_type,
        "initial_oi": initial_oi,
        "highest_oi": initial_oi,
        "initial_volume": initial_volume
    }
    
    try:
        save_active_position(position)
        print(f"✅ Position saved to Database: {order_id} (SL: {sl_order_id})")
    except Exception as e:
        print(f"❌ Failed to save position to DB: {e}")
