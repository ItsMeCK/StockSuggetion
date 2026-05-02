import os
import json
import logging
from datetime import datetime, timedelta
from pipeline.screener import SovereignScreener
from core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_trading_dates(start_date, end_date):
    import psycopg2
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )
    cur = conn.cursor()
    query = """
        SELECT DISTINCT time::date 
        FROM daily_ohlcv 
        WHERE time >= %s AND time <= %s
        ORDER BY time ASC
    """
    cur.execute(query, (start_date, end_date))
    dates = [row[0].strftime('%Y-%m-%d') for row in cur.fetchall()]
    cur.close()
    conn.close()
    return dates

def track_performance(symbol, entry_date, entry_price):
    import psycopg2
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )
    cur = conn.cursor()
    query = "SELECT time, close FROM daily_ohlcv WHERE symbol = %s AND time > %s ORDER BY time ASC"
    cur.execute(query, (symbol, entry_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    # Elite Sovereign Parameters
    target = entry_price * 1.24 # 24% Profit Target
    base_stop = entry_price * 0.92 # 8% Stop Loss
    current_stop = base_stop
    breakeven_triggered = False
    
    for row in rows:
        price = float(row[1])
        if not breakeven_triggered and price >= (entry_price * 1.15):
            current_stop = entry_price
            breakeven_triggered = True
            
        if price >= target:
            return "PROFIT", 24.0, row[0].strftime('%Y-%m-%d'), price
        if price <= current_stop:
            pnl = 0.0 if breakeven_triggered else -8.0
            return "LOSS", pnl, row[0].strftime('%Y-%m-%d'), price
            
    if rows:
        last_price = float(rows[-1][1])
        floating_pnl = ((last_price - entry_price) / entry_price) * 100
        return "OPEN", round(floating_pnl, 2), "N/A", last_price
    return "OPEN", 0.0, "N/A", entry_price

def run_engine_for_day(target_date, filtered_candidates):
    initial_state = SovereignState(
        target_date=target_date,
        macro_regime="",
        candidates=filtered_candidates,
        approved_allocations={},
        agent_scores={},
        error_log=[]
    )
    
    screener = SovereignScreener()
    _, _, base_scores, macro_regime = screener.run_pipeline(target_date=target_date)
    initial_state["macro_regime"] = macro_regime["regime"]
    initial_state["base_scores"] = base_scores
    
    if not filtered_candidates:
        return {
            "approved": [],
            "macro_regime": macro_regime["regime"],
            "agent_scores": {}
        }
        
    from langgraph.checkpoint.postgres import PostgresSaver
    from graph.builder import build_sovereign_graph_with_checkpointer
    db_uri = f"postgresql://agent:agentpassword@{os.getenv('DB_HOST', 'localhost')}:5433/sovereign_state"
    
    import time
    with PostgresSaver.from_conn_string(db_uri) as checkpointer:
        checkpointer.setup()
        app = build_sovereign_graph_with_checkpointer(checkpointer)
        thread_id = f"elite_{target_date.replace('-', '')}_{int(time.time())}"
        final_state = app.invoke(initial_state, config={"configurable": {"thread_id": thread_id}})
        
    return {
        "approved": list(final_state.get('approved_allocations', {}).keys()),
        "macro_regime": final_state.get("macro_regime", "UNKNOWN"),
        "agent_scores": final_state.get("agent_scores", {}),
        "incubator": final_state.get("incubator", [])
    }

def run_backtest():
    start_date = datetime(2026, 2, 1)
    end_date = "2026-05-01"
    trading_dates = get_trading_dates(start_date, end_date)
    
    results = []
    cool_off_tracker = {} # symbol -> last_loss_date
    
    screener = SovereignScreener()
    
    for date in trading_dates:
        logging.info(f"ELITE RUN: {date}")
        
        # 1. Get Screener Candidates
        candidates, incubator, _, _ = screener.run_pipeline(target_date=date)
        all_candidates = candidates + incubator
        
        # 2. Apply Cool-Off
        today_obj = datetime.strptime(date, "%Y-%m-%d")
        available_candidates = []
        for s in all_candidates:
            if s in cool_off_tracker:
                last_loss = datetime.strptime(cool_off_tracker[s], "%Y-%m-%d")
                if (today_obj - last_loss).days < 15:
                    logging.warning(f"COOL-OFF: Skipping {s}")
                    continue
            available_candidates.append(s)
            
        # 3. Run Cognitive Gate
        run_data = run_engine_for_day(date, available_candidates)
        
        trades = []
        for symbol in run_data["approved"]:
            # Fetch Entry Price
            import psycopg2
            conn = psycopg2.connect(host=os.getenv("DB_HOST", "localhost"), port=os.getenv("DB_PORT", "5432"), user=os.getenv("POSTGRES_USER", "quant"), password=os.getenv("POSTGRES_PASSWORD", "quantpassword"), database=os.getenv("POSTGRES_DB", "market_data"))
            cur = conn.cursor()
            cur.execute("SELECT close FROM daily_ohlcv WHERE symbol = %s AND time::date = %s", (symbol, date))
            row = cur.fetchone()
            entry_price = float(row[0]) if row else 0.0
            cur.close()
            conn.close()
            
            outcome, pnl, exit_date, current_price = track_performance(symbol, date, entry_price)
            if outcome == "LOSS":
                cool_off_tracker[symbol] = date
                
            trades.append({
                "symbol": symbol, "entry_price": entry_price, "outcome": outcome, "pnl": pnl, "exit_date": exit_date
            })
            
        results.append({
            "date": date,
            "market_regime": run_data["macro_regime"],
            "gate_1_candidates": all_candidates,
            "gate_2_approved": run_data["approved"],
            "agent_scores": run_data["agent_scores"],
            "trades": trades
        })
        
        with open("backtest_results.json", "w") as f:
            json.dump(results, f, indent=4)
            
    logging.info("Sovereign Elite Backtest Complete.")

if __name__ == "__main__":
    run_backtest()
