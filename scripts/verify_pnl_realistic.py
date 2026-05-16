import os
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def calculate_real_pnl():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )
    
    # Get all signaled trades
    query = """
    SELECT symbol, time as scan_time, price_at_scan 
    FROM decision_ledger 
    WHERE mode = 'back_audit' AND status = 'SIGNALED' AND price_at_scan > 0
    """
    signals = pd.read_sql(query, conn)
    
    results = []
    
    for _, sig in signals.iterrows():
        ticker = sig['symbol']
        scan_date = sig['scan_time']
        entry_price = float(sig['price_at_scan'])
        
        # Fetch next 3 days of OHLCV
        ohlc_query = f"""
        SELECT time, open, high, low, close 
        FROM daily_ohlcv 
        WHERE symbol = '{ticker}' AND time > '{scan_date}' 
        ORDER BY time ASC LIMIT 3
        """
        price_history = pd.read_sql(ohlc_query, conn)
        
        if len(price_history) == 0:
            continue
            
        final_return = 0
        exit_reason = "TIME_EXIT"
        
        # Simulate Day by Day
        for i, day in price_history.iterrows():
            high_ret = (day['high'] - entry_price) / entry_price * 100
            low_ret = (day['low'] - entry_price) / entry_price * 100
            
            # 1. Check Stop Loss First (Conservative)
            if low_ret <= -5.0:
                final_return = -5.0
                exit_reason = "STOP_LOSS"
                break
            
            # 2. Check Profit Target
            if high_ret >= 10.0:
                final_return = 10.0
                exit_reason = "TAKE_PROFIT"
                break
                
            # If it's the 3rd day and no triggers, exit at close
            if i == len(price_history) - 1:
                final_return = (day['close'] - entry_price) / entry_price * 100
                exit_reason = "TIME_EXIT"

        results.append({
            "date": scan_date,
            "symbol": ticker,
            "return": final_return,
            "reason": exit_reason
        })

    res_df = pd.DataFrame(results)
    if res_df.empty:
        print("No signals found to calculate P&L.")
        return

    res_df['date'] = pd.to_datetime(res_df['date'])
    res_df['week'] = res_df['date'].dt.isocalendar().week
    res_df['month'] = res_df['date'].dt.month
    
    # Weekly Summary
    weekly = res_df.groupby(['month', 'week']).agg({
        "return": ["count", "mean", "sum"],
        "reason": lambda x: (x == "TAKE_PROFIT").sum()
    })
    weekly.columns = ["Trade Count", "Avg Return (%)", "Total Week Return (%)", "TP Hits"]
    
    print("\n🏛️ SOVEREIGN 3-DAY REALISTIC P&L (10% TP / 5% SL)")
    print("===============================================")
    print(weekly.to_string())
    
    # Monthly Convergence
    print("\n📈 MONTHLY EQUITY CURVE CONVERGENCE")
    monthly = res_df.groupby('month').agg({
        "return": ["count", "sum"],
        "reason": lambda x: (x == "TAKE_PROFIT").sum()
    })
    monthly.columns = ["Trades", "Total Monthly P&L (%)", "TP Hits"]
    print(monthly)

if __name__ == "__main__":
    calculate_real_pnl()
