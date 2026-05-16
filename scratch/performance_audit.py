import psycopg2
import os
from datetime import datetime
from decimal import Decimal

def get_price(cur, ticker, date_str):
    cur.execute("""
        SELECT close FROM daily_ohlcv 
        WHERE symbol = %s AND time::date <= %s 
        ORDER BY time DESC LIMIT 1;
    """, (ticker, date_str))
    res = cur.fetchone()
    return float(res[0]) if res else None

def analyze_performance():
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        user="quant",
        password="quantpassword",
        database="market_data"
    )
    cur = conn.cursor()

    # Data from logs
    openai_trades = {
        "2026-05-11": ['GOPAL', 'BIOCON', 'SAGILITY', 'BASF', 'SAIL', 'WESTLIFE', 'KRISHIVAL', 'SAURASHCEM', 'INDIGOPNTS'],
        "2026-05-12": ['HINDZINC', 'WESTLIFE', 'OIL', 'SAURASHCEM', 'BIOCON', 'BASF', 'INDIGOPNTS', 'HINDCOPPER', 'GOPAL', 'KRISHIVAL'],
        "2026-05-13": ['GOLDADD', 'SENCO', 'LICMFGOLD', 'CNL', 'MEDICO', 'FOSECOIND', 'TTKPRESTIG', 'GOKUL', 'HINDPETRO', 'WESTLIFE', 'TEXINFRA', 'MOLDTECH', 'CUBEXTUB', 'MONARCH', 'BAIDFIN', 'ASIANPAINT', 'KROSS', 'GROWWGOLD', 'GROWWLIQID', 'SAURASHCEM', 'AMRUTANJAN', 'KRISHIVAL', 'BASF', 'VGL', 'BALRAMCHIN', 'RANEHOLDIN', 'GANDHITUBE', 'SAGILITY', 'GOPAL', 'BTML', 'GOLD1', 'ESTER', 'MAYURUNIQ', 'MAHKTECH', 'TATASTEEL', 'RHL', 'PPAP', 'AUROPHARMA', 'JKLAKSHMI', 'HINDCOPPER', 'RAYMONDLSL']
    }

    sonnet_trades = {
        "2026-05-11": ['BIOCON', 'ASTERDM', 'ADSL'],
        "2026-05-12": ['BIOCON', 'OIL', 'ADSL'],
        "2026-05-13": ['BTML', 'AMRUTANJAN', 'GANDHITUBE', 'VGL', 'GOKUL', 'PPAP', 'TEXINFRA', 'MOLDTECH', 'JKLAKSHMI', 'LICMFGOLD', 'ESTER', 'MAHKTECH', 'DIXON', 'GROWWLIQID']
    }

    exit_date = "2026-05-15"

    def calc_group(trades_dict):
        total_pnl = 0
        count = 0
        winners = 0
        for date, tickers in trades_dict.items():
            for t in tickers:
                buy_p = get_price(cur, t, date)
                exit_p = get_price(cur, t, exit_date)
                if buy_p and exit_p:
                    pnl = (exit_p - buy_p) / buy_p * 100
                    total_pnl += pnl
                    count += 1
                    if pnl > 0: winners += 1
        return total_pnl, count, winners

    o_pnl, o_count, o_win = calc_group(openai_trades)
    s_pnl, s_count, s_win = calc_group(sonnet_trades)

    print(f"--- PERFORMANCE AUDIT (MAY 11-15) ---")
    print(f"OPENAI SYSTEM (Strict/Institutional):")
    print(f"  Total Trades: {o_count}")
    print(f"  Win Rate: {(o_win/o_count*100) if o_count > 0 else 0:.2f}%")
    print(f"  Avg PnL: {(o_pnl/o_count) if o_count > 0 else 0:.2f}%")
    print(f"  Total PnL Sum: {o_pnl:.2f}%")
    print(f"\nSONNET SYSTEM (Current/Local):")
    print(f"  Total Trades: {s_count}")
    print(f"  Win Rate: {(s_win/s_count*100) if s_count > 0 else 0:.2f}%")
    print(f"  Avg PnL: {(s_pnl/s_count) if s_count > 0 else 0:.2f}%")
    print(f"  Total PnL Sum: {s_pnl:.2f}%")

    cur.close()
    conn.close()

if __name__ == "__main__":
    analyze_performance()
