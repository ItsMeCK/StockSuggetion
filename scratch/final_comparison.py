import psycopg2
import os
from datetime import datetime

def get_price(cur, ticker, date_str):
    cur.execute("""
        SELECT close FROM daily_ohlcv 
        WHERE symbol = %s AND time::date <= %s 
        ORDER BY time DESC LIMIT 1;
    """, (ticker, date_str))
    res = cur.fetchone()
    return float(res[0]) if res else None

def compare_systems():
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        user="quant",
        password="quantpassword",
        database="market_data"
    )
    cur = conn.cursor()

    # System A: OpenAI GPT-4o (Strict)
    # Approved KRISHIVAL on May 11.
    openai_trades = [
        {"ticker": "KRISHIVAL", "buy_date": "2026-05-11"}
    ]

    # System B: Sonnet Heuristic (Relaxed)
    # Approved these on May 11:
    sonnet_trades = [
        {"ticker": "BIOCON", "buy_date": "2026-05-11"},
        {"ticker": "OIL", "buy_date": "2026-05-11"},
        {"ticker": "ADSL", "buy_date": "2026-05-11"},
        {"ticker": "HINDZINC", "buy_date": "2026-05-11"}
    ]

    # Veto Audit: Stocks that Sonnet liked but OpenAI rejected on May 11
    vetoed_by_openai = ["GOPAL", "BIOCON", "SAGILITY", "BASF", "SAIL", "WESTLIFE", "SAURASHCEM", "INDIGOPNTS"]

    exit_date = "2026-05-15"

    print("--- END-TO-END COMPARISON (MAY 11-15) ---")
    
    print("\nSYSTEM A: OPENAI GPT-4O VISION (The Skeptical Auditor)")
    for t in openai_trades:
        buy_p = get_price(cur, t['ticker'], t['buy_date'])
        exit_p = get_price(cur, t['ticker'], exit_date)
        if buy_p and exit_p:
            pnl = (exit_p - buy_p) / buy_p * 100
            print(f"  {t['ticker']}: Buy at {buy_p:.2f} (May 11) -> Current {exit_p:.2f}. PnL: {pnl:+.2f}%")

    print("\nSYSTEM B: SONNET HEURISTIC (The Picky Librarian)")
    for t in sonnet_trades:
        buy_p = get_price(cur, t['ticker'], t['buy_date'])
        exit_p = get_price(cur, t['ticker'], exit_date)
        if buy_p and exit_p:
            pnl = (exit_p - buy_p) / buy_p * 100
            print(f"  {t['ticker']}: Buy at {buy_p:.2f} (May 11) -> Current {exit_p:.2f}. PnL: {pnl:+.2f}%")

    print("\nNEURAL AUDIT: What did OpenAI save us from? (Rejected by OpenAI)")
    for ticker in vetoed_by_openai:
        buy_p = get_price(cur, ticker, "2026-05-11")
        exit_p = get_price(cur, ticker, exit_date)
        if buy_p and exit_p:
            pnl = (exit_p - buy_p) / buy_p * 100
            status = "GOOD VETO (Saved Loss)" if pnl < 0 else "MISS (Lost Profit)"
            print(f"  {ticker}: Potential Buy May 11 -> Current. PnL: {pnl:+.2f}% -> {status}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    compare_systems()
