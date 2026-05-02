import os
import logging
import json
import psycopg2
import polars as pl
from datetime import datetime, timedelta
from typing import Dict, Any, List

from core.state import SovereignState
from core.config import DB_CONFIG
from pipeline.screener import SovereignScreener
from agents.macro_gate import run_macro_regime_gate
from agents.pattern_agent import run_pattern_agent
from agents.critic_agent import run_critic_agent
from agents.sector_agent import run_sector_agent

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SovereignDeepBacktester:
    def __init__(self):
        self.screener = SovereignScreener()
        self.open_trades = [] 
        self.closed_trades = []
        self.history = [] # Full daily logs
        self.target_profit = 0.10
        self.stop_loss = 0.05

    def get_trading_days(self, start_date: str, end_date: str) -> List[str]:
        conn = psycopg2.connect(**DB_CONFIG)
        query = "SELECT DISTINCT time::date FROM daily_ohlcv WHERE symbol = 'NIFTY 50' AND time::date BETWEEN %s AND %s ORDER BY time ASC"
        df = pl.read_database(query, conn, execute_options={"parameters": (start_date, end_date)})
        conn.close()
        return [d.strftime('%Y-%m-%d') for d in df["time"].to_list()]

    def get_price_on_date(self, symbol: str, target_date: str) -> float:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        query = "SELECT close FROM daily_ohlcv WHERE symbol = %s AND time::date <= %s ORDER BY time DESC LIMIT 1"
        cur.execute(query, (symbol, target_date))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return float(row[0]) if row else None

    def run_simulation(self, start_date: str, end_date: str):
        days = self.get_trading_days(start_date, end_date)
        logging.info(f"STARTING SOVEREIGN DEEP BACKTEST: {len(days)} trading days found.")

        for current_day in days:
            logging.info(f"\n>>> SIMULATING DATE: {current_day} <<<")
            
            # 1. Manage Existing Trades (Check Exits)
            self.manage_exits(current_day)

            # 2. Daily Scan for New Entries
            state = SovereignState(
                target_date=current_day,
                macro_regime="",
                candidates=[],
                agent_scores={},
                vision_validations={},
                critic_results={},
                approved_allocations={}
            )

            # A. Macro Gate
            macro_delta = run_macro_regime_gate(state)
            state.update(macro_delta)
            
            if state["macro_regime"] == "CAPITULATION":
                logging.warning(f"MARKET CAPITULATION on {current_day}. Skipping entries.")
                continue

            # B. Screener
            # Exclude currently open symbols
            active_symbols = [t["symbol"] for t in self.open_trades]
            candidates, incubator, base_scores, macro_regime_full = self.screener.run_pipeline(target_date=current_day)
            
            # Filter candidates already in portfolio
            candidates = [c for c in candidates if c not in active_symbols]
            state["candidates"] = candidates
            state["agent_scores"] = {
                c: {
                    "entry": float(base_scores.get(c, 70.0)),
                    "vision": 70.0,
                    "dtw": 70.0,
                    "sector": 70.0
                } for c in candidates
            }

            if not candidates:
                continue

            # C. Cognitive Engine (Vision + Sector + Critic)
            # 1. Vision Pattern Audit
            vision_delta = run_pattern_agent(state)
            state.update(vision_delta)

            # 2. Sector Strength Audit
            sector_delta = run_sector_agent(state)
            state.update(sector_delta)

            # 3. Critic Veto Audit
            critic_delta = run_critic_agent(state)
            state.update(critic_delta)

            # D. Entry Execution
            for symbol, result in state["critic_results"].items():
                if result.get("approved"):
                    # Check if we have room (max 5 open trades)
                    if len(self.open_trades) < 5:
                        price = self.get_price_on_date(symbol, current_day)
                        if price:
                            logging.info(f"💎 INSTITUTIONAL BUY: {symbol} at ₹{price:.2f} (Confidence: {result['total_confidence']:.1f}%)")
                            self.open_trades.append({
                                "symbol": symbol,
                                "entry_date": current_day,
                                "entry_price": price,
                                "stop_loss": price * (1 - self.stop_loss),
                                "take_profit": price * (1 + self.target_profit)
                            })
            
            # Record state history
            self.history.append({
                "date": current_day,
                "macro_regime": state["macro_regime"],
                "candidates": state["candidates"],
                "open_trades_count": len(self.open_trades),
                "closed_trades_count": len(self.closed_trades)
            })

        # 3. Forced Exit for remaining positions at terminal date
        if self.open_trades:
            final_day = days[-1]
            logging.info(f"\n>>> FORCED LIQUIDATION: Simulation End on {final_day} <<<")
            self.manage_exits(final_day, forced=True)

        self.generate_report()

    def manage_exits(self, current_date: str, forced: bool = False):
        still_open = []
        for trade in self.open_trades:
            current_price = self.get_price_on_date(trade["symbol"], current_date)
            if not current_price:
                still_open.append(trade)
                continue

            # Exit Conditions
            exit_reason = None
            if current_price <= trade["stop_loss"]:
                exit_reason = "STOP_LOSS"
            elif current_price >= trade["take_profit"]:
                exit_reason = "TAKE_PROFIT"
            elif forced:
                exit_reason = "FORCED_EXIT"

            if exit_reason:
                pnl = ((current_price - trade["entry_price"]) / trade["entry_price"]) * 100
                trade.update({
                    "exit_date": current_date,
                    "exit_price": current_price,
                    "pnl_pct": float(pnl),
                    "reason": exit_reason
                })
                self.closed_trades.append(trade)
                logging.info(f"🚢 EXIT {trade['symbol']}: {exit_reason} at ₹{current_price:.2f} | P&L: {pnl:+.2f}%")
            else:
                still_open.append(trade)
        
        self.open_trades = still_open

    def generate_report(self):
        logging.info("\n" + "="*50)
        logging.info("SOVEREIGN DEEP BACKTEST SUMMARY")
        logging.info("="*50)
        
        report = {
            "summary": {
                "total_trades": len(self.closed_trades),
                "win_rate": 0,
                "total_pnl": 0
            },
            "history": self.history,
            "trades": self.closed_trades
        }

        if self.closed_trades:
            total_pnl = sum(t["pnl_pct"] for t in self.closed_trades)
            wins = [t for t in self.closed_trades if t["pnl_pct"] > 0]
            win_rate = (len(wins) / len(self.closed_trades)) * 100
            report["summary"].update({
                "win_rate": win_rate,
                "total_pnl": total_pnl
            })
            
            logging.info(f"Total Trades: {len(self.closed_trades)}")
            logging.info(f"Win Rate: {win_rate:.1f}%")
            logging.info(f"Cumulative P&L: {total_pnl:+.2f}%")
        
        with open("backtest_results.json", "w") as f:
            json.dump(report, f, indent=4)
        logging.info("Comprehensive Telemetry saved to: backtest_results.json")
        
        # Breakdown by reason
        tp = [t for t in self.closed_trades if t["reason"] == "TAKE_PROFIT"]
        sl = [t for t in self.closed_trades if t["reason"] == "STOP_LOSS"]
        forced = [t for t in self.closed_trades if t["reason"] == "FORCED_EXIT"]
        logging.info(f"Take Profits Hit: {len(tp)}")
        logging.info(f"Stop Losses Hit: {len(sl)}")
        logging.info(f"Forced Exits: {len(forced)}")
        logging.info("="*50)

if __name__ == "__main__":
    tester = SovereignDeepBacktester()
    tester.run_simulation("2026-02-01", "2026-04-30")
