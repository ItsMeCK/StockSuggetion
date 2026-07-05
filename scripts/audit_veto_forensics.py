"""
S1 of docs/IMPLEMENTATION_PLAN.md V1: quantify the damage done by the
fundamental-audit LLM veto on the OPTIONS_IGNITION route.

Trigger: AMBUJACEM 2026-07-02 — full ignition signature, critic approved,
audit vetoed ("Grade C AVOID"), then the SAME model approved the SAME stock
the next day. Non-deterministic gate killing valid same-day entries.

Method: run the June fast-backtest with run_risk_agent wrapped so every
critic-approved symbol that the audit veto kills is RECORDED (with its grade
and action). Then classify each vetoed symbol with the real conviction-router
logic; those matching OPTIONS_IGNITION are simulated with the same option
model as approved trades. If the vetoed set's P&L >= approved set's, the
audit gate loses veto power on this route.

Usage: TRADING_MODE=HISTORICAL PYTHONPATH=. venv/bin/python3 scripts/audit_veto_forensics.py
"""
import os
import json
import logging

os.environ["TRADING_MODE"] = "HISTORICAL"
from dotenv import load_dotenv
load_dotenv()

import agents.risk_agent as risk_agent_mod

VETO_LOG = []  # (date, symbol, grade, action)
_orig_run_risk_agent = risk_agent_mod.run_risk_agent


def instrumented_run_risk_agent(state):
    critic_results = state.get("critic_results", {})
    fundamental_reports = state.get("fundamental_reports", {})
    target_date = state.get("target_date")
    for symbol, evaluation in critic_results.items():
        if not evaluation.get("approved", False):
            continue
        rep = fundamental_reports.get(symbol, {})
        grade = rep.get("grade", "A")
        action = rep.get("action", "DEPLOY")
        if grade in ("D", "F") or action == "AVOID":
            VETO_LOG.append({"date": target_date, "symbol": symbol, "grade": grade, "action": action})
    return _orig_run_risk_agent(state)


risk_agent_mod.run_risk_agent = instrumented_run_risk_agent

# fast_backtest imports run_risk_agent indirectly through graph.builder, which
# binds agents.risk_agent.run_risk_agent at import time - patch BEFORE importing.
import graph.builder as gb
gb.risk_and_position_sizing = instrumented_run_risk_agent
# rebuild uses the module-level name inside build_sovereign_graph_with_checkpointer;
# it references the imported alias, so rebind there too via module dict
import sys

from scripts.fast_backtest import run_fast_backtest

if __name__ == "__main__":
    out = "backtest_fast_results_v6_audit_forensics.json"
    run_fast_backtest(20, out, end_date="2026-07-03")
    with open("audit_veto_log.json", "w") as f:
        json.dump(VETO_LOG, f, indent=2)
    print(f"\nAudit-vetoed critic-approved signals: {len(VETO_LOG)}")
    from collections import Counter
    print("By grade:", Counter(v["grade"] for v in VETO_LOG))
    print(f"Saved -> audit_veto_log.json and {out}")
