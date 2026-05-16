import polars as pl
import re
import os

# 1. Load Winners and Diagnostics
winners_df = pl.read_csv("weekly_winners.csv")
diag_df = pl.read_csv("screening_diagnostics.csv")

# 2. Helper to parse log for agent decisions
def parse_log_for_decisions(log_path):
    decisions = {}
    with open(log_path, "r") as f:
        log_content = f.read()
        
    # Example patterns to look for:
    # 2026-05-16 20:18:49,764 - WARNING - CRITIC VETO: CCL - Failed Hybrid Elite criteria. (Conf: 61.5)
    # 2026-05-16 19:57:17,041 - INFO - VISION APPROVED: HEALTHCARE is a valid ascending_triangle setup.
    # Librarian Audit VETO: AJOONI -> INTRADAY_FADE
    
    for symbol in winners_df["symbol"]:
        sym_dec = {
            "screener": "PASSED",
            "librarian": "N/A",
            "critic": "N/A",
            "vision": "N/A"
        }
        
        # Check Screener Diagnostics first
        row = diag_df.filter(pl.col("symbol") == symbol)
        if len(row) > 0:
            row_dict = row.to_dicts()[0]
            reasons = []
            for col in diag_df.columns:
                if col.startswith("REJECT_") and row_dict[col] is True:
                    reasons.append(col.replace("REJECT_reason_", ""))
            if reasons:
                sym_dec["screener"] = f"FAILED: {', '.join(reasons)}"
        else:
            sym_dec["screener"] = "FAILED: No Market Data"

        # Search log for Librarian, Critic, Vision
        lib_match = re.search(fr"Librarian Audit VETO: {symbol} -> (\w+)", log_content)
        if lib_match:
            sym_dec["librarian"] = f"VETOED: {lib_match.group(1)}"
        elif f"Audit Approved for {symbol}" in log_content:
            sym_dec["librarian"] = "PASSED"
            
        critic_match = re.search(fr"CRITIC VETO: {symbol} - (.*)", log_content)
        if critic_match:
            sym_dec["critic"] = f"VETOED: {critic_match.group(1)}"
        elif f"HYBRID ELITE APPROVED: {symbol}" in log_content:
            sym_dec["critic"] = "PASSED"
            
        vision_match = re.search(fr"VISION APPROVED: {symbol}", log_content)
        if vision_match:
            sym_dec["vision"] = "PASSED"
        elif f"VISION REJECTED: {symbol}" in log_content:
            sym_dec["vision"] = "REJECTED"
            
        decisions[symbol] = sym_dec
        
    return decisions

# 3. Build Final Dataset
log_decisions = parse_log_for_decisions("weekly_report_data_ULTIMATE.log")

final_rows = []
for row in winners_df.to_dicts():
    sym = row["symbol"]
    dec = log_decisions.get(sym, {"screener": "MISSING", "librarian": "N/A", "critic": "N/A", "vision": "N/A"})
    final_rows.append({
        "Symbol": sym,
        "Weekly_Return_Pct": round(row["pnl_pct"], 2),
        "Screener_Status": dec["screener"],
        "Librarian_Status": dec["librarian"],
        "Critic_Status": dec["critic"],
        "Vision_Status": dec["vision"]
    })

pl.DataFrame(final_rows).write_csv("docs/weekly_winner_miss_analysis.csv")
print("Analysis CSV generated: docs/weekly_winner_miss_analysis.csv")
