import re

log_path = "/Users/poonamsalke/.gemini/antigravity/brain/310c2a32-4d78-4de7-9a47-64d8d330ff45/.system_generated/tasks/task-3596.log"
output_path = "/Users/poonamsalke/.gemini/antigravity/brain/310c2a32-4d78-4de7-9a47-64d8d330ff45/july_trades.md"

trades = []
with open(log_path, 'r') as f:
    for line in f:
        # Looking for: 2026-07-28 23:57:01,135 - INFO - [2026-07-05] TRADE EXECUTED: RADICO. Outcome: -22.50%
        match = re.search(r'\[(.*?)\] TRADE EXECUTED: (.*?). Outcome: (.*?)%', line)
        if match:
            date = match.group(1)
            symbol = match.group(2)
            pnl = float(match.group(3))
            trades.append((date, symbol, pnl))

# Sort by date
trades.sort(key=lambda x: x[0])

markdown = "# All 150 Executed Trades (July 1 - 30)\n\n"
markdown += "Here is the comprehensive list of all trades executed by the Asymmetric Probability Engine.\n\n"
markdown += "| Date | Symbol | Option Return (PnL) |\n"
markdown += "| :--- | :--- | :--- |\n"

for date, symbol, pnl in trades:
    # Color code the PnL
    if pnl > 0:
        pnl_str = f"**<span style='color:green'>+{pnl:.2f}%</span>**"
    else:
        pnl_str = f"<span style='color:red'>{pnl:.2f}%</span>"
        
    # Highlight the unicorns!
    if pnl > 100:
        pnl_str += " 🚀"
        
    markdown += f"| {date} | {symbol} | {pnl_str} |\n"

with open(output_path, 'w') as f:
    f.write(markdown)
    
print(f"Successfully wrote {len(trades)} trades to {output_path}")
