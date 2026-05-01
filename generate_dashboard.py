import json
import os
from datetime import datetime

def generate_dashboard():
    results_path = "backtest_results.json"
    if not os.path.exists(results_path):
        print(f"Error: {results_path} not found.")
        return

    with open(results_path, "r") as f:
        data = json.load(f)

    # Calculate metrics
    total_days = len(data)
    all_trades = []
    for day in data:
        all_trades.extend(day.get("trades", []))

    total_trades = len(all_trades)
    profits = [t for t in all_trades if t["outcome"] == "PROFIT"]
    losses = [t for t in all_trades if t["outcome"] == "LOSS"]
    
    win_rate = (len(profits) / (len(profits) + len(losses)) * 100) if (len(profits) + len(losses)) > 0 else 0
    total_pnl = sum([t["pnl"] for t in all_trades])

    total_candidates = sum([len(day.get("gate_1_candidates", [])) for day in data])
    total_incubator = sum([len(day.get("gate_1_incubator", [])) for day in data])
    total_approved = sum([len(day.get("gate_2_approved", [])) for day in data])

    rows = ""
    # Flatten all trades across all days
    flattened_trades = []
    for day in data:
        for t in day.get("trades", []):
            flattened_trades.append({
                "date": day["date"],
                "symbol": t["symbol"],
                "entry_price": t["entry_price"],
                "current_price": t["current_price"],
                "pnl": t["pnl"],
                "outcome": t["outcome"]
            })

    # Sort trades by date (descending)
    flattened_trades.sort(key=lambda x: x["date"], reverse=True)

    for t in flattened_trades:
        outcome_class = f"outcome-{t['outcome'].lower()}"
        pnl_str = f"{t['pnl']:+.1f}%"
        
        status_badge = f'<span class="badge" style="background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1)">{t["outcome"]}</span>'
        if t["outcome"] == "PROFIT":
            status_badge = '<span class="badge" style="background: rgba(16, 185, 129, 0.1); color: #10b981; border: 1px solid #10b981">PROFIT</span>'
        elif t["outcome"] == "LOSS":
            status_badge = '<span class="badge" style="background: rgba(239, 68, 68, 0.1); color: #ef4444; border: 1px solid #ef4444">LOSS</span>'

        rows += f"""
        <tr>
            <td>{t['date']}</td>
            <td><span class="symbol-tag" style="font-weight: 600; font-size: 1rem;">{t['symbol']}</span></td>
            <td>₹{t['entry_price']:.2f}</td>
            <td>₹{t['current_price']:.2f}</td>
            <td class="{outcome_class}" style="font-size: 1.1rem;">{pnl_str}</td>
            <td>{status_badge}</td>
        </tr>
        """

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    pnl_color = "#10b981" if total_pnl >= 0 else "#ef4444"

    # Final HTML assembly
    final_html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Midnight Sovereign: Signal Audit Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg: #0b0e14;
            --card-bg: rgba(23, 28, 38, 0.7);
            --accent: #00f2ff;
            --secondary: #bd00ff;
            --text: #ffffff;
            --text-dim: #94a3b8;
            --green: #10b981;
            --red: #ef4444;
            --gold: #f59e0b;
        }}

        body {{
            background-color: var(--bg);
            color: var(--text);
            font-family: 'Outfit', sans-serif;
            margin: 0;
            padding: 2rem;
            background: radial-gradient(circle at top right, #1a1b3a, var(--bg));
            min-height: 100vh;
        }}

        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}

        header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 3rem;
            border-bottom: 1px solid rgba(255,255,255,0.1);
            padding-bottom: 1rem;
        }}

        h1 {{
            font-size: 2rem;
            margin: 0;
            background: linear-gradient(to right, var(--accent), var(--secondary));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}

        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1.5rem;
            margin-bottom: 3rem;
        }}

        .stat-card {{
            background: var(--card-bg);
            backdrop-filter: blur(10px);
            padding: 1.5rem;
            border-radius: 1rem;
            border: 1px solid rgba(255,255,255,0.05);
            text-align: center;
            transition: transform 0.3s ease;
        }}

        .stat-card:hover {{
            transform: translateY(-5px);
            border-color: var(--accent);
        }}

        .stat-value {{
            font-size: 2.2rem;
            font-weight: 600;
            margin-bottom: 0.5rem;
        }}

        .stat-label {{
            color: var(--text-dim);
            text-transform: uppercase;
            letter-spacing: 1px;
            font-size: 0.7rem;
        }}

        .table-container {{
            background: var(--card-bg);
            border-radius: 1rem;
            overflow: hidden;
            border: 1px solid rgba(255,255,255,0.05);
            margin-bottom: 2rem;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            text-align: left;
        }}

        th {{
            background: rgba(255,255,255,0.02);
            padding: 1.2rem;
            color: var(--text-dim);
            font-weight: 400;
            text-transform: uppercase;
            font-size: 0.75rem;
            letter-spacing: 1.5px;
        }}

        td {{
            padding: 1.2rem;
            border-bottom: 1px solid rgba(255,255,255,0.02);
            font-size: 0.95rem;
        }}

        tr:hover td {{
            background: rgba(0, 242, 255, 0.05);
        }}

        .badge {{
            padding: 0.3rem 0.8rem;
            border-radius: 0.5rem;
            font-size: 0.7rem;
            font-weight: 700;
            display: inline-block;
        }}

        .outcome-profit {{ color: var(--green); font-weight: 600; }}
        .outcome-loss {{ color: var(--red); font-weight: 600; }}
        .outcome-open {{ color: var(--accent); font-weight: 600; }}

        .symbol-tag {{
            background: rgba(255,255,255,0.05);
            padding: 4px 8px;
            border-radius: 6px;
            font-family: monospace;
            border: 1px solid rgba(255,255,255,0.1);
            color: var(--accent);
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>MIDNIGHT SOVEREIGN <span>SIGNAL AUDIT</span></h1>
            <div style="color: var(--text-dim)">3-Month Performance | Generated: {generated_at}</div>
        </header>

        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value">{total_trades}</div>
                <div class="stat-label">Total Signals</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="color: var(--green)">{len(profits)}</div>
                <div class="stat-label">Profits (10%)</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="color: var(--red)">{len(losses)}</div>
                <div class="stat-label">Losses (5%)</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="color: var(--accent)">{win_rate:.1f}%</div>
                <div class="stat-label">Win Rate</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="color: {pnl_color}">{total_pnl:+.1f}%</div>
                <div class="stat-label">Net Performance</div>
            </div>
        </div>

        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>Symbol</th>
                        <th>Entry Price</th>
                        <th>Exit/Current</th>
                        <th>PnL (%)</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
        </div>
    </div>
</body>
</html>
"""

    with open("dashboard_report.html", "w") as f:
        f.write(final_html)
    
    print("Dashboard generated: dashboard_report.html")

if __name__ == "__main__":
    generate_dashboard()
