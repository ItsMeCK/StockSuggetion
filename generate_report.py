import json
import os

def generate_full_dashboard():
    with open('backtest_results.json', 'r') as f:
        data = json.load(f)
        
    trades_html = ""
    profits = 0
    losses = 0
    total_approved = 0
    total_candidates = 0
    
    for day in data:
        total_candidates += len(day.get('gate_1_candidates', []))
        total_approved += len(day.get('gate_2_approved', []))
        
        for t in day.get('trades', []):
            status = t['outcome']
            if status == "PROFIT": profits += 1
            elif status == "LOSS": losses += 1
            
            pnl_color = "var(--green)" if status == "PROFIT" else ("var(--red)" if status == "LOSS" else ( "var(--green)" if t['pnl'] > 0 else "var(--red)" if t['pnl'] < 0 else "var(--text)"))
            
            trades_html += f"""
                <tr>
                    <td>{day['date']}</td>
                    <td class="{'badge-winner' if t['symbol'] == 'BHEL' else ''}">{t['symbol']}</td>
                    <td><span class="badge badge-{status.lower()}">{status}</span></td>
                    <td>₹{t['entry_price']:.1f}</td>
                    <td>₹{t.get('current_price', t['entry_price']):.1f}</td>
                    <td style="color: {pnl_color}">{t['pnl']}%</td>
                    <td style="color: var(--text-dim); font-size: 0.85rem;">Scored {t.get('confidence', 75)}%</td>
                </tr>
            """

    win_rate = int((profits / (profits + losses) * 100)) if (profits + losses) > 0 else 0

    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Midnight Sovereign: Comprehensive Backtest</title>
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
        }}
        body {{ background-color: var(--bg); color: var(--text); font-family: 'Outfit', sans-serif; margin: 0; padding: 2rem; background: radial-gradient(circle at top right, #1a1b3a, var(--bg)); min-height: 100vh; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 3rem; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 1rem; }}
        h1 {{ font-size: 2rem; margin: 0; background: linear-gradient(to right, var(--accent), var(--secondary)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
        .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1.5rem; margin-bottom: 3rem; }}
        .stat-card {{ background: var(--card-bg); backdrop-filter: blur(10px); padding: 1.5rem; border-radius: 1rem; border: 1px solid rgba(255,255,255,0.05); text-align: center; }}
        .stat-value {{ font-size: 2.5rem; font-weight: 600; margin-bottom: 0.5rem; }}
        .stat-label {{ color: var(--text-dim); text-transform: uppercase; letter-spacing: 1px; font-size: 0.8rem; }}
        .table-container {{ background: var(--card-bg); border-radius: 1rem; overflow: hidden; border: 1px solid rgba(255,255,255,0.05); }}
        table {{ width: 100%; border-collapse: collapse; text-align: left; }}
        th {{ background: rgba(255,255,255,0.02); padding: 1.2rem; color: var(--text-dim); font-size: 0.75rem; text-transform: uppercase; }}
        td {{ padding: 1.2rem; border-bottom: 1px solid rgba(255,255,255,0.02); }}
        .badge {{ padding: 0.25rem 0.75rem; border-radius: 2rem; font-size: 0.75rem; font-weight: 600; }}
        .badge-profit {{ background: rgba(16, 185, 129, 0.1); color: var(--green); border: 1px solid var(--green); }}
        .badge-loss {{ background: rgba(239, 68, 68, 0.1); color: var(--red); border: 1px solid var(--red); }}
        .badge-open {{ background: rgba(255, 255, 255, 0.1); color: var(--text); border: 1px solid var(--text); }}
        .badge-winner {{ color: #facc15; font-weight: bold; border-left: 3px solid #facc15; padding-left: 10px; }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>MIDNIGHT SOVEREIGN <span>V2.2 FULL</span></h1>
            <div style="color: var(--text-dim)">Comprehensive April 2026 Backtest</div>
        </header>
        <div class="stats-grid">
            <div class="stat-card"><div class="stat-value">{total_candidates}</div><div class="stat-label">Gate 1 Total</div></div>
            <div class="stat-card"><div class="stat-value" style="color: var(--secondary)">{total_approved}</div><div class="stat-label">Gate 2 Total</div></div>
            <div class="stat-card"><div class="stat-value" style="color: var(--green)">{profits}</div><div class="stat-label">Profits</div></div>
            <div class="stat-card"><div class="stat-value" style="color: var(--red)">{losses}</div><div class="stat-label">Losses</div></div>
            <div class="stat-card"><div class="stat-value" style="color: var(--accent)">{win_rate}%</div><div class="stat-label">Win Rate</div></div>
        </div>
        <div class="table-container">
            <table>
                <thead><tr><th>Date</th><th>Symbol</th><th>Status</th><th>Entry</th><th>Current</th><th>PnL</th><th>Note</th></tr></thead>
                <tbody>{trades_html}</tbody>
            </table>
        </div>
    </div>
</body>
</html>
    """
    with open('cognitive_report.html', 'w') as f:
        f.write(html)
    print("Full report generated at cognitive_report.html")

if __name__ == "__main__":
    generate_full_dashboard()
