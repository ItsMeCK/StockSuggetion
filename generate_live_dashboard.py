import json
import os
import glob
from datetime import datetime
from dotenv import load_dotenv
from kiteconnect import KiteConnect

def generate_live_dashboard():
    # Load env and setup kite
    load_dotenv()
    api_key = os.getenv('KITE_API_KEY')
    access_token = os.getenv('KITE_ACCESS_TOKEN')
    
    kite = None
    if api_key and access_token:
        try:
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)
        except Exception as e:
            print(f"Kite init error: {e}")
            kite = None

    history_files = glob.glob("run_history/*.json")
    history_files.sort(key=os.path.getmtime, reverse=True)
    latest_files = history_files[:3]
    
    # Pre-collect all symbols to fetch quotes in one go
    all_symbols = set()
    run_datasets = []
    
    for file_path in latest_files:
        with open(file_path, "r") as f:
            run_data = json.load(f)
            run_datasets.append(run_data)
            approved_dict = run_data.get("approved_allocations", {})
            for sym in approved_dict.keys():
                all_symbols.add(f"NSE:{sym}")
                
    quotes = {}
    if kite and all_symbols:
        try:
            # Kite API expects list
            quotes = kite.quote(list(all_symbols))
        except Exception as e:
            print(f"Failed to fetch quotes: {e}")

    rows = ""
    for run_data in run_datasets:
        timestamp_str = run_data.get("timestamp", "Unknown")
        try:
            dt = datetime.fromisoformat(timestamp_str)
            display_date = dt.strftime("%Y-%m-%d %H:%M")
        except:
            display_date = timestamp_str
            
        macro = run_data.get("macro_regime", "UNKNOWN")
        approved_dict = run_data.get("approved_allocations", {})
        
        for symbol, details in approved_dict.items():
            entry_price = details.get("entry", 0)
            target = details.get("target", 0)
            stop_loss = details.get("stop_loss", 0)
            conviction = details.get("conviction_score", 0)
            
            kite_symbol = f"NSE:{symbol}"
            live_price = 0.0
            if kite_symbol in quotes:
                live_price = quotes[kite_symbol].get("last_price", 0.0)
                
            pnl_pct = 0.0
            if entry_price > 0 and live_price > 0:
                pnl_pct = ((live_price - entry_price) / entry_price) * 100
                
            pnl_color = "var(--green)" if pnl_pct >= 0 else "var(--red)"
            pnl_sign = "+" if pnl_pct >= 0 else ""
            pnl_display = f"<span style='color: {pnl_color}; font-weight: bold;'>{pnl_sign}{pnl_pct:.2f}%</span>" if live_price > 0 else "-"
            live_price_display = f"₹{live_price:.2f}" if live_price > 0 else "N/A"
            
            rows += f"""
            <tr>
                <td>{display_date}</td>
                <td><span class="symbol-tag" style="font-weight: 600; font-size: 1rem;">{symbol}</span></td>
                <td><span class="badge" style="background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1)">{macro}</span></td>
                <td>₹{entry_price:.2f}</td>
                <td style="font-weight: 600;">{live_price_display}</td>
                <td>{pnl_display}</td>
                <td style="color: var(--green);">₹{target:.2f}</td>
                <td style="color: var(--red);">₹{stop_loss:.2f}</td>
                <td style="color: var(--accent);">{conviction:.1f}</td>
            </tr>
            """

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Final HTML assembly
    final_html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Midnight Sovereign: Live Executions Dashboard</title>
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
            max-width: 1400px;
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
            <h1>MIDNIGHT SOVEREIGN <span>LIVE SUGGESTIONS</span></h1>
            <div style="color: var(--text-dim)">Latest 3 Executions | Generated: {generated_at}</div>
        </header>

        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th>Execution Time</th>
                        <th>Symbol</th>
                        <th>Macro Regime</th>
                        <th>Entry Level</th>
                        <th>Live Price</th>
                        <th>Live PnL</th>
                        <th>Target (2x)</th>
                        <th>Stop Loss</th>
                        <th>Conviction Score</th>
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

    with open("dashboard_live.html", "w") as f:
        f.write(final_html)
    
    print("Live Dashboard generated: dashboard_live.html")

if __name__ == "__main__":
    generate_live_dashboard()
