import psycopg2
import json
import os
from datetime import datetime
from dotenv import load_dotenv
from kiteconnect import KiteConnect

def get_db_connection():
    load_dotenv()
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'quant'),
        password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
        dbname=os.getenv('POSTGRES_DB', 'market_data')
    )

def get_kite_client():
    load_dotenv()
    api_key = os.getenv('KITE_API_KEY')
    access_token = os.getenv('KITE_ACCESS_TOKEN')
    if api_key and access_token:
        try:
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)
            return kite
        except Exception as e:
            pass
    return None

def generate_dashboard():
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        WITH latest_status AS (
            SELECT trade_id, ticker, status, price, quantity, notes, system_time,
                   ROW_NUMBER() OVER(PARTITION BY trade_id ORDER BY system_time DESC) as rn
            FROM trade_events
        )
        SELECT trade_id, ticker, status, price, quantity, notes, system_time 
        FROM latest_status 
        WHERE rn = 1
    """)
    all_trades = cur.fetchall()
    
    active_trades, signaled_trades, closed_trades, incubating_trades = [], [], [], []
    tickers_to_fetch = set()
    
    for row in all_trades:
        status, ticker = row[2], row[1]
        if status == 'ACTIVE': active_trades.append(row)
        elif status == 'SIGNALED': signaled_trades.append(row)
        elif status == 'INCUBATING': incubating_trades.append(row)
        elif status.startswith('CLOSED'): closed_trades.append(row)
        if status != 'CLOSED': tickers_to_fetch.add(f"NSE:{ticker}")
            
    kite = get_kite_client()
    quotes = kite.quote(list(tickers_to_fetch)) if kite and tickers_to_fetch else {}
            
    total_active_pnl_pct, wins, losses = 0.0, 0, 0

    def build_row(row, is_active=True, is_closed=False, is_incubator=False):
        nonlocal total_active_pnl_pct, wins, losses
        trade_id, ticker, status, price, quantity, notes_str, sys_time = row
        price = float(price) if price is not None else 0.0
        try: notes = json.loads(notes_str)
        except: notes = {}
            
        ai_score = notes.get('ai_score', notes.get('conviction', 0.0))
        mom_score = notes.get('mom_score', notes.get('extension', 0.0))
        vol_z = notes.get('vol_z', notes.get('vol_surge', 0.0))
        turnover = notes.get('turnover', 0.0)
        regime = notes.get('macro', notes.get('regime', 'N/A'))
        
        display_date = sys_time.strftime("%Y-%m-%d")
        kite_symbol = f"NSE:{ticker}"
        live_price = quotes.get(kite_symbol, {}).get('last_price', 0.0)
        
        pnl_pct = 0.0
        if is_closed:
            exit_p = notes.get('exit_price', price)
            if price > 0: pnl_pct = ((exit_p - price) / price) * 100
            if status == 'CLOSED_WIN': wins += 1
            else: losses += 1
            live_price_display = f"₹{exit_p:.2f}"
        else:
            if price > 0 and live_price > 0: pnl_pct = ((live_price - price) / price) * 100
            live_price_display = f"₹{live_price:.2f}" if live_price > 0 else "N/A"
            if is_active: total_active_pnl_pct += pnl_pct
            
        pnl_color = "var(--green)" if pnl_pct >= 0 else "var(--red)"
        pnl_sign = "+" if pnl_pct >= 0 else ""
        pnl_display = f"<span style='color: {pnl_color}; font-weight: 600;'>{pnl_sign}{pnl_pct:.2f}%</span>"
        
        if is_incubator:
            # Special formatting for Incubators (Focus on Tightness and VDU)
            pnl_display = f"<span style='color: var(--gold);'>COILING</span>"
            if live_price > 0 and price > 0:
                day_change = ((live_price - price) / price) * 100
                if day_change > 2.0:
                    pnl_display = f"<span style='color: var(--green); font-weight: 800; animation: blink 1s infinite;'>🚀 IGNITION!</span>"

        return f"""
        <tr class="trade-row" data-ai="{ai_score}" data-mom="{mom_score}" data-vol="{vol_z}" data-liq="{turnover}">
            <td data-order="{sys_time.timestamp()}">{display_date}</td>
            <td><span class="symbol-tag">{ticker}</span></td>
            <td data-order="{price}">₹{price:.2f}</td>
            <td data-order="{live_price}" style="font-weight: 600;">{live_price_display}</td>
            <td data-order="{pnl_pct}">{pnl_display}</td>
            <td data-order="{ai_score}" style="color: var(--accent);">{ai_score:.1f}</td>
            <td data-order="{mom_score}" style="color: var(--secondary);">{mom_score:.1f}</td>
            <td data-order="{vol_z}" style="color: var(--gold);">{vol_z:.2f}</td>
            <td data-order="{turnover}">₹{turnover:.1f} Cr</td>
            <td>{regime}</td>
        </tr>
        """

    active_html = "".join([build_row(r) for r in active_trades])
    signaled_html = "".join([build_row(r, is_active=False) for r in signaled_trades])
    incubator_html = "".join([build_row(r, is_active=False, is_incubator=True) for r in incubating_trades])
    closed_html = "".join([build_row(r, is_closed=True) for r in closed_trades])

    avg_active_pnl = (total_active_pnl_pct / len(active_trades)) if active_trades else 0.0
    success_rate = (wins / (wins+losses) * 100) if (wins+losses) > 0 else 0.0
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    final_html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Sovereign: Institutional Analytics</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600&display=swap" rel="stylesheet">
    <style>
        :root {{ --bg: #0b0e14; --card-bg: rgba(23, 28, 38, 0.7); --accent: #00f2ff; --secondary: #bd00ff; --text: #ffffff; --text-dim: #94a3b8; --green: #10b981; --red: #ef4444; --gold: #f59e0b; }}
        body {{ background-color: var(--bg); color: var(--text); font-family: 'Outfit', sans-serif; margin: 0; padding: 2rem; background: radial-gradient(circle at top right, #1a1b3a, var(--bg)); min-height: 100vh; }}
        .container {{ max-width: 1500px; margin: 0 auto; }}
        header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 2rem; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 1rem; }}
        h1 {{ font-size: 1.8rem; margin: 0; background: linear-gradient(to right, var(--accent), var(--secondary)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
        
        .filter-bar {{ background: var(--card-bg); padding: 1.5rem; border-radius: 1rem; border: 1px solid var(--accent); margin-bottom: 2rem; display: flex; gap: 1.5rem; align-items: center; flex-wrap: wrap; }}
        .filter-group {{ display: flex; flex-direction: column; gap: 0.3rem; min-width: 150px; }}
        .filter-group label {{ font-size: 0.65rem; color: var(--text-dim); text-transform: uppercase; letter-spacing: 1px; }}
        input[type=range] {{ width: 100%; accent-color: var(--accent); }}
        
        .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1.5rem; margin-bottom: 2rem; }}
        .stat-card {{ background: var(--card-bg); padding: 1.2rem; border-radius: 1rem; border: 1px solid rgba(255,255,255,0.05); text-align: center; }}
        .stat-value {{ font-size: 1.8rem; font-weight: 600; margin-bottom: 0.3rem; }}
        .stat-label {{ color: var(--text-dim); text-transform: uppercase; font-size: 0.6rem; }}
        
        .section-title {{ font-size: 1.1rem; margin: 2rem 0 1rem 0; color: var(--accent); border-left: 4px solid var(--secondary); padding-left: 10px; }}
        .table-container {{ background: var(--card-bg); border-radius: 1rem; overflow: hidden; border: 1px solid rgba(255,255,255,0.05); margin-bottom: 2rem; }}
        table {{ width: 100%; border-collapse: collapse; text-align: left; }}
        th {{ background: rgba(255,255,255,0.02); padding: 1rem; color: var(--text-dim); font-size: 0.65rem; text-transform: uppercase; cursor: pointer; position: relative; transition: background 0.2s; }}
        th:hover {{ background: rgba(255,255,255,0.05); color: var(--accent); }}
        th::after {{ content: ' ↕'; opacity: 0.3; margin-left: 5px; }}
        th.asc::after {{ content: ' ▲'; opacity: 1; color: var(--accent); }}
        th.desc::after {{ content: ' ▼'; opacity: 1; color: var(--accent); }}
        td {{ padding: 1rem; border-bottom: 1px solid rgba(255,255,255,0.02); font-size: 0.85rem; }}
        .symbol-tag {{ background: rgba(255,255,255,0.05); padding: 4px 8px; border-radius: 6px; font-family: monospace; color: var(--accent); }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>MIDNIGHT SOVEREIGN <span>INSTITUTIONAL HUB</span></h1>
            <div style="color: var(--text-dim)">{generated_at}</div>
        </header>

        <div class="filter-bar">
            <div style="font-weight: 600; color: var(--accent); border-right: 1px solid #333; padding-right: 1.5rem;">DYNAMIC FILTERS</div>
            <div class="filter-group">
                <label>Min AI Score: <span id="aiVal">0</span></label>
                <input type="range" id="aiFilter" min="0" max="150" value="0" oninput="applyFilters()">
            </div>
            <div class="filter-group">
                <label>Min Momentum: <span id="momVal">0</span></label>
                <input type="range" id="momFilter" min="0" max="150" value="0" oninput="applyFilters()">
            </div>
            <div class="filter-group">
                <label>Min Vol Thrust: <span id="volVal">0</span></label>
                <input type="range" id="volFilter" min="0" max="10" step="0.1" value="0" oninput="applyFilters()">
            </div>
            <div class="filter-group">
                <label>Min Liquidity (Cr): <span id="liqVal">0</span></label>
                <input type="range" id="liqFilter" min="0" max="200" step="1" value="0" oninput="applyFilters()">
            </div>
            <div style="margin-left: auto; font-size: 0.8rem; color: var(--green);">Visible: <span id="visibleCount">-</span></div>
        </div>

        <div class="stats-grid">
            <div class="stat-card"><div class="stat-value" style="color: var(--accent)">{len(active_trades)}</div><div class="stat-label">Active</div></div>
            <div class="stat-card"><div class="stat-value" style="color: { 'var(--green)' if avg_active_pnl >= 0 else 'var(--red)' }">{avg_active_pnl:+.2f}%</div><div class="stat-label">Avg. P&L</div></div>
            <div class="stat-card"><div class="stat-value" style="color: var(--gold)">{success_rate:.1f}%</div><div class="stat-label">Success Rate</div></div>
        </div>

        <h2 class="section-title">Institutional Pipeline (Signals)</h2>
        <div class="table-container">
            <table id="signalTable">
                <thead><tr>
                    <th onclick="sortTable(0, 'signalTable')">Date</th>
                    <th onclick="sortTable(1, 'signalTable')">Symbol</th>
                    <th onclick="sortTable(2, 'signalTable')">Entry</th>
                    <th onclick="sortTable(3, 'signalTable')">Live</th>
                    <th onclick="sortTable(4, 'signalTable')">P&L</th>
                    <th onclick="sortTable(5, 'signalTable')">AI Score</th>
                    <th onclick="sortTable(6, 'signalTable')">Tightness</th>
                    <th onclick="sortTable(7, 'signalTable')">Vol Surge</th>
                    <th onclick="sortTable(8, 'signalTable')">Liquidity</th>
                    <th onclick="sortTable(9, 'signalTable')">Regime</th>
                </tr></thead>
                <tbody id="signalBody">{signaled_html if signaled_html else "<tr><td colspan='10' style='text-align:center;'>No signals.</td></tr>"}</tbody>
            </table>
        </div>

        <h2 class="section-title" style="color: var(--gold); border-left-color: var(--gold);">Incubator Watchlist (Coiled Springs)</h2>
        <div class="table-container" style="border-color: rgba(245, 158, 11, 0.2);">
            <table id="incubatorTable">
                <thead><tr>
                    <th onclick="sortTable(0, 'incubatorTable')">Date</th>
                    <th onclick="sortTable(1, 'incubatorTable')">Symbol</th>
                    <th onclick="sortTable(2, 'incubatorTable')">Base Price</th>
                    <th onclick="sortTable(3, 'incubatorTable')">Live</th>
                    <th onclick="sortTable(4, 'incubatorTable')">Status</th>
                    <th onclick="sortTable(5, 'incubatorTable')">AI Score</th>
                    <th onclick="sortTable(6, 'incubatorTable')">Tightness</th>
                    <th onclick="sortTable(7, 'incubatorTable')">Vol Dry-up</th>
                    <th onclick="sortTable(8, 'incubatorTable')">Liquidity</th>
                    <th onclick="sortTable(9, 'incubatorTable')">Regime</th>
                </tr></thead>
                <tbody id="incubatorBody">{incubator_html if incubator_html else "<tr><td colspan='10' style='text-align:center;'>No coiled springs.</td></tr>"}</tbody>
            </table>
        </div>

        <h2 class="section-title" style="color: var(--secondary);">Active Portfolio</h2>
        <div class="table-container">
            <table id="activeTable">
                <thead><tr>
                    <th onclick="sortTable(0, 'activeTable')">Date</th>
                    <th onclick="sortTable(1, 'activeTable')">Symbol</th>
                    <th onclick="sortTable(2, 'activeTable')">Entry</th>
                    <th onclick="sortTable(3, 'activeTable')">Live</th>
                    <th onclick="sortTable(4, 'activeTable')">P&L</th>
                    <th onclick="sortTable(5, 'activeTable')">AI Score</th>
                    <th onclick="sortTable(6, 'activeTable')">Momentum</th>
                    <th onclick="sortTable(7, 'activeTable')">Vol Z</th>
                    <th onclick="sortTable(8, 'activeTable')">Liquidity</th>
                    <th onclick="sortTable(9, 'activeTable')">Regime</th>
                </tr></thead>
                <tbody id="activeBody">{active_html if active_html else "<tr><td colspan='10' style='text-align:center;'>No active trades.</td></tr>"}</tbody>
            </table>
        </div>
    </div>

    <script>
    function applyFilters() {{
        const ai = document.getElementById('aiFilter').value;
        const mom = document.getElementById('momFilter').value;
        const vol = document.getElementById('volFilter').value;
        const liq = document.getElementById('liqFilter').value;
        
        document.getElementById('aiVal').innerText = ai;
        document.getElementById('momVal').innerText = mom;
        document.getElementById('volVal').innerText = vol;
        document.getElementById('liqVal').innerText = liq;
        
        let visible = 0;
        document.querySelectorAll('.trade-row').forEach(row => {{
            const rowAi = parseFloat(row.dataset.ai);
            const rowMom = parseFloat(row.dataset.mom);
            const rowVol = parseFloat(row.dataset.vol);
            const rowLiq = parseFloat(row.dataset.liq);
            
            if (rowAi >= ai && rowMom >= mom && rowVol >= vol && rowLiq >= liq) {{
                row.style.display = "";
                visible++;
            }} else {{
                row.style.display = "none";
            }}
        }});
        document.getElementById('visibleCount').innerText = visible;
    }}

    function sortTable(n, tableId) {{
        var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
        table = document.getElementById(tableId);
        
        // Remove classes from all headers in this table
        let headers = table.getElementsByTagName("TH");
        for (let h of headers) h.classList.remove("asc", "desc");
        
        switching = true;
        dir = "asc";
        while (switching) {{
            switching = false;
            rows = table.rows;
            for (i = 1; i < (rows.length - 1); i++) {{
                shouldSwitch = false;
                x = rows[i].getElementsByTagName("TD")[n];
                y = rows[i + 1].getElementsByTagName("TD")[n];
                
                let xVal = x.getAttribute('data-order') || x.innerText.toLowerCase();
                let yVal = y.getAttribute('data-order') || y.innerText.toLowerCase();
                
                if (!isNaN(xVal) && !isNaN(yVal)) {{
                    xVal = parseFloat(xVal);
                    yVal = parseFloat(yVal);
                }}

                if (dir == "asc") {{
                    if (xVal > yVal) {{ shouldSwitch = true; break; }}
                }} else if (dir == "desc") {{
                    if (xVal < yVal) {{ shouldSwitch = true; break; }}
                }}
            }}
            if (shouldSwitch) {{
                rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                switching = true;
                switchcount++;
            }} else {{
                if (switchcount == 0 && dir == "asc") {{
                    dir = "desc";
                    switching = true;
                }}
            }}
        }}
        // Add the correct class to the current header
        headers[n].classList.add(dir);
    }}
    window.onload = applyFilters;
    </script>
</body>
</html>
"""
    with open("dashboard_portfolio.html", "w") as f: f.write(final_html)
    print("Dashboard Updated with Liquidity and Regime Columns.")

if __name__ == "__main__": generate_dashboard()
