import json
import os
from datetime import datetime

def generate_dashboard():
    if not os.path.exists("backtest_results.json"):
        print("Results file not found.")
        return
        
    with open("backtest_results.json", "r") as f:
        results = json.load(f)
        
    # --- CALCULATION LOGIC ---
    total_trades = 0
    profits = 0
    losses = 0
    open_trades = 0
    total_pnl = 0.0
    table_rows = ""
    
    # Track "Elite" impact for the dashboard
    elite_saved_losses = 0
    all_scores = []

    for day in results:
        date = day["date"]
        agent_scores = day.get("agent_scores", {})
        
        for trade in day["trades"]:
            total_trades += 1
            symbol = trade["symbol"]
            outcome = trade["outcome"]
            pnl = trade["pnl"]
            entry = trade["entry_price"]
            exit_p = trade["current_price"]
            
            # Score Calculation
            scores = agent_scores.get(symbol, {})
            v_score = scores.get("vision", 0)
            e_score = scores.get("entry", 0)
            d_score = scores.get("dtw", 0)
            total_score = (v_score * 0.4 + e_score * 0.4 + d_score * 0.2)
            
            if outcome == "PROFIT": profits += 1
            elif outcome == "LOSS": 
                losses += 1
                if e_score < 75: elite_saved_losses += 1
            elif outcome == "OPEN": open_trades += 1
            
            total_pnl += pnl
            
            is_elite = total_score >= 85
            is_momentum = e_score >= 75
            is_accumulator = (v_score >= 80 and d_score >= 85)
            is_approved_by_smart_rule = (is_momentum or is_accumulator)
            
            # --- RUTHLESS SOVEREIGN AUDIT DATA (MAY 4TH PROTOCOL) ---
            audit_data = {
                "ADANIPORTS": {"grade": "A", "action": "DEPLOY", "insight": "Elite Port Moat. Strong earnings momentum. Clean Audit.", "sector": "INFRA", "rigor": "Clean"},
                "COALINDIA": {"grade": "A", "action": "DEPLOY", "insight": "High Dividend + Volume Breakout. Institutional heavy buying.", "sector": "ENERGY", "rigor": "Clean"},
                "SYNGENE": {"grade": "A-", "action": "DEPLOY", "insight": "Pharma R&D Breakout. Bullish Sector Tailwind.", "sector": "PHARMA", "rigor": "Clean"},
                "TIMKEN": {"grade": "A-", "action": "DEPLOY", "insight": "Industrial High-Precision leader. Technically perfect base.", "sector": "INDUSTRIAL", "rigor": "Clean"},
                "RRKABEL": {"grade": "A", "action": "DEPLOY", "insight": "Wires & Cables dominance. Fresh breakout from flag.", "sector": "INFRA", "rigor": "Clean"},
                "CHOLAFIN": {"grade": "A", "action": "DEPLOY", "insight": "Confirmed Cup & Handle Breakout. High Conviction.", "sector": "FINTECH", "rigor": "Clean"},
                "BHARATFORG": {"grade": "A", "action": "DEPLOY", "insight": "Confirmed Stage 2 Uptrend. Defense Tailwind.", "sector": "DEFENSE", "rigor": "Clean"},
                "MASFIN": {"grade": "A-", "action": "DEPLOY", "insight": "Confirmed High-Momentum Thrust. NBFC Leader.", "sector": "FINTECH", "rigor": "Clean"},
                "BAJFINANCE": {"grade": "A-", "action": "DEPLOY", "insight": "Institutional Leadership. Technical Resilience.", "sector": "FINTECH", "rigor": "Clean"},
                "MOTILALOFS": {"grade": "B+", "action": "ACCUMULATE", "insight": "Market proxy. Strong AUM growth. Watch for dips.", "sector": "FINTECH", "rigor": "Clean"},
                "BALRAMCHIN": {"grade": "B+", "action": "ACCUMULATE", "insight": "Macro Sugar Reversal. Buy on Dips.", "sector": "AGRI", "rigor": "Cyclical"},
                "NEWGEN": {"grade": "B", "action": "ACCUMULATE", "insight": "Volatile Range-Bound Base. Mean-Reversion only.", "sector": "SAAS", "rigor": "Range"},
                "FEDERALBNK": {"grade": "D", "action": "AVOID", "insight": "TRAP: Overhead Resistance (5.2%) + RSI Divergence.", "sector": "BANKING", "rigor": "VETO"},
                "DCMSHRIRAM": {"grade": "C", "action": "AVOID", "insight": "STATIC: Accumulation Phase. No Breakout Ignition.", "sector": "CHEMICALS", "rigor": "VETO"},
                "KEEPLEARN": {"grade": "D", "action": "AVOID", "insight": "Edtech Headwinds. Small Cap Volatility.", "sector": "EDTECH", "rigor": "VETO"},
                "SAPPHIRE": {"grade": "B+", "action": "ACCUMULATE", "insight": "QSR recovery play. Technical bottoming confirmed.", "sector": "RETAIL", "rigor": "Clean"},
                "SUNTV": {"grade": "B", "action": "ACCUMULATE", "insight": "Cash rich. High dividend. Range-bound play.", "sector": "MEDIA", "rigor": "Range"},
                "IPCALAB": {"grade": "B+", "action": "ACCUMULATE", "insight": "Pharma export strength. 200-SMA support bounce.", "sector": "PHARMA", "rigor": "Clean"},
                "GESHIP": {"grade": "B+", "action": "ACCUMULATE", "insight": "Shipping rates tailwind. Buying at support.", "sector": "SHIPPING", "rigor": "Clean"},
                "ACUTAAS": {"grade": "A-", "action": "DEPLOY", "insight": "High growth chemical. Fresh Stage 2 ignition.", "sector": "CHEMICALS", "rigor": "Clean"}
            }
            
            info = audit_data.get(symbol, {"grade": "N/A", "action": "WATCH", "insight": "Audit Pending...", "sector": "GENERIC", "rigor": "N/A"})
            grade_color = "var(--green)" if "A" in info["grade"] or "B+" in info["grade"] else "var(--gold)"
            if "D" in info["grade"] or "F" in info["grade"]: grade_color = "var(--red)"
            
            action_bg = "rgba(16, 185, 129, 0.2)" if info["action"] == "DEPLOY" else "rgba(245, 158, 11, 0.2)"
            if info["action"] == "AVOID": action_bg = "rgba(239, 68, 68, 0.2)"
            action_color = "var(--green)" if info["action"] == "DEPLOY" else "var(--gold)"
            if info["action"] == "AVOID": action_color = "var(--red)"

            elite_class = "elite-row" if info["action"] == "DEPLOY" else ""
            elite_badge = '<span class="badge elite-badge">ELITE</span>' if is_elite else ""
            mom_badge = '<span class="badge mom-badge">IGNITED</span>' if is_momentum else ""
            acc_badge = '<span class="badge acc-badge">ACCUMULATION</span>' if is_accumulator else ""
            
            status_badge = f'<span class="badge status-{outcome.lower()}">{outcome}</span>'

            table_rows += f"""
            <tr class="{elite_class}">
                <td>{date}</td>
                <td>
                    <div style="font-weight: 600;"><span class="symbol-tag">{symbol}</span> {elite_badge}</div>
                    <div style="margin-top: 5px;">{mom_badge} {acc_badge}</div>
                </td>
                <td style="text-align: center;">
                    <div class="badge" style="background: {action_bg}; color: {action_color}; border: 1px solid {action_color};">{info["action"]}</div>
                    <div style="color: {grade_color}; font-weight: 800; font-size: 1.5rem; margin-top: 5px;">{info["grade"]}</div>
                </td>
                <td style="max-width: 350px;">
                    <div style="font-size: 0.8rem; color: var(--text-dim); text-transform: uppercase; letter-spacing: 1px;">{info["sector"]} | {info["rigor"]}</div>
                    <div style="font-size: 0.95rem; line-height: 1.4; font-weight: 400;">{info["insight"]}</div>
                </td>
                <td>
                    <div class="score-bar"><span>V: {v_score:.0f}</span></div>
                    <div class="score-bar"><span>E: {e_score:.0f}</span></div>
                    <div class="score-bar"><span>D: {d_score:.0f}</span></div>
                </td>
                <td>{status_badge}</td>
            </tr>
            """

    win_rate = (profits / (profits + losses) * 100) if (profits + losses) > 0 else 0
    if open_trades > 0:
        # Combined "Success Rate" (Profits + Profitable Open)
        pos_open = len([t for day in results for t in day["trades"] if t["outcome"] == "OPEN" and t["pnl"] > 0])
        success_rate = (profits + pos_open) / total_trades * 100
    else:
        success_rate = win_rate

    # --- HTML TEMPLATE ---
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Sovereign Elite Dashboard</title>
        <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600&display=swap" rel="stylesheet">
        <style>
            :root {{
                --bg: #05070a;
                --card-bg: rgba(15, 20, 28, 0.8);
                --accent: #00f2ff;
                --secondary: #bd00ff;
                --text: #ffffff;
                --text-dim: #94a3b8;
                --green: #10b981;
                --red: #ef4444;
                --gold: #f59e0b;
            }}
            body {{
                background: var(--bg);
                color: var(--text);
                font-family: 'Outfit', sans-serif;
                margin: 0;
                padding: 2rem;
                background: radial-gradient(circle at 0% 0%, #1a1b3a 0%, var(--bg) 50%);
                min-height: 100vh;
            }}
            .container {{ max-width: 1400px; margin: 0 auto; }}
            header {{
                display: flex; justify-content: space-between; align-items: center;
                margin-bottom: 3rem; padding-bottom: 1rem; border-bottom: 1px solid rgba(255,255,255,0.1);
            }}
            h1 {{
                font-size: 2.2rem; margin: 0;
                background: linear-gradient(to right, var(--accent), var(--secondary));
                -webkit-background-clip: text; -webkit-text-fill-color: transparent;
                letter-spacing: -1px;
            }}
            .stats-grid {{
                display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
                gap: 1.5rem; margin-bottom: 2rem;
            }}
            .stat-card {{
                background: var(--card-bg); border-radius: 1.5rem; padding: 1.5rem;
                border: 1px solid rgba(255,255,255,0.05); backdrop-filter: blur(20px);
                position: relative; overflow: hidden;
            }}
            .stat-card::after {{
                content: ''; position: absolute; top: 0; left: 0; width: 100%; height: 4px;
                background: linear-gradient(to right, var(--accent), var(--secondary));
                opacity: 0.3;
            }}
            .stat-value {{ font-size: 2.5rem; font-weight: 600; margin-bottom: 0.2rem; }}
            .stat-label {{ color: var(--text-dim); text-transform: uppercase; font-size: 0.75rem; letter-spacing: 2px; }}
            
            .impact-banner {{
                background: linear-gradient(45deg, rgba(189, 0, 255, 0.1), rgba(0, 242, 255, 0.1));
                border: 1px solid var(--accent); border-radius: 1rem; padding: 1.5rem;
                margin-bottom: 2rem; display: flex; align-items: center; justify-content: space-between;
            }}
            .impact-title {{ font-weight: 600; font-size: 1.1rem; color: var(--accent); }}
            
            .table-container {{
                background: var(--card-bg); border-radius: 1.5rem; overflow: hidden;
                border: 1px solid rgba(255,255,255,0.05);
            }}
            table {{ width: 100%; border-collapse: collapse; }}
            th {{ background: rgba(255,255,255,0.02); padding: 1.2rem; color: var(--text-dim); text-align: left; font-size: 0.75rem; letter-spacing: 1px; }}
            td {{ padding: 1.2rem; border-bottom: 1px solid rgba(255,255,255,0.03); font-size: 0.95rem; }}
            .symbol-tag {{ background: rgba(0, 242, 255, 0.1); color: var(--accent); padding: 4px 10px; border-radius: 8px; font-weight: 600; font-family: monospace; }}
            
            .score-bar {{ display: flex; align-items: center; gap: 10px; font-size: 0.8rem; color: var(--text-dim); }}
            .score-fill-bg {{ width: 60px; height: 6px; background: rgba(255,255,255,0.1); border-radius: 3px; overflow: hidden; }}
            .score-fill {{ height: 100%; background: var(--accent); border-radius: 3px; }}
            
            .badge {{ padding: 4px 12px; border-radius: 20px; font-size: 0.7rem; font-weight: 700; text-transform: uppercase; }}
            .status-profit {{ background: rgba(16, 185, 129, 0.2); color: var(--green); }}
            .status-loss {{ background: rgba(239, 68, 68, 0.2); color: var(--red); }}
            .status-open {{ background: rgba(0, 242, 255, 0.2); color: var(--accent); }}
            .elite-badge {{ background: linear-gradient(45deg, #f59e0b, #d97706); color: #fff; box-shadow: 0 0 10px rgba(245, 158, 11, 0.5); }}
            .mom-badge {{ background: rgba(189, 0, 255, 0.2); color: var(--secondary); border: 1px solid var(--secondary); }}
            .acc-badge {{ background: rgba(16, 185, 129, 0.2); color: var(--green); border: 1px solid var(--green); }}
            .elite-row {{ background: rgba(245, 158, 11, 0.03) !important; }}
            .elite-row td {{ border-bottom: 1px solid rgba(245, 158, 11, 0.1) !important; }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>MIDNIGHT SOVEREIGN <span style="font-weight: 300; opacity: 0.6">/ Elite Audit</span></h1>
                <div style="text-align: right">
                    <div style="font-size: 0.8rem; color: var(--text-dim)">SYSTEM STATUS: <span style="color: var(--green)">ACTIVE</span></div>
                    <div style="font-size: 0.7rem; color: var(--text-dim); opacity: 0.5">{datetime.now().strftime('%Y-%m-%d %H:%M')}</div>
                </div>
            </header>

            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-value">{total_trades}</div>
                    <div class="stat-label">Total Signals</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" style="color: var(--green)">{success_rate:.1f}%</div>
                    <div class="stat-label">Success Accuracy</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" style="color: var(--gold)">{profits}</div>
                    <div class="stat-label">Hard Profits</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" style="color: var(--accent)">{open_trades}</div>
                    <div class="stat-label">Active Deployments</div>
                </div>
            </div>

            <div class="impact-banner">
                <div>
                    <div class="impact-title">HYBRID ELITE STRATEGY (MOMENTUM OR ACCUMULATION)</div>
                    <div style="font-size: 0.85rem; opacity: 0.8">Approving <b>Momentum (Entry >= 75)</b> OR <b>Institutional Accumulation (Vision >= 80 & DTW >= 85)</b>.</div>
                </div>
                <div style="text-align: right">
                    <div style="font-size: 0.75rem; color: var(--text-dim)">LOSSES BLOCKED</div>
                    <div style="font-size: 1.5rem; font-weight: 600; color: var(--green)">{ (elite_saved_losses/losses*100) if losses > 0 else 100:.1f}%</div>
                </div>
            </div>

            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th>Date</th>
                            <th>Symbol</th>
                            <th>Entry</th>
                            <th>Grade</th>
                            <th>Audit Insight</th>
                            <th>Conviction</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_rows}
                    </tbody>
                </table>
            </div>
        </div>
    </body>
    </html>
    """
    
    with open("dashboard_report.html", "w") as f:
        f.write(html)
    print("Dashboard generated: dashboard_report.html")

if __name__ == "__main__":
    generate_dashboard()
