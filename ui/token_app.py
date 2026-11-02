import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import re
import json
import logging
import urllib.parse
from datetime import datetime
from flask import Flask, request, render_template_string, redirect, url_for, session, jsonify
from dotenv import load_dotenv
from kiteconnect import KiteConnect

from core.sovereign_execution_gateway import SovereignExecutionGateway
from core.smart_volume_reversal_engine import SmartVolumeReversalEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
# Use deterministic secret key so restarts do not invalidate browser sessions
app.secret_key = os.getenv("FLASK_SECRET_KEY", "midnight_sovereign_persistent_secret_key_8899")

load_dotenv()
UI_PASSWORD = os.getenv("UI_PASSWORD", "midnight123")
SOVEREIGN_AUTH_KEY = os.getenv("SOVEREIGN_AUTH_KEY", UI_PASSWORD)
LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
TOKENS_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "kite_tokens.json")

def extract_request_token(raw_input: str) -> str:
    """
    Extracts request_token whether user pastes the raw string or the full redirect URL:
    e.g. 'https://www.google.com/?action=login&type=login&status=success&request_token=abc123xyz'
    """
    if not raw_input:
        return ""
    raw_input = raw_input.strip()
    if "request_token=" in raw_input:
        try:
            parsed = urllib.parse.urlparse(raw_input)
            qs = urllib.parse.parse_qs(parsed.query)
            if "request_token" in qs:
                return qs["request_token"][0].strip()
        except Exception:
            pass
        match = re.search(r"request_token=([a-zA-Z0-9]+)", raw_input)
        if match:
            return match.group(1).strip()
    return raw_input

def update_env_file(key, value, filepath=".env"):
    if not os.path.exists(filepath):
        with open(filepath, "w") as f:
            f.write(f"{key}='{value}'\n")
        return

    with open(filepath, "r") as f:
        lines = f.readlines()
        
    updated = False
    with open(filepath, "w") as f:
        for line in lines:
            if line.startswith(f"{key}="):
                f.write(f"{key}='{value}'\n")
                updated = True
            else:
                f.write(line)
        if not updated:
            f.write(f"{key}='{value}'\n")

def save_persistent_tokens(tokens_dict: dict):
    os.makedirs(os.path.dirname(TOKENS_FILE), exist_ok=True)
    existing = {}
    if os.path.exists(TOKENS_FILE):
        try:
            with open(TOKENS_FILE, "r") as f:
                existing = json.load(f)
        except Exception:
            existing = {}
    existing.update(tokens_dict)
    existing["updated_at"] = datetime.now().isoformat()
    with open(TOKENS_FILE, "w") as f:
        json.dump(existing, f, indent=2)

def load_persistent_tokens() -> dict:
    if os.path.exists(TOKENS_FILE):
        try:
            with open(TOKENS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def get_log_files():
    if not os.path.exists(LOGS_DIR):
        return []
    return sorted([f for f in os.listdir(LOGS_DIR) if f.endswith(".log")])

def parse_cron():
    cron_file = "current_cron.txt"
    if not os.path.exists(cron_file):
        cron_file = "gcp_cron.txt"
    if not os.path.exists(cron_file):
        return []

    crons = []
    with open(cron_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(maxsplit=5)
            if len(parts) == 6:
                crons.append({
                    "time": " ".join(parts[:5]),
                    "cmd": parts[5]
                })
    return crons

# Premium HTML Template
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Midnight Sovereign Command Center</title>
    <style>
        :root {
            --bg-color: #0b0f19;
            --surface-color: rgba(22, 27, 34, 0.7);
            --border-color: rgba(48, 54, 61, 0.8);
            --accent-color: #58a6ff;
            --success-color: #3fb950;
            --danger-color: #ff7b72;
            --text-color: #c9d1d9;
            --text-muted: #8b949e;
        }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; 
            background-color: var(--bg-color); 
            background-image: radial-gradient(circle at top right, rgba(88, 166, 255, 0.1), transparent 40%),
                              radial-gradient(circle at bottom left, rgba(63, 185, 80, 0.05), transparent 40%);
            color: var(--text-color); 
            margin: 0; 
            padding: 20px; 
            display: flex; 
            flex-direction: column; 
            align-items: center; 
            min-height: 100vh;
        }
        .container { 
            max-width: 860px; 
            width: 100%; 
            background: var(--surface-color); 
            backdrop-filter: blur(16px);
            -webkit-backdrop-filter: blur(16px);
            padding: 30px; 
            border-radius: 12px; 
            border: 1px solid var(--border-color); 
            box-shadow: 0 8px 32px rgba(0,0,0,0.4); 
            margin-bottom: 20px;
        }
        h2 { color: var(--accent-color); text-align: center; margin-top: 0; font-weight: 700; letter-spacing: -0.5px; }
        
        .nav-tabs {
            display: flex;
            border-bottom: 1px solid var(--border-color);
            margin-bottom: 25px;
            gap: 10px;
        }
        .nav-tab {
            padding: 10px 20px;
            cursor: pointer;
            color: var(--text-muted);
            border-bottom: 2px solid transparent;
            transition: all 0.2s ease;
            font-weight: 600;
        }
        .nav-tab:hover { color: var(--text-color); }
        .nav-tab.active {
            color: var(--accent-color);
            border-bottom: 2px solid var(--accent-color);
        }
        .tab-content { display: none; }
        .tab-content.active { display: block; animation: fadeIn 0.3s ease; }
        
        @keyframes fadeIn { from { opacity: 0; transform: translateY(5px); } to { opacity: 1; transform: translateY(0); } }

        label { display: block; margin-bottom: 8px; font-weight: 600; font-size: 14px; }
        .hint { font-size: 12px; color: var(--text-muted); margin-bottom: 8px; }
        input[type="text"], input[type="password"], select { 
            width: 100%; padding: 12px; margin-bottom: 20px; 
            border: 1px solid var(--border-color); 
            border-radius: 8px; 
            background-color: rgba(13, 17, 23, 0.8); 
            color: var(--text-color); 
            box-sizing: border-box;
            transition: border-color 0.2s;
        }
        input:focus, select:focus { outline: none; border-color: var(--accent-color); box-shadow: 0 0 0 3px rgba(88, 166, 255, 0.2); }
        button { 
            width: 100%; padding: 14px; 
            background: linear-gradient(180deg, #2ea043 0%, #238636 100%);
            color: #fff; border: 1px solid rgba(240, 246, 252, 0.1); 
            border-radius: 8px; font-size: 16px; font-weight: 600; 
            cursor: pointer; transition: all 0.2s; 
            box-shadow: 0 2px 4px rgba(0,0,0,0.2);
        }
        button:hover { filter: brightness(1.1); }
        
        .alert { padding: 14px; border-radius: 8px; margin-bottom: 20px; font-size: 14px; font-weight: 500; }
        .alert.success { background-color: rgba(35, 134, 54, 0.15); border: 1px solid rgba(46, 160, 67, 0.4); color: var(--success-color); }
        .alert.error { background-color: rgba(248, 81, 73, 0.1); border: 1px solid rgba(248, 81, 73, 0.4); color: var(--danger-color); }
        
        .badge { display: inline-block; padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; text-transform: uppercase; }
        .badge.active { background: rgba(63, 185, 80, 0.2); color: var(--success-color); border: 1px solid var(--success-color); }
        .badge.inactive { background: rgba(248, 81, 73, 0.2); color: var(--danger-color); border: 1px solid var(--danger-color); }

        .terminal {
            background-color: #010409;
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 15px;
            font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
            font-size: 13px;
            line-height: 1.5;
            height: 400px;
            overflow-y: auto;
            color: #8b949e;
            box-shadow: inset 0 2px 10px rgba(0,0,0,0.5);
            white-space: pre-wrap;
            word-wrap: break-word;
        }
        .terminal-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }
        .terminal-status {
            display: flex;
            align-items: center;
            gap: 6px;
            font-size: 12px;
            color: var(--success-color);
        }
        .status-dot {
            width: 8px; height: 8px;
            background-color: var(--success-color);
            border-radius: 50%;
            animation: pulse 2s infinite;
        }
        @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.4; } 100% { opacity: 1; } }

        .status-card {
            background: rgba(13, 17, 23, 0.6);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 16px;
            margin-bottom: 20px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
    </style>
</head>
<body>
    <div class="container">
        <h2>🏛️ Midnight Sovereign Command Center</h2>
        
        {% if message %}
            <div class="alert {% if success %}success{% else %}error{% endif %}">
                {{ message }}
            </div>
        {% endif %}

        {% if not session.get('authenticated') %}
            <form method="POST" action="/login">
                <label for="password">Authentication Required</label>
                <input type="password" id="password" name="password" required placeholder="Enter UI Password">
                <button type="submit">Unlock Console</button>
            </form>
        {% else %}
            
            <div class="nav-tabs">
                <div class="nav-tab active" onclick="switchTab('tokens')">Token Manager</div>
                <div class="nav-tab" onclick="switchTab('logs')">Live Telemetry</div>
                <div class="nav-tab" onclick="switchTab('schedule')">Schedule</div>
            </div>
            
            <!-- TOKENS TAB -->
            <div id="tab-tokens" class="tab-content active">
                <div class="status-card">
                    <div>
                        <div style="font-weight: bold; color: #ffffff;">Execution Gateway (Port 5000)</div>
                        <div style="font-size: 12px; color: var(--text-muted);">Status: Active | Policy: Lot Size &lt; 600 Only | 10s Verification: Armed</div>
                    </div>
                    <span class="badge active">OPERATIONAL</span>
                </div>

                <form method="POST" action="/generate">
                    <!-- Data Account -->
                    <div style="background: rgba(88, 166, 255, 0.05); border: 1px solid rgba(88, 166, 255, 0.2); border-radius: 8px; padding: 16px; margin-bottom: 20px;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; flex-wrap: wrap; gap: 10px;">
                            <div>
                                <span style="font-weight: 700; color: #58a6ff; font-size: 15px;">1. Main Data Account (YF4472)</span>
                                <div style="font-size: 12px; color: var(--text-muted); margin-top: 2px;">API Key: <code>anywvvfkcyjhhqiy</code></div>
                            </div>
                            <a href="https://kite.zerodha.com/connect/login?v=3&api_key=anywvvfkcyjhhqiy" target="zerodha_data_tab" rel="noopener noreferrer" style="display: inline-block; padding: 9px 18px; background: #1f6feb; color: #ffffff; text-decoration: none; border-radius: 6px; font-size: 13px; font-weight: 600; box-shadow: 0 2px 4px rgba(0,0,0,0.2);">
                                🔗 1. Open Zerodha Login (YF4472) ↗
                            </a>
                        </div>
                        <label for="main_token" style="font-size: 13px; color: var(--text-color); margin-bottom: 6px;">2. Paste Redirected URL or Request Token below:</label>
                        <input type="text" id="main_token" name="main_token" placeholder="Paste redirected URL (e.g. https://www.google.com/?...request_token=...)" style="margin-bottom: 0;">
                    </div>
                    
                    <!-- Exec Account -->
                    <div style="background: rgba(63, 185, 80, 0.05); border: 1px solid rgba(63, 185, 80, 0.2); border-radius: 8px; padding: 16px; margin-bottom: 25px;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; flex-wrap: wrap; gap: 10px;">
                            <div>
                                <span style="font-weight: 700; color: #3fb950; font-size: 15px;">2. Live Execution Account (HU0092)</span>
                                <div style="font-size: 12px; color: var(--text-muted); margin-top: 2px;">API Key: <code>lhrw3sc9uo6ofenq</code></div>
                            </div>
                            <a href="https://kite.zerodha.com/connect/login?v=3&api_key=lhrw3sc9uo6ofenq" target="zerodha_exec_tab" rel="noopener noreferrer" style="display: inline-block; padding: 9px 18px; background: #238636; color: #ffffff; text-decoration: none; border-radius: 6px; font-size: 13px; font-weight: 600; box-shadow: 0 2px 4px rgba(0,0,0,0.2);">
                                🔗 1. Open Zerodha Login (HU0092) ↗
                            </a>
                        </div>
                        <label for="exec_token" style="font-size: 13px; color: var(--text-color); margin-bottom: 6px;">2. Paste Redirected URL or Request Token below:</label>
                        <input type="text" id="exec_token" name="exec_token" placeholder="Paste redirected URL (e.g. https://www.google.com/?...request_token=...)" style="margin-bottom: 0;">
                    </div>
                    
                    <button type="submit">⚡ Step 3: Generate & Sync Tokens to Engine</button>
                </form>
            </div>

            <!-- LOGS TAB -->
            <div id="tab-logs" class="tab-content">
                <div class="terminal-header">
                    <select id="log_selector" onchange="fetchLogs()">
                        <option value="">Select Log Stream...</option>
                        {% for log_file in log_files %}
                            <option value="{{ log_file }}">{{ log_file }}</option>
                        {% endfor %}
                    </select>
                    <div class="terminal-status"><div class="status-dot"></div> Live</div>
                </div>
                <div id="terminal_output" class="terminal">Select a log file to stream telemetry...</div>
            </div>

            <!-- SCHEDULE TAB -->
            <div id="tab-schedule" class="tab-content">
                <ul style="list-style: none; padding: 0;">
                    {% for cron in crons %}
                        <li style="background: rgba(13, 17, 23, 0.5); border: 1px solid var(--border-color); border-radius: 8px; padding: 12px; margin-bottom: 8px;">
                            <span style="color: var(--accent-color); font-family: monospace; font-weight: bold;">{{ cron.time }}</span>
                            <div style="color: var(--text-color); font-size: 13px; margin-top: 4px; word-break: break-all;">{{ cron.cmd }}</div>
                        </li>
                    {% else %}
                        <li>No active crons found.</li>
                    {% endfor %}
                </ul>
            </div>

            <script>
                function switchTab(tabId) {
                    document.querySelectorAll('.nav-tab').forEach(t => t.classList.remove('active'));
                    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
                    
                    event.target.classList.add('active');
                    document.getElementById('tab-' + tabId).classList.add('active');
                }

                let logInterval = null;
                function fetchLogs() {
                    if(logInterval) clearInterval(logInterval);
                    updateLog();
                    logInterval = setInterval(updateLog, 2000);
                }

                function updateLog() {
                    const logFile = document.getElementById('log_selector').value;
                    const terminal = document.getElementById('terminal_output');
                    if (!logFile) return;

                    fetch('/api/logs/' + logFile)
                        .then(response => response.text())
                        .then(text => {
                            const isScrolledToBottom = terminal.scrollHeight - terminal.clientHeight <= terminal.scrollTop + 10;
                            terminal.textContent = text || "Waiting for data...";
                            if (isScrolledToBottom) {
                                terminal.scrollTop = terminal.scrollHeight;
                            }
                        })
                        .catch(err => {
                            terminal.textContent = "Error fetching logs: " + err;
                        });
                }
            </script>
        {% endif %}
    </div>
</body>
</html>
"""

# =============================================================================
# Web Routes
# =============================================================================

@app.route("/", methods=["GET"])
def index():
    log_files = get_log_files()
    crons = parse_cron()
    return render_template_string(HTML_TEMPLATE, log_files=log_files, crons=crons)

@app.route("/login", methods=["POST"])
def login():
    password = request.form.get("password", "")
    if password == UI_PASSWORD:
        session['authenticated'] = True
        return redirect(url_for('index'))
    else:
        return render_template_string(HTML_TEMPLATE, message="Invalid password.", success=False)

@app.route("/api/logs/<filename>", methods=["GET"])
def get_logs(filename):
    if not session.get('authenticated'):
        return "Unauthorized", 401
    
    filepath = os.path.join(LOGS_DIR, filename)
    if not os.path.exists(filepath):
        return f"File {filename} not found."
    
    try:
        with open(filepath, "r") as f:
            lines = f.readlines()
            return "".join(lines[-150:])
    except Exception as e:
        return str(e)

@app.route("/generate", methods=["POST"])
def generate():
    if not session.get('authenticated'):
        return redirect(url_for('index'))

    load_dotenv() 
    
    raw_main = request.form.get("main_token", "").strip()
    raw_exec = request.form.get("exec_token", "").strip()
    
    main_request_token = extract_request_token(raw_main)
    exec_request_token = extract_request_token(raw_exec)
    
    main_api_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    main_api_secret = os.getenv("KITE_API_SECRET", "").strip("'\"")
    
    exec_api_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
    exec_api_secret = os.getenv("EXEC_KITE_API_SECRET", "").strip("'\"")
    
    tokens_to_save = {}
    success_msgs = []

    try:
        if main_request_token:
            kite_main = KiteConnect(api_key=main_api_key)
            data_main = kite_main.generate_session(main_request_token, api_secret=main_api_secret)
            acc_token = data_main["access_token"]
            update_env_file("KITE_ACCESS_TOKEN", acc_token)
            tokens_to_save["KITE_ACCESS_TOKEN"] = acc_token
            success_msgs.append("Data Account (YF4472) Refreshed")
            
        if exec_request_token:
            kite_exec = KiteConnect(api_key=exec_api_key)
            data_exec = kite_exec.generate_session(exec_request_token, api_secret=exec_api_secret)
            acc_exec = data_exec["access_token"]
            update_env_file("EXEC_KITE_ACCESS_TOKEN", acc_exec)
            tokens_to_save["EXEC_KITE_ACCESS_TOKEN"] = acc_exec
            success_msgs.append("Execution Account (HU0092) Refreshed")
        
        if tokens_to_save:
            save_persistent_tokens(tokens_to_save)

        msg = f"Successfully generated tokens: {', '.join(success_msgs)}" if success_msgs else "No tokens provided."
        return render_template_string(HTML_TEMPLATE, message=msg, success=bool(success_msgs), log_files=get_log_files(), crons=parse_cron())
        
    except Exception as e:
        return render_template_string(HTML_TEMPLATE, message=f"Error generating session: {str(e)}", success=False, log_files=get_log_files(), crons=parse_cron())

# =============================================================================
# REST API Endpoints (Laptop -> GCP Order Gateway)
# =============================================================================

def is_authorized(req) -> bool:
    """Verifies X-Sovereign-Auth header, Bearer token, or auth_key/token query parameter."""
    auth_header = req.headers.get("X-Sovereign-Auth")
    auth_bearer = req.headers.get("Authorization", "").replace("Bearer ", "").strip()
    auth_param = req.args.get("auth_key") or req.args.get("token")
    token = auth_header or auth_bearer or auth_param
    return token in (SOVEREIGN_AUTH_KEY, UI_PASSWORD)

@app.route("/api/tokens/data", methods=["GET"])
def get_data_token_endpoint():
    """Returns only the Data Account (YF4472) access token."""
    if not is_authorized(request):
        return jsonify({"error": "Unauthorized"}), 401

    load_dotenv()
    persistent = load_persistent_tokens()
    token = persistent.get("KITE_ACCESS_TOKEN") or os.getenv("KITE_ACCESS_TOKEN", "")
    return jsonify({
        "account": "YF4472",
        "api_key": os.getenv("KITE_API_KEY", "").strip("'\""),
        "access_token": token,
        "updated_at": persistent.get("updated_at", datetime.now().isoformat())
    }), 200

@app.route("/api/tokens/sync", methods=["GET"])
def sync_tokens():
    """Allows laptop crons to sync active tokens from GCP."""
    if not is_authorized(request):
        return jsonify({"error": "Unauthorized"}), 401

    load_dotenv()
    persistent = load_persistent_tokens()
    result = {
        "KITE_ACCESS_TOKEN": persistent.get("KITE_ACCESS_TOKEN") or os.getenv("KITE_ACCESS_TOKEN", ""),
        "EXEC_KITE_ACCESS_TOKEN": persistent.get("EXEC_KITE_ACCESS_TOKEN") or os.getenv("EXEC_KITE_ACCESS_TOKEN", ""),
        "updated_at": persistent.get("updated_at", datetime.now().isoformat())
    }
    return jsonify(result), 200

@app.route("/api/orders/execute", methods=["POST"])
def execute_order_endpoint():
    """
    Automated Order Execution Gateway Endpoint:
    Receives order requests from laptop crons, validates lot size < 600,
    checks available cash, places limit order, checks fill in 10s, sets SL.
    """
    if not is_authorized(request):
        return jsonify({"error": "Unauthorized: Invalid Sovereign Auth Header"}), 401

    data = request.get_json(force=True, silent=True) or {}
    symbol = data.get("symbol")
    option_symbol = data.get("option_symbol")
    lot_size = int(data.get("lot_size", 0))
    entry_premium = float(data.get("entry_premium", 0.0))

    if not symbol or not option_symbol or lot_size <= 0 or entry_premium <= 0:
        return jsonify({
            "success": False,
            "error": "INVALID_PAYLOAD",
            "message": "Required fields: symbol, option_symbol, lot_size > 0, entry_premium > 0"
        }), 400

    gateway = SovereignExecutionGateway()
    result = gateway.execute_option_order(
        symbol=symbol,
        option_symbol=option_symbol,
        lot_size=lot_size,
        entry_premium=entry_premium,
        direction=data.get("direction", "LONG"),
        instrument_type=data.get("instrument_type", "CE"),
        best_ask=data.get("best_ask"),
        catalyst=data.get("catalyst", ""),
        score=float(data.get("score", 85.0)),
        holding_type=data.get("holding_type", "TYPE_C_BTST")
    )

    status_code = 200 if result.get("success") else 422
    return jsonify(result), status_code

@app.route("/api/positions", methods=["GET"])
def get_positions_endpoint():
    """Returns live open positions on the execution account."""
    if not is_authorized(request):
        return jsonify({"error": "Unauthorized"}), 401

    engine = SmartVolumeReversalEngine()
    positions = engine.get_live_open_positions()
    return jsonify({"positions": positions, "count": len(positions)}), 200

@app.route("/api/guardian/run_20m", methods=["POST"])
def run_20m_guardian_endpoint():
    """Triggers a 20-minute Volume Reversal cycle on demand."""
    if not is_authorized(request):
        return jsonify({"error": "Unauthorized"}), 401

    engine = SmartVolumeReversalEngine()
    results = engine.run_20m_cycle()
    return jsonify({"status": "SUCCESS", "results": results}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
