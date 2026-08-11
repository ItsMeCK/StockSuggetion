import os
from flask import Flask, request, render_template_string, redirect, url_for, session, jsonify
from dotenv import load_dotenv
from kiteconnect import KiteConnect

app = Flask(__name__)
app.secret_key = os.urandom(24)

load_dotenv()
UI_PASSWORD = os.getenv("UI_PASSWORD", "midnight123")
LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")

# Premium HTML Template with CSS/JS embedded
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
            max-width: 800px; 
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
        
        /* Navigation Tabs */
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

        /* Forms */
        label { display: block; margin-bottom: 8px; font-weight: 600; font-size: 14px; }
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
        
        /* Alerts */
        .alert { padding: 14px; border-radius: 8px; margin-bottom: 20px; font-size: 14px; font-weight: 500; }
        .alert.success { background-color: rgba(35, 134, 54, 0.15); border: 1px solid rgba(46, 160, 67, 0.4); color: var(--success-color); }
        .alert.error { background-color: rgba(248, 81, 73, 0.1); border: 1px solid rgba(248, 81, 73, 0.4); color: #ff7b72; }
        
        /* Terminal / Logs */
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

        /* Schedule Table */
        .schedule-list {
            list-style: none;
            padding: 0;
            margin: 0;
        }
        .schedule-item {
            background: rgba(13, 17, 23, 0.5);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 10px;
            display: flex;
            flex-direction: column;
            gap: 5px;
        }
        .schedule-cron { color: var(--accent-color); font-family: monospace; font-weight: bold; }
        .schedule-cmd { color: var(--text-color); font-size: 14px; word-break: break-all; }
    </style>
</head>
<body>
    <div class="container">
        <h2>Midnight Sovereign</h2>
        
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
                <div class="nav-tab active" onclick="switchTab('logs')">Live Telemetry</div>
                <div class="nav-tab" onclick="switchTab('schedule')">Schedule</div>
                <div class="nav-tab" onclick="switchTab('tokens')">Tokens</div>
            </div>
            
            <!-- LOGS TAB -->
            <div id="tab-logs" class="tab-content active">
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
                <ul class="schedule-list">
                    {% for cron in crons %}
                        <li class="schedule-item">
                            <span class="schedule-cron">{{ cron.time }}</span>
                            <span class="schedule-cmd">{{ cron.cmd }}</span>
                        </li>
                    {% else %}
                        <li class="schedule-item">No active crons found.</li>
                    {% endfor %}
                </ul>
            </div>

            <!-- TOKENS TAB -->
            <div id="tab-tokens" class="tab-content">
                <form method="POST" action="/generate">
                    <label for="main_token">Main Account Request Token</label>
                    <input type="text" id="main_token" name="main_token" required placeholder="Paste Main Request Token">
                    
                    <label for="exec_token">Execution Account Request Token</label>
                    <input type="text" id="exec_token" name="exec_token" required placeholder="Paste Execution Request Token">
                    
                    <button type="submit">Generate & Sync to .env</button>
                </form>
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
                    logInterval = setInterval(updateLog, 2000); // Fetch every 2s
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
        # Read the last 150 lines safely
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
    
    main_request_token = request.form.get("main_token", "").strip()
    exec_request_token = request.form.get("exec_token", "").strip()
    
    main_api_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    main_api_secret = os.getenv("KITE_API_SECRET", "").strip("'\"")
    
    exec_api_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
    exec_api_secret = os.getenv("EXEC_KITE_API_SECRET", "").strip("'\"")
    
    try:
        if main_request_token:
            kite_main = KiteConnect(api_key=main_api_key)
            data_main = kite_main.generate_session(main_request_token, api_secret=main_api_secret)
            update_env_file("KITE_ACCESS_TOKEN", data_main["access_token"])
            
        if exec_request_token:
            kite_exec = KiteConnect(api_key=exec_api_key)
            data_exec = kite_exec.generate_session(exec_request_token, api_secret=exec_api_secret)
            update_env_file("EXEC_KITE_ACCESS_TOKEN", data_exec["access_token"])
        
        msg = "Successfully generated and saved Access Tokens!"
        return render_template_string(HTML_TEMPLATE, message=msg, success=True, log_files=get_log_files(), crons=parse_cron())
        
    except Exception as e:
        return render_template_string(HTML_TEMPLATE, message=f"Error: {str(e)}", success=False, log_files=get_log_files(), crons=parse_cron())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
