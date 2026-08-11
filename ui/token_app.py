import os
from flask import Flask, request, render_template_string, redirect, url_for, session
from dotenv import load_dotenv
from kiteconnect import KiteConnect

app = Flask(__name__)
# Used for securely signing the session cookie
app.secret_key = os.urandom(24)

# Load the password from .env, or use a default if not set
load_dotenv()
UI_PASSWORD = os.getenv("UI_PASSWORD", "midnight123")

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Midnight Sovereign - Token Refresher</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: #0d1117; color: #c9d1d9; margin: 0; padding: 20px; display: flex; flex-direction: column; align-items: center; }
        .container { max-width: 400px; width: 100%; background: #161b22; padding: 30px; border-radius: 8px; border: 1px solid #30363d; box-shadow: 0 4px 12px rgba(0,0,0,0.5); }
        h2 { color: #58a6ff; text-align: center; margin-top: 0; }
        label { display: block; margin-bottom: 8px; font-weight: 600; font-size: 14px; }
        input[type="text"], input[type="password"] { width: 100%; padding: 10px; margin-bottom: 20px; border: 1px solid #30363d; border-radius: 6px; background-color: #0d1117; color: #c9d1d9; box-sizing: border-box; }
        input[type="text"]:focus, input[type="password"]:focus { outline: none; border-color: #58a6ff; }
        button { width: 100%; padding: 12px; background-color: #238636; color: #fff; border: none; border-radius: 6px; font-size: 16px; font-weight: 600; cursor: pointer; transition: background-color 0.2s; }
        button:hover { background-color: #2ea043; }
        .alert { padding: 12px; border-radius: 6px; margin-bottom: 20px; font-size: 14px; }
        .alert.success { background-color: rgba(35, 134, 54, 0.15); border: 1px solid rgba(46, 160, 67, 0.4); color: #3fb950; }
        .alert.error { background-color: rgba(248, 81, 73, 0.1); border: 1px solid rgba(248, 81, 73, 0.4); color: #ff7b72; }
    </style>
</head>
<body>
    <div class="container">
        <h2>Token Refresher</h2>
        
        {% if message %}
            <div class="alert {% if success %}success{% else %}error{% endif %}">
                {{ message }}
            </div>
        {% endif %}

        {% if not session.get('authenticated') %}
            <form method="POST" action="/login">
                <label for="password">Password</label>
                <input type="password" id="password" name="password" required placeholder="Enter UI Password">
                <button type="submit">Login</button>
            </form>
        {% else %}
            <form method="POST" action="/generate">
                <label for="main_token">Main Account Request Token</label>
                <input type="text" id="main_token" name="main_token" required placeholder="Paste Main Request Token">
                
                <label for="exec_token">Execution Account Request Token</label>
                <input type="text" id="exec_token" name="exec_token" required placeholder="Paste Execution Request Token">
                
                <button type="submit">Generate & Sync to .env</button>
            </form>
        {% endif %}
    </div>
</body>
</html>
"""

def update_env_file(key, value, filepath=".env"):
    if not os.path.exists(filepath):
        # Create it if it doesn't exist
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

@app.route("/", methods=["GET"])
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route("/login", methods=["POST"])
def login():
    password = request.form.get("password", "")
    if password == UI_PASSWORD:
        session['authenticated'] = True
        return redirect(url_for('index'))
    else:
        return render_template_string(HTML_TEMPLATE, message="Invalid password.", success=False)

@app.route("/generate", methods=["POST"])
def generate():
    if not session.get('authenticated'):
        return redirect(url_for('index'))

    load_dotenv() # Reload just in case
    
    main_request_token = request.form.get("main_token", "").strip()
    exec_request_token = request.form.get("exec_token", "").strip()
    
    main_api_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    main_api_secret = os.getenv("KITE_API_SECRET", "").strip("'\"")
    
    exec_api_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
    exec_api_secret = os.getenv("EXEC_KITE_API_SECRET", "").strip("'\"")
    
    try:
        # Generate Main Token
        kite_main = KiteConnect(api_key=main_api_key)
        data_main = kite_main.generate_session(main_request_token, api_secret=main_api_secret)
        main_access_token = data_main["access_token"]
        
        # Generate Execution Token
        kite_exec = KiteConnect(api_key=exec_api_key)
        data_exec = kite_exec.generate_session(exec_request_token, api_secret=exec_api_secret)
        exec_access_token = data_exec["access_token"]
        
        # Update .env
        update_env_file("KITE_ACCESS_TOKEN", main_access_token)
        update_env_file("EXEC_KITE_ACCESS_TOKEN", exec_access_token)
        
        msg = "Successfully generated and saved both Access Tokens to .env!"
        return render_template_string(HTML_TEMPLATE, message=msg, success=True)
        
    except Exception as e:
        return render_template_string(HTML_TEMPLATE, message=f"Error generating tokens: {str(e)}", success=False)

if __name__ == "__main__":
    # Ensure it listens on all interfaces (0.0.0.0) so it's accessible externally
    app.run(host="0.0.0.0", port=5000)
