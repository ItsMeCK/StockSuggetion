import os
import sys
import smtplib
from datetime import date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

sys.path.append(os.getcwd())

load_dotenv(override=True)

from kiteconnect import KiteConnect

def send_email(subject, html_content):
    username = os.getenv("MAIL_USERNAME")
    password = os.getenv("MAIL_PASSWORD", "").replace(" ", "")
    server_addr = os.getenv("MAIL_SERVER", "smtp.gmail.com")
    port = int(os.getenv("MAIL_PORT", 587))
    target = "chandrakant7892@gmail.com"
    
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"🏛️ SOVEREIGN: {subject}"
    msg["From"] = f"Sovereign Engine <{username}>"
    msg["To"] = target
    msg.attach(MIMEText(html_content, "html"))
    
    try:
        server = smtplib.SMTP(server_addr, port)
        server.starttls()
        server.login(username, password)
        server.sendmail(username, target, msg.as_string())
        server.close()
        print("Email sent successfully!")
    except Exception as e:
        print("Failed to send email:", e)

def main():
    token = os.environ.get('KITE_ACCESS_TOKEN').strip('\'\"')
    kite = KiteConnect(api_key=os.getenv('KITE_API_KEY', 'anywvvfkcyjhhqiy'))
    kite.set_access_token(token)
    
    tickers = ['NSE:NIFTY 50', 'NSE:FORTIS', 'NSE:RBLBANK', 'NSE:IDFCFIRSTB',
               'NFO:FORTIS26JUL970CE', 'NFO:RBLBANK26JUL380CE', 'NFO:IDFCFIRSTB26JUL80CE']
    
    try:
        quotes = kite.quote(tickers)
    except Exception as e:
        print(f"Error fetching quotes: {e}")
        send_email("Error Fetching Intraday Quotes", f"<h3>Error: {e}</h3><p>Could not connect to Kite API.</p>")
        return
        
    nifty = quotes.get('NSE:NIFTY 50', {})
    nifty_close = nifty.get('ohlc', {}).get('close', 24206.9)
    nifty_ltp = nifty.get('last_price', 0.0)
    nifty_change = (nifty_ltp - nifty_close) / nifty_close * 100
    
    candidates = [
        {
            'symbol': 'FORTIS',
            'stock_ticker': 'NSE:FORTIS',
            'option_ticker': 'NFO:FORTIS26JUL970CE',
            'friday_close': 968.0,
            'suggested_premium': 20.30
        },
        {
            'symbol': 'RBLBANK',
            'stock_ticker': 'NSE:RBLBANK',
            'option_ticker': 'NFO:RBLBANK26JUL380CE',
            'friday_close': 380.65,
            'suggested_premium': 11.70
        },
        {
            'symbol': 'IDFCFIRSTB',
            'stock_ticker': 'NSE:IDFCFIRSTB',
            'option_ticker': 'NFO:IDFCFIRSTB26JUL80CE',
            'friday_close': 80.83,
            'suggested_premium': 2.59
        }
    ]
    
    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; background-color: #0b0e14; color: #e1e8ed; padding: 20px;">
        <div style="max-width: 600px; margin: auto; background: #151921; padding: 30px; border-radius: 12px; border: 1px solid #2d333b;">
            <h2 style="color: #ffffff; text-align: center; border-bottom: 1px solid #2d333b; padding-bottom: 15px;">10:30 AM INTRADAY ACTION ALERT</h2>
            
            <div style="background: #1f242d; border-radius: 8px; padding: 15px; margin: 20px 0; border: 1px solid #30363d;">
                <p style="margin: 0; color: #8b949e; font-size: 13px; text-transform: uppercase;"><b>Nifty 50 Status</b></p>
                <p style="margin: 5px 0 0 0; font-size: 18px; font-weight: bold; color: {'#3fb950' if nifty_change >= 0 else '#f85149'}">
                    LTP: {nifty_ltp:.2f} ({nifty_change:+.2f}%)
                </p>
            </div>
            
            <h3 style="color: #ffffff;">Breakout Target Details:</h3>
    """
    
    triggers_fired = []
    
    for c in candidates:
        st_data = quotes.get(c['stock_ticker'], {})
        opt_data = quotes.get(c['option_ticker'], {})
        
        st_ltp = st_data.get('last_price', 0.0)
        opt_ltp = opt_data.get('last_price', 0.0)
        
        st_change = (st_ltp - c['friday_close']) / c['friday_close'] * 100
        
        # Re-entry condition: stock LTP >= Friday Close
        condition_met = st_ltp >= c['friday_close']
        
        status_label = "🟢 ACTION: BUY" if condition_met else "🔴 ACTION: SKIP"
        status_color = "#3fb950" if condition_met else "#f85149"
        bg_color = "rgba(63, 185, 80, 0.05)" if condition_met else "rgba(248, 81, 73, 0.05)"
        
        if condition_met:
            triggers_fired.append(c['symbol'])
            
        html += f"""
            <div style="margin-bottom: 20px; padding: 20px; border-left: 5px solid {status_color}; background: {bg_color}; border-radius: 6px; border: 1px solid #30363d;">
                <table width="100%">
                    <tr>
                        <td>
                            <h3 style="margin: 0; color: #ffffff;">{c['symbol']}</h3>
                            <span style="font-size: 11px; font-weight: bold; color: {status_color};">{status_label}</span>
                        </td>
                        <td style="text-align: right;">
                            <div style="font-size: 14px; color: #8b949e;">Stock Price: <b>₹{st_ltp:.2f} ({st_change:+.2f}%)</b></div>
                            <div style="font-size: 12px; color: #8b949e;">Friday Close: ₹{c['friday_close']:.2f}</div>
                        </td>
                    </tr>
                </table>
                <div style="margin-top: 15px; border-top: 1px solid #30363d; padding-top: 10px; font-size: 13px;">
                    <p style="margin: 3px 0;"><b>Option Contract:</b> {c['option_ticker'].split(':', 1)[1]}</p>
                    <p style="margin: 3px 0;"><b>Current Premium LTP:</b> ₹{opt_ltp:.2f}</p>
                    <p style="margin: 3px 0;"><b>Suggested Entry Limit:</b> ₹{c['suggested_premium']:.2f}</p>
                    <p style="margin: 3px 0; color: #8b949e;"><b>Stop-Loss (GTT):</b> ₹{(c['suggested_premium']*0.6):.2f}</p>
                </div>
            </div>
        """
        
    html += """
        </div>
    </body>
    </html>
    """
    
    subject = "NO ACTIONS TODAY"
    if triggers_fired:
        subject = f"BUY TRIGGER: {', '.join(triggers_fired)}"
    else:
        subject = "SKIP ALL (No Recovery Confirmed)"
        
    send_email(subject, html)

if __name__ == "__main__":
    main()
