import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import logging

load_dotenv()

class SovereignEmailer:
    """
    The Institutional Communication Layer.
    Sends rich HTML scorecards via SMTP.
    """
    def __init__(self):
        self.server = os.getenv("MAIL_SERVER")
        self.port = int(os.getenv("MAIL_PORT", 587))
        self.username = os.getenv("MAIL_USERNAME")
        self.password = os.getenv("MAIL_PASSWORD").replace(" ", "")
        self.target = os.getenv("TARGET_EMAIL")

    def send_scorecard(self, subject: str, signals: list):
        """
        Signals is a list of dicts from the Librarian.
        Sorted by score descending.
        """
        if not signals:
            return

        # Sort signals by score descending and limit to Top 5
        signals = sorted(signals, key=lambda x: x['score'], reverse=True)[:5]
        
        logging.info(f"📤 SENDING EMAIL: {subject} | TICKERS: {[s['ticker'] for s in signals]}")

        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"🏛️ SOVEREIGN: {subject}"
        msg["From"] = f"Sovereign Engine <{self.username}>"
        msg["To"] = self.target

        # Create HTML Content
        html = f"""
        <html>
        <body style="font-family: 'Inter', 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #0b0e14; padding: 20px; color: #e1e8ed;">
            <div style="max-width: 650px; margin: auto; background: #151921; padding: 40px; border-radius: 16px; border: 1px solid #2d333b; box-shadow: 0 10px 30px rgba(0,0,0,0.5);">
                <div style="text-align: center; border-bottom: 1px solid #2d333b; padding-bottom: 20px; margin-bottom: 30px;">
                    <h1 style="color: #ffffff; font-size: 28px; margin: 0; letter-spacing: 1px;">SOVEREIGN INTELLIGENCE</h1>
                    <p style="color: #8b949e; font-size: 14px; margin-top: 5px;">Pring Codex v4.0 Institutional Audit</p>
                </div>
                
                <div style="background: #1f242d; border-radius: 8px; padding: 15px; margin-bottom: 30px; border: 1px solid #30363d;">
                    <p style="margin: 0; font-size: 13px; color: #58a6ff; font-weight: bold; text-transform: uppercase;">Executive Summary</p>
                    <p style="margin: 5px 0 0 0; color: #c9d1d9; font-size: 15px;">
                        The Librarian has identified <b>{len(signals)}</b> high-conviction targets. 
                        {"Priority: Execute Platinum signals at the 12:00 PM Strike." if "12:00" in subject else "Focus: Accumulate Incubators for tomorrow."}
                    </p>
                </div>
        """

        for s in signals:
            # Determine Tier Styling
            if s['score'] >= 90:
                tier_label = "🔱 PLATINUM CHAMPION"
                border_color = "#58a6ff"
                bg_color = "rgba(88, 166, 255, 0.05)"
            elif s['score'] >= 80:
                tier_label = "🏅 GOLD MOMENTUM"
                border_color = "#3fb950"
                bg_color = "rgba(63, 185, 80, 0.05)"
            else:
                tier_label = "🥈 SILVER WATCHLIST"
                border_color = "#8b949e"
                bg_color = "rgba(139, 148, 158, 0.05)"

            html += f"""
                <div style="margin-bottom: 25px; padding: 25px; border-left: 5px solid {border_color}; background: {bg_color}; border-radius: 6px; border-top: 1px solid #30363d; border-right: 1px solid #30363d; border-bottom: 1px solid #30363d;">
                    <table width="100%">
                        <tr>
                            <td>
                                <h2 style="margin: 0; color: #ffffff; font-size: 22px; letter-spacing: 0.5px;">{s['ticker']}</h2>
                                <span style="font-size: 11px; font-weight: bold; color: {border_color}; text-transform: uppercase;">{tier_label}</span>
                            </td>
                            <td style="text-align: right;">
                                <div style="font-size: 32px; font-weight: bold; color: {border_color};">{s['score']}</div>
                                <div style="font-size: 10px; color: #8b949e; text-transform: uppercase;">Pring Score</div>
                            </td>
                        </tr>
                    </table>
                    
                    <div style="margin-top: 20px; border-top: 1px solid #30363d; padding-top: 15px;">
                        <p style="margin: 5px 0; color: #c9d1d9; font-size: 13px;"><b>✅ COMPLIANCE:</b> {", ".join(s['passed'][:5])}...</p>
                        <p style="margin: 5px 0; color: #8b949e; font-size: 12px;"><b>Institutional Logic:</b> Setups verified against Pring Chapters 6, 11, and 22.</p>
                    </div>
                </div>
            """

        html += """
                <div style="text-align: center; margin-top: 40px; border-top: 1px solid #2d333b; padding-top: 20px;">
                    <p style="font-size: 11px; color: #484f58; margin: 0;">
                        CONFIDENTIAL INSTITUTIONAL RESEARCH<br>
                        This report is generated using real-time TimescaleDB OHLCV data.
                    </p>
                </div>
            </div>
        </body>
        </html>
        """

        part = MIMEText(html, "html")
        msg.attach(part)

        try:
            with smtplib.SMTP(self.server, self.port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.sendmail(self.username, self.target, msg.as_string())
                logging.info(f"Email Signal Report sent successfully to {self.target}")
        except Exception as e:
            logging.error(f"Failed to send email: {e}")

if __name__ == "__main__":
    # Test Email
    emailer = SovereignEmailer()
    test_signals = [{
        "ticker": "ZYDUSWELL",
        "score": 95,
        "status": "SIGNALED",
        "passed": ["PRING_CH6_ACCEL", "CHAMPIONS_CLAUSE", "STAGE_2"],
        "failed": []
    }]
    emailer.send_scorecard("Wednesday 12:00 PM Strike Test", test_signals)
