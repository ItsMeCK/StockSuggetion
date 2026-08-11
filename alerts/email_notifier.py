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

    def send_pulse_report(self, pulse_number: int, macro_regime: str, initial_candidates: list, approved_allocations: dict, critic_results: dict, total_screened: int = 0):
        """
        Sends a comprehensive, institutional-grade HTML report of the pulse execution.
        Includes all approved positions, critic vetoes, and engine rejections.
        """
        logging.info(f"📤 Preparing Sovereign Pulse #{pulse_number} Execution Report...")
        
        msg = MIMEMultipart("alternative")
        
        # Style indicator based on whether we placed live orders/allocations
        if approved_allocations:
            subject = f"Pulse #{pulse_number} Trade Signals (ACTIVE BUY)"
            header_color = "#3fb950" # Premium Green
            summary_badge = "🔥 ACTIVE ORDER EXECUTION"
            summary_text = f"Midnight Sovereign has successfully ledgered and queued orders for <b>{len(approved_allocations)}</b> approved setups."
        else:
            subject = f"Pulse #{pulse_number} Trade Signals (No Positions)"
            header_color = "#f85149" # Premium Coral/Red
            summary_badge = "🛡️ SCREENING COMPLETE (RESTING)"
            summary_text = "The cognitive validation pipeline completed successfully. No candidates cleared the strict Elite hybrid conviction gates today."

        msg["Subject"] = f"🏛️ SOVEREIGN: {subject}"
        msg["From"] = f"Sovereign Engine <{self.username}>"
        msg["To"] = self.target

        # Build approved setups HTML block
        approved_html = ""
        if approved_allocations:
            approved_html += f"""
            <div style="margin-top: 30px; margin-bottom: 30px;">
                <h3 style="color: #3fb950; font-size: 16px; text-transform: uppercase; margin-bottom: 15px; border-bottom: 1px solid #30363d; padding-bottom: 5px;">🔥 Approved Transactions</h3>
            """
            for symbol, alloc in approved_allocations.items():
                is_momentum = alloc.get("is_momentum", False)
                type_label = "🔱 TITAN BREAKOUT" if is_momentum else "🏅 GOLD ACCUMULATION"
                approved_html += f"""
                <div style="background: rgba(63, 185, 80, 0.04); border: 1px solid #3fb950; border-radius: 8px; padding: 20px; margin-bottom: 15px;">
                    <table width="100%">
                        <tr>
                            <td>
                                <span style="font-size: 11px; font-weight: bold; color: #3fb950; text-transform: uppercase; background: rgba(63, 185, 80, 0.1); padding: 3px 8px; border-radius: 4px; display: inline-block; margin-bottom: 8px;">{type_label}</span>
                                <h2 style="margin: 0; color: #ffffff; font-size: 24px;">{symbol}</h2>
                            </td>
                            <td style="text-align: right; vertical-align: top;">
                                <div style="font-size: 11px; color: #8b949e; text-transform: uppercase;">Allocated Capital</div>
                                <div style="font-size: 20px; font-weight: bold; color: #3fb950;">₹{alloc.get('capital_allocated', 0.0):,.0f}</div>
                            </td>
                        </tr>
                    </table>
                    <div style="margin-top: 15px; border-top: 1px solid #30363d; padding-top: 15px;">
                        <table width="100%" style="font-size: 13px; color: #c9d1d9;">
                            <tr>
                                <td style="color: #8b949e; padding: 4px 0;">Shares Queue:</td>
                                <td style="font-weight: bold; text-align: right; padding: 4px 0;">{alloc.get('shares', 0)} shares</td>
                            </tr>
                            <tr>
                                <td style="color: #8b949e; padding: 4px 0;">Limit Entry Price:</td>
                                <td style="font-weight: bold; text-align: right; color: #58a6ff; padding: 4px 0;">₹{alloc.get('entry', 0.0):,.2f}</td>
                            </tr>
                            <tr>
                                <td style="color: #8b949e; padding: 4px 0;">Stop-Loss:</td>
                                <td style="font-weight: bold; text-align: right; color: #f85149; padding: 4px 0;">₹{alloc.get('stop_loss', 0.0):,.2f}</td>
                            </tr>
                            <tr>
                                <td style="color: #8b949e; padding: 4px 0;">Conviction Score:</td>
                                <td style="font-weight: bold; text-align: right; color: #ffca28; padding: 4px 0;">{alloc.get('conviction_score', 0.0):,.1f} / 100</td>
                            </tr>
                        </table>
                    </div>
                </div>
                """
            approved_html += "</div>"
        else:
            approved_html += f"""
            <div style="margin-top: 30px; margin-bottom: 30px; background: rgba(248, 81, 73, 0.02); border: 1px dashed #30363d; border-radius: 8px; padding: 30px; text-align: center;">
                <p style="color: #8b949e; font-size: 15px; margin: 0;">No buy transactions were approved for this intraday pulse.</p>
            </div>
            """

        # Build rejected and vetoed block
        audit_html = ""
        # Let's collect all analyzed symbols (either in initial_candidates or critic_results)
        all_symbols = list(set(initial_candidates + list(critic_results.keys())))
        
        # Filter out the approved ones from this list
        non_approved_symbols = [s for s in all_symbols if s not in approved_allocations]
        
        if non_approved_symbols:
            audit_html += f"""
            <div style="margin-top: 30px; margin-bottom: 30px;">
                <h3 style="color: #8b949e; font-size: 16px; text-transform: uppercase; margin-bottom: 15px; border-bottom: 1px solid #30363d; padding-bottom: 5px;">🛡️ Cognitive Audit & Veto Registry</h3>
            """
            for symbol in sorted(non_approved_symbols):
                eval_data = critic_results.get(symbol, {})
                score = eval_data.get("total_confidence")
                
                if eval_data:
                    # It reached the Critic but failed/vetoed
                    veto_reason = eval_data.get("veto_reason", "Failed Pring Momentum Validation")
                    status_badge = "🛡️ CRITIC VETOED"
                    badge_color = "#f85149"
                    bg_color = "rgba(248, 81, 73, 0.04)"
                    border_style = "1px solid rgba(248, 81, 73, 0.3)"
                else:
                    # It was filtered earlier
                    veto_reason = "Failed Pring Stage 2 or Momentum Ignition requirements prior to cognitive audit."
                    status_badge = "⚙️ ENGINE FILTERED"
                    badge_color = "#ffca28"
                    bg_color = "rgba(255, 202, 40, 0.02)"
                    border_style = "1px solid rgba(255, 202, 40, 0.2)"
                    score = "N/A"
                
                score_str = f"{score:.1f}" if isinstance(score, float) else str(score)

                audit_html += f"""
                <div style="background: {bg_color}; border: {border_style}; border-radius: 6px; padding: 15px; margin-bottom: 10px;">
                    <table width="100%">
                        <tr>
                            <td>
                                <span style="font-size: 10px; font-weight: bold; color: {badge_color}; text-transform: uppercase; padding: 2px 6px; background: rgba(255, 255, 255, 0.05); border-radius: 3px; display: inline-block; margin-bottom: 5px;">{status_badge}</span>
                                <h4 style="margin: 0; color: #ffffff; font-size: 16px;">{symbol}</h4>
                            </td>
                            <td style="text-align: right; vertical-align: top;">
                                <div style="font-size: 10px; color: #8b949e; text-transform: uppercase;">Confidence</div>
                                <div style="font-size: 16px; font-weight: bold; color: #c9d1d9;">{score_str}</div>
                            </td>
                        </tr>
                    </table>
                    <p style="margin: 10px 0 0 0; font-size: 13px; color: #8b949e; line-height: 1.4;">
                        <b>Reason:</b> {veto_reason}
                    </p>
                </div>
                """
            audit_html += "</div>"

        # Construct final HTML
        html = f"""
        <html>
        <body style="font-family: 'Inter', 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #0b0e14; padding: 20px; color: #e1e8ed;">
            <div style="max-width: 650px; margin: auto; background: #151921; padding: 40px; border-radius: 16px; border: 1px solid #2d333b; box-shadow: 0 10px 30px rgba(0,0,0,0.5);">
                
                <!-- HEADER -->
                <div style="text-align: center; border-bottom: 1px solid #2d333b; padding-bottom: 25px; margin-bottom: 30px;">
                    <span style="font-size: 11px; font-weight: bold; color: {header_color}; letter-spacing: 2px; text-transform: uppercase; background: rgba(255,255,255,0.03); padding: 5px 12px; border-radius: 20px; border: 1px solid #30363d; display: inline-block; margin-bottom: 15px;">{summary_badge}</span>
                    <h1 style="color: #ffffff; font-size: 28px; margin: 0; letter-spacing: 1px;">SOVEREIGN INTELLIGENCE</h1>
                    <p style="color: #8b949e; font-size: 13px; margin-top: 5px; text-transform: uppercase; letter-spacing: 1px;">Pulse #{pulse_number} Execution Report</p>
                </div>
                
                <!-- SUMMARY BANNER -->
                <div style="background: #1f242d; border-radius: 8px; padding: 20px; margin-bottom: 30px; border: 1px solid #30363d;">
                    <table width="100%">
                        <tr>
                            <td>
                                <p style="margin: 0; font-size: 11px; color: #58a6ff; font-weight: bold; text-transform: uppercase; letter-spacing: 0.5px;">Macro Regime</p>
                                <p style="margin: 3px 0 0 0; color: #ffffff; font-size: 16px; font-weight: bold;">🏛️ {macro_regime.upper()}</p>
                            </td>
                            <td style="text-align: right;">
                                <p style="margin: 0; font-size: 11px; color: #8b949e; font-weight: bold; text-transform: uppercase; letter-spacing: 0.5px;">Database Universe</p>
                                <p style="margin: 3px 0 0 0; color: #ffffff; font-size: 16px; font-weight: bold;">{total_screened if total_screened else 499} Screened</p>
                            </td>
                        </tr>
                    </table>
                    <div style="margin-top: 15px; border-top: 1px solid #30363d; padding-top: 15px; font-size: 12px; color: #8b949e;">
                        <table width="100%">
                            <tr>
                                <td><b>Stage 2 & Momentum Ignition:</b></td>
                                <td style="text-align: right; color: #ffffff; font-weight: bold;">{len(all_symbols)} passed to Cognitive Agent</td>
                            </tr>
                        </table>
                    </div>
                    <p style="margin: 15px 0 0 0; border-top: 1px solid #30363d; padding-top: 15px; color: #c9d1d9; font-size: 14px; line-height: 1.5;">
                        {summary_text}
                    </p>
                </div>
                
                <!-- APPROVED SETUPS SECTION -->
                {approved_html}
                
                <!-- AUDIT REGISTRY SECTION -->
                {audit_html}
                
                <!-- FOOTER -->
                <div style="text-align: center; margin-top: 40px; border-top: 1px solid #2d333b; padding-top: 25px;">
                    <p style="font-size: 11px; color: #484f58; margin: 0; line-height: 1.6;">
                        CONFIDENTIAL SYSTEM DIAGNOSTIC REPORT &bull; FOR INTERNAL AUDIT ONLY<br>
                        Engineered by Antigravity under Pring Codex Compliance v4.0.
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
                logging.info(f"✅ Pulse Scorecard Email sent successfully to {self.target}")
        except Exception as e:
            logging.error(f"❌ Failed to send pulse report email: {e}")

    def send_live_alert(self, subject: str, message_html: str):
        """
        Sends an immediate live execution alert (e.g. SL Hit, Smart Exit, Margin Failure)
        """
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"🏛️ SOVEREIGN ALERT: {subject}"
        msg["From"] = f"Sovereign Engine <{self.username}>"
        msg["To"] = self.target

        # Create HTML Content
        html = f"""
        <html>
        <body style="font-family: 'Inter', 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #0b0e14; padding: 20px; color: #e1e8ed;">
            <div style="max-width: 600px; margin: auto; background: #151921; padding: 30px; border-radius: 12px; border: 1px solid #2d333b;">
                <div style="border-bottom: 1px solid #2d333b; padding-bottom: 15px; margin-bottom: 20px;">
                    <h2 style="color: #ffffff; font-size: 20px; margin: 0;">{subject}</h2>
                    <p style="color: #8b949e; font-size: 12px; margin-top: 5px;">Live Execution Engine Notification</p>
                </div>
                
                <div style="font-size: 14px; line-height: 1.6; color: #c9d1d9;">
                    {message_html}
                </div>
                
                <div style="margin-top: 30px; border-top: 1px solid #2d333b; padding-top: 15px; text-align: center;">
                    <p style="font-size: 10px; color: #484f58; margin: 0;">
                        Generated by Midnight Sovereign - Automated Option Desk
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
                logging.info(f"✅ Live Alert Email sent: {subject}")
        except Exception as e:
            logging.error(f"❌ Failed to send live alert email: {e}")

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
