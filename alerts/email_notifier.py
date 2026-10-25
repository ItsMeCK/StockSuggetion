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
        self.target_string = os.getenv("TARGET_EMAIL", "")
        self.target_list = [email.strip() for email in self.target_string.split(",")]
        
        import socket
        hostname = socket.gethostname()
        self.env_prefix = "[GCP]" if "sovereign-engine" in hostname else "[LOCAL]"

    def send_scorecard(self, subject: str, signals: list, rejected_signals: list = None):
        """
        Signals is a list of dicts from the Librarian.
        Sorted by score descending.
        """
        if not rejected_signals:
            rejected_signals = []
            
        if not signals and not rejected_signals:
            return

        # Sort signals by score descending and limit to Top 5
        signals = sorted(signals, key=lambda x: x['score'], reverse=True)[:5]
        
        logging.info(f"📤 SENDING EMAIL: {subject} | TICKERS: {[s['ticker'] for s in signals]} | REJECTED: {[s['ticker'] for s in rejected_signals]}")

        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"{self.env_prefix} 🏛️ SOVEREIGN: {subject}"
        msg["From"] = f"Sovereign Engine <{self.username}>"
        msg["To"] = self.target_string

        summary_msg = f"The AI Arbiter has identified <b>{len(signals)}</b> high-conviction targets." if signals else "No candidates cleared the strict conviction thresholds. Capital preserved."

        # Create HTML Content
        html = f"""
        <html>
        <body style="font-family: 'Inter', 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #0b0e14; padding: 20px; color: #e1e8ed;">
            <div style="max-width: 650px; margin: auto; background: #151921; padding: 40px; border-radius: 16px; border: 1px solid #2d333b; box-shadow: 0 10px 30px rgba(0,0,0,0.5);">
                <div style="text-align: center; border-bottom: 1px solid #2d333b; padding-bottom: 20px; margin-bottom: 30px;">
                    <h1 style="color: #ffffff; font-size: 28px; margin: 0; letter-spacing: 1px;">SOVEREIGN INTELLIGENCE</h1>
                    <p style="color: #8b949e; font-size: 14px; margin-top: 5px;">Live Execution Report</p>
                </div>
                
                <div style="background: #1f242d; border-radius: 8px; padding: 15px; margin-bottom: 30px; border: 1px solid #30363d;">
                    <p style="margin: 0; font-size: 13px; color: #58a6ff; font-weight: bold; text-transform: uppercase;">Executive Summary</p>
                    <p style="margin: 5px 0 0 0; color: #c9d1d9; font-size: 15px;">
                        {summary_msg}
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
                                <div style="font-size: 10px; color: #8b949e; text-transform: uppercase;">Conviction Score</div>
                            </td>
                        </tr>
                    </table>
                    
                    <div style="margin-top: 20px; border-top: 1px solid #30363d; padding-top: 15px;">
                        <p style="margin: 5px 0; color: #c9d1d9; font-size: 13px;"><b>✅ CATALYST:</b> {", ".join(s['passed'][:5])}...</p>
                    </div>
                </div>
            """
            
        if rejected_signals:
            html += """
                <div style="margin-top: 30px; margin-bottom: 30px;">
                    <h3 style="color: #8b949e; font-size: 16px; text-transform: uppercase; margin-bottom: 15px; border-bottom: 1px solid #30363d; padding-bottom: 5px;">🛡️ Cognitive Audit & Veto Registry</h3>
            """
            for s in rejected_signals:
                html += f"""
                <div style="background: rgba(248, 81, 73, 0.04); border: 1px solid rgba(248, 81, 73, 0.3); border-radius: 6px; padding: 15px; margin-bottom: 10px;">
                    <table width="100%">
                        <tr>
                            <td>
                                <span style="font-size: 10px; font-weight: bold; color: #f85149; text-transform: uppercase; padding: 2px 6px; background: rgba(255, 255, 255, 0.05); border-radius: 3px; display: inline-block; margin-bottom: 5px;">🛡️ REJECTED</span>
                                <h4 style="margin: 0; color: #ffffff; font-size: 16px;">{s['ticker']}</h4>
                            </td>
                            <td style="text-align: right; vertical-align: top;">
                                <div style="font-size: 10px; color: #8b949e; text-transform: uppercase;">Score</div>
                                <div style="font-size: 16px; font-weight: bold; color: #c9d1d9;">{s['score']}</div>
                            </td>
                        </tr>
                    </table>
                    <p style="margin: 10px 0 0 0; font-size: 13px; color: #8b949e; line-height: 1.4;">
                        <b>Reason:</b> {s['reason']}
                    </p>
                </div>
                """
            html += "</div>"

        # Attach Active Focus Watchlist to each Scorecard Email
        html += self.build_watchlist_html()

        html += """
                <div style="text-align: center; margin-top: 40px; border-top: 1px solid #2d333b; padding-top: 20px;">
                    <p style="font-size: 11px; color: #484f58; margin: 0;">
                        CONFIDENTIAL INSTITUTIONAL RESEARCH<br>
                        This report is generated using real-time live market data.
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
                server.sendmail(self.username, self.target_list, msg.as_string())
                logging.info(f"Email Signal Report sent successfully to {self.target_string}")
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

        msg["Subject"] = f"{self.env_prefix} 🏛️ SOVEREIGN: {subject}"
        msg["From"] = f"Sovereign Engine <{self.username}>"
        msg["To"] = self.target_string

        # Limit to top 2 by conviction score
        if approved_allocations:
            sorted_allocs = sorted(approved_allocations.items(), key=lambda item: item[1].get('conviction_score', 0), reverse=True)[:2]
            approved_allocations = dict(sorted_allocs)

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
                server.sendmail(self.username, self.target_list, msg.as_string())
                logging.info(f"✅ Pulse Scorecard Email sent successfully to {self.target_string}")
        except Exception as e:
            logging.error(f"❌ Failed to send pulse report email: {e}")

    def send_live_alert(self, subject: str, message_html: str):
        """
        Sends an immediate live execution alert (e.g. SL Hit, Smart Exit, Margin Failure)
        """
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"{self.env_prefix} 🏛️ SOVEREIGN ALERT: {subject}"
        msg["From"] = f"Sovereign Engine <{self.username}>"
        msg["To"] = self.target_string

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
                
                {self.build_watchlist_html()}
                
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
                server.sendmail(self.username, self.target_list, msg.as_string())
                logging.info(f"✅ Live Alert Email sent: {subject}")
        except Exception as e:
            logging.error(f"❌ Failed to send live alert email: {e}")

    def build_watchlist_html(self, focus_watchlist: list = None) -> str:
        """
        Renders an institutional HTML card containing the current Active Focus Watchlist.
        Segregates candidates into:
        1. 🟢 Call Option (CE) Focus List (Bullish Momentum & Expansion)
        2. 🔴 Put Option (PE) Focus List (Bearish Breakdown & Short Targets)
        Can be dynamically appended to any email (signals, alerts, pulses).
        """
        if focus_watchlist is None:
            try:
                from core.db_manager import get_active_daily_focus_watchlist
                focus_watchlist = get_active_daily_focus_watchlist(min_score=50.0)
            except Exception as e:
                logging.debug(f"Could not load active focus watchlist for email: {e}")
                focus_watchlist = []

        if not focus_watchlist:
            return ""

        ce_candidates = [s for s in focus_watchlist if str(s.get("direction", "LONG")).upper() == "LONG"]
        pe_candidates = [s for s in focus_watchlist if str(s.get("direction", "LONG")).upper() != "LONG"]

        def render_table_rows(candidates_list, is_call=True):
            rows = ""
            for s in candidates_list:
                sym = s.get("symbol", "")
                direction = str(s.get("direction", "LONG" if is_call else "SHORT")).upper()
                score = float(s.get("conviction_score", 0.0))
                catalyst = s.get("catalyst_summary", "High institutional conviction")
                if len(catalyst) > 95:
                    catalyst = catalyst[:92] + "..."

                dir_color = "#3fb950" if is_call else "#f85149"
                dir_bg = "rgba(63, 185, 80, 0.15)" if is_call else "rgba(248, 81, 73, 0.15)"
                inst_tag = "CE" if is_call else "PE"

                rows += f"""
                <tr style="border-bottom: 1px solid #21262d;">
                    <td style="padding: 10px 8px; font-weight: bold; color: #ffffff; font-size: 13px;">{sym}</td>
                    <td style="padding: 10px 8px;">
                        <span style="font-size: 11px; font-weight: bold; color: {dir_color}; background: {dir_bg}; padding: 2px 7px; border-radius: 4px; display: inline-block;">
                            {direction} &bull; {inst_tag}
                        </span>
                    </td>
                    <td style="padding: 10px 8px; font-weight: bold; color: #ffca28; font-size: 13px; text-align: center;">{score:.0f}</td>
                    <td style="padding: 10px 8px; font-size: 12px; color: #8b949e; line-height: 1.4;">{catalyst}</td>
                </tr>
                """
            return rows

        ce_rows = render_table_rows(ce_candidates, is_call=True)
        pe_rows = render_table_rows(pe_candidates, is_call=False)

        ce_section = f"""
        <div style="margin-bottom: 20px;">
            <div style="background: rgba(63, 185, 80, 0.08); border: 1px solid rgba(63, 185, 80, 0.25); border-radius: 6px; padding: 8px 12px; margin-bottom: 8px;">
                <span style="font-size: 12px; font-weight: bold; color: #3fb950; text-transform: uppercase;">
                    🟢 Call Option (CE) Focus Candidates ({len(ce_candidates)})
                </span>
                <span style="font-size: 11px; color: #8b949e; margin-left: 8px;">Bullish Momentum & Ignition Targets</span>
            </div>
            <table width="100%" style="border-collapse: collapse; text-align: left;">
                <thead>
                    <tr style="border-bottom: 1px solid #30363d; font-size: 10px; color: #8b949e; text-transform: uppercase;">
                        <th style="padding: 6px 8px;">Symbol</th>
                        <th style="padding: 6px 8px;">Bias</th>
                        <th style="padding: 6px 8px; text-align: center;">Score</th>
                        <th style="padding: 6px 8px;">Catalyst / Setup</th>
                    </tr>
                </thead>
                <tbody>
                    {ce_rows if ce_rows else '<tr><td colspan="4" style="padding: 8px; color: #8b949e; font-size: 12px; text-align: center;">No active CE candidates flagged.</td></tr>'}
                </tbody>
            </table>
        </div>
        """

        pe_section = f"""
        <div style="margin-bottom: 10px;">
            <div style="background: rgba(248, 81, 73, 0.08); border: 1px solid rgba(248, 81, 73, 0.25); border-radius: 6px; padding: 8px 12px; margin-bottom: 8px;">
                <span style="font-size: 12px; font-weight: bold; color: #f85149; text-transform: uppercase;">
                    🔴 Put Option (PE) Focus Candidates ({len(pe_candidates)})
                </span>
                <span style="font-size: 11px; color: #8b949e; margin-left: 8px;">Bearish Breakdown & Short Targets</span>
            </div>
            <table width="100%" style="border-collapse: collapse; text-align: left;">
                <thead>
                    <tr style="border-bottom: 1px solid #30363d; font-size: 10px; color: #8b949e; text-transform: uppercase;">
                        <th style="padding: 6px 8px;">Symbol</th>
                        <th style="padding: 6px 8px;">Bias</th>
                        <th style="padding: 6px 8px; text-align: center;">Score</th>
                        <th style="padding: 6px 8px;">Catalyst / Setup</th>
                    </tr>
                </thead>
                <tbody>
                    {pe_rows if pe_rows else '<tr><td colspan="4" style="padding: 8px; color: #8b949e; font-size: 12px; text-align: center;">No active PE candidates flagged.</td></tr>'}
                </tbody>
            </table>
        </div>
        """

        html = f"""
        <div style="margin-top: 30px; margin-bottom: 25px; background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px;">
            <div style="border-bottom: 1px solid #30363d; padding-bottom: 10px; margin-bottom: 15px;">
                <table width="100%">
                    <tr>
                        <td>
                            <h3 style="margin: 0; color: #58a6ff; font-size: 14px; text-transform: uppercase; letter-spacing: 1px;">
                                🎯 Active Focus Watchlist ({len(focus_watchlist)} Tickers)
                            </h3>
                        </td>
                        <td style="text-align: right; color: #8b949e; font-size: 11px;">
                            Segregated by Option Mandate (CE / PE)
                        </td>
                    </tr>
                </table>
            </div>
            {ce_section}
            {pe_section}
        </div>
        """
        return html

    def send_morning_watchlist_report(self, focus_watchlist: list, themes: dict = None, review_or_pruned: list = None):
        """
        Sends the institutional 09:08 AM Pre-Open Focus Watchlist Email.
        Fires immediately after NSE Pre-Open equilibrium discovery (09:08 AM).
        """
        if not themes:
            themes = {}
        if not review_or_pruned:
            review_or_pruned = []

        from datetime import datetime
        today_str = datetime.now().strftime("%d-%b-%Y")
        subject = f"09:08 AM Pre-Open Focus Watchlist & Market Regime ({today_str})"
        
        logging.info(f"📤 Preparing 09:08 AM Pre-Open Watchlist Email for {len(focus_watchlist)} active symbols...")
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"{self.env_prefix} 🏛️ SOVEREIGN: {subject}"
        msg["From"] = f"Sovereign Engine <{self.username}>"
        msg["To"] = self.target_string

        sentiment = themes.get("market_sentiment", "NEUTRAL").upper()
        sentiment_color = "#3fb950" if sentiment == "BULLISH" else ("#f85149" if sentiment == "BEARISH" else "#58a6ff")
        strong_sectors = ", ".join(themes.get("strong_sectors", ["Automobile", "Capital Goods"]))
        weak_sectors = ", ".join(themes.get("weak_sectors", ["IT"]))
        narrative = themes.get("narrative_summary", "System operating on quantitative sector baseline.")

        # Build Watchlist Table
        watchlist_html = self.build_watchlist_html(focus_watchlist)

        # Build Review / Pruned HTML
        review_html = ""
        if review_or_pruned:
            review_html += """
            <div style="margin-top: 25px; margin-bottom: 20px;">
                <h4 style="color: #8b949e; font-size: 12px; text-transform: uppercase; margin-bottom: 10px;">
                    🛡️ Disqualified / Routed to Review
                </h4>
            """
            for item in review_or_pruned[:6]:
                sym = item.get("symbol", "")
                status = item.get("status", "REVIEW")
                reason = item.get("route_reason", "Excessive gap or earnings pending")
                badge_color = "#f85149" if status == "PRUNED" else "#ffca28"
                review_html += f"""
                <div style="background: rgba(255,255,255,0.02); border: 1px solid #30363d; border-radius: 6px; padding: 10px; margin-bottom: 6px;">
                    <span style="font-size: 10px; font-weight: bold; color: {badge_color}; text-transform: uppercase;">[{status}]</span>
                    <strong style="color: #ffffff; font-size: 13px; margin-left: 6px;">{sym}</strong>:
                    <span style="color: #8b949e; font-size: 12px;">{reason}</span>
                </div>
                """
            review_html += "</div>"

        html = f"""
        <html>
        <body style="font-family: 'Inter', 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #0b0e14; padding: 20px; color: #e1e8ed;">
            <div style="max-width: 650px; margin: auto; background: #151921; padding: 35px; border-radius: 16px; border: 1px solid #2d333b; box-shadow: 0 10px 30px rgba(0,0,0,0.5);">
                
                <!-- HEADER -->
                <div style="text-align: center; border-bottom: 1px solid #2d333b; padding-bottom: 20px; margin-bottom: 25px;">
                    <span style="font-size: 11px; font-weight: bold; color: #58a6ff; letter-spacing: 2px; text-transform: uppercase; background: rgba(88, 166, 255, 0.1); padding: 4px 10px; border-radius: 12px; border: 1px solid #30363d; display: inline-block; margin-bottom: 10px;">PRE-OPEN DISCOVERY</span>
                    <h1 style="color: #ffffff; font-size: 26px; margin: 0; letter-spacing: 1px;">SOVEREIGN INTELLIGENCE</h1>
                    <p style="color: #8b949e; font-size: 13px; margin-top: 5px;">Daily Focus Watchlist & Sector Grounding &bull; {today_str}</p>
                </div>

                <!-- MACRO SUMMARY -->
                <div style="background: #1f242d; border-radius: 8px; padding: 18px; margin-bottom: 25px; border: 1px solid #30363d;">
                    <table width="100%">
                        <tr>
                            <td>
                                <p style="margin: 0; font-size: 11px; color: #8b949e; text-transform: uppercase;">Macro Sentiment</p>
                                <p style="margin: 2px 0 0 0; color: {sentiment_color}; font-size: 16px; font-weight: bold;">🏛️ {sentiment}</p>
                            </td>
                            <td style="text-align: right;">
                                <p style="margin: 0; font-size: 11px; color: #8b949e; text-transform: uppercase;">Active Focus Tickers</p>
                                <p style="margin: 2px 0 0 0; color: #ffffff; font-size: 16px; font-weight: bold;">{len(focus_watchlist)} Selected</p>
                            </td>
                        </tr>
                    </table>
                    <div style="margin-top: 12px; border-top: 1px solid #30363d; padding-top: 12px; font-size: 12px; color: #c9d1d9;">
                        <p style="margin: 3px 0;"><b>🟢 Institutional Inflows:</b> {strong_sectors}</p>
                        <p style="margin: 3px 0;"><b>🔴 Outflows / Pressured:</b> {weak_sectors}</p>
                    </div>
                    <p style="margin: 12px 0 0 0; border-top: 1px solid #30363d; padding-top: 10px; color: #8b949e; font-size: 13px; line-height: 1.4;">
                        <b>Executive Brief:</b> {narrative}
                    </p>
                </div>

                <!-- WATCHLIST SECTION -->
                {watchlist_html}

                <!-- DISQUALIFIED SECTION -->
                {review_html}

                <!-- FOOTER -->
                <div style="text-align: center; margin-top: 35px; border-top: 1px solid #2d333b; padding-top: 18px;">
                    <p style="font-size: 11px; color: #484f58; margin: 0; line-height: 1.5;">
                        CONFIDENTIAL PRE-MARKET STRATEGY BRIEFING<br>
                        Automated 5-minute Precision Trigger Engine engages at 09:30 AM IST.
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
                server.sendmail(self.username, self.target_list, msg.as_string())
                logging.info(f"✅ 09:08 AM Pre-Open Watchlist Email sent to {self.target_string}")
        except Exception as e:
            logging.error(f"❌ Failed to send pre-open watchlist email: {e}")

    def send_weekly_universe_report(self, eligible_candidates: list, banned_count: int = 0, themes: dict = None, focus_candidates: dict = None):
        """
        Sends the Sunday 18:00 IST Weekly Eligible Universe & Strategy Briefing Email.
        Renders segregated CE and PE Focus Leaders aligned with macro sector tailwinds.
        """
        if not themes:
            themes = {}
        if not focus_candidates:
            focus_candidates = {}

        from datetime import datetime
        today_str = datetime.now().strftime("%d-%b-%Y")
        subject = f"Weekly Eligible Universe & Macro Strategy Briefing ({today_str})"
        
        logging.info(f"📤 Preparing Weekly Universe Email for {len(eligible_candidates)} eligible symbols...")
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"{self.env_prefix} 🏛️ SOVEREIGN: {subject}"
        msg["From"] = f"Sovereign Engine <{self.username}>"
        msg["To"] = self.target_string

        sentiment = themes.get("market_sentiment", "NEUTRAL").upper()
        sentiment_color = "#3fb950" if sentiment == "BULLISH" else ("#f85149" if sentiment == "BEARISH" else "#58a6ff")
        strong_sectors = ", ".join(themes.get("strong_sectors", ["Automobile", "Capital Goods"]))
        weak_sectors = ", ".join(themes.get("weak_sectors", ["IT"]))
        narrative = themes.get("narrative_summary", "Weekly universe filtered against F&O Ban list and low DTE.")

        ce_list = focus_candidates.get("ce_focus", [])
        pe_list = focus_candidates.get("pe_focus", [])

        # Build Focus Tables HTML
        ce_rows = ""
        for cand in ce_list:
            sym = cand.get("symbol", "")
            sec = cand.get("sector", "NIFTY")
            atr = cand.get("atr_pct", 2.5)
            ce_rows += f"""
            <tr style="border-bottom: 1px solid #21262d;">
                <td style="padding: 8px; font-weight: bold; color: #ffffff; font-size: 13px;">{sym}</td>
                <td style="padding: 8px; color: #8b949e; font-size: 12px;">{sec}</td>
                <td style="padding: 8px; color: #3fb950; font-size: 12px; font-weight: bold;">{atr:.2f}%</td>
                <td style="padding: 8px; font-size: 12px;"><span style="color: #3fb950; background: rgba(63, 185, 80, 0.15); padding: 2px 8px; border-radius: 4px; font-weight: bold;">🟢 CE / CALL FOCUS</span></td>
            </tr>
            """

        pe_rows = ""
        for cand in pe_list:
            sym = cand.get("symbol", "")
            sec = cand.get("sector", "NIFTY")
            atr = cand.get("atr_pct", 2.5)
            pe_rows += f"""
            <tr style="border-bottom: 1px solid #21262d;">
                <td style="padding: 8px; font-weight: bold; color: #ffffff; font-size: 13px;">{sym}</td>
                <td style="padding: 8px; color: #8b949e; font-size: 12px;">{sec}</td>
                <td style="padding: 8px; color: #f85149; font-size: 12px; font-weight: bold;">{atr:.2f}%</td>
                <td style="padding: 8px; font-size: 12px;"><span style="color: #f85149; background: rgba(248, 81, 73, 0.15); padding: 2px 8px; border-radius: 4px; font-weight: bold;">🔴 PE / PUT FOCUS</span></td>
            </tr>
            """

        focus_section_html = ""
        if ce_rows or pe_rows:
            focus_section_html = f"""
            <!-- CE FOCUS TABLE -->
            <div style="background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; margin-bottom: 20px;">
                <h3 style="margin: 0 0 12px 0; color: #3fb950; font-size: 13px; text-transform: uppercase; letter-spacing: 1px;">
                    🟢 Top Weekly Call (CE) Focus Candidates ({len(ce_list)} Tickers &bull; Sector Tailwinds)
                </h3>
                <table width="100%" style="border-collapse: collapse; text-align: left;">
                    <thead>
                        <tr style="border-bottom: 1px solid #30363d; font-size: 10px; color: #8b949e; text-transform: uppercase;">
                            <th style="padding: 6px 8px;">Symbol</th>
                            <th style="padding: 6px 8px;">Sector</th>
                            <th style="padding: 6px 8px;">20D ATR%</th>
                            <th style="padding: 6px 8px;">Mandate</th>
                        </tr>
                    </thead>
                    <tbody>
                        {ce_rows}
                    </tbody>
                </table>
            </div>

            <!-- PE FOCUS TABLE -->
            <div style="background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; margin-bottom: 20px;">
                <h3 style="margin: 0 0 12px 0; color: #f85149; font-size: 13px; text-transform: uppercase; letter-spacing: 1px;">
                    🔴 Top Weekly Put (PE) Focus Candidates ({len(pe_list)} Tickers &bull; Sector Pressures)
                </h3>
                <table width="100%" style="border-collapse: collapse; text-align: left;">
                    <thead>
                        <tr style="border-bottom: 1px solid #30363d; font-size: 10px; color: #8b949e; text-transform: uppercase;">
                            <th style="padding: 6px 8px;">Symbol</th>
                            <th style="padding: 6px 8px;">Sector</th>
                            <th style="padding: 6px 8px;">20D ATR%</th>
                            <th style="padding: 6px 8px;">Mandate</th>
                        </tr>
                    </thead>
                    <tbody>
                        {pe_rows}
                    </tbody>
                </table>
            </div>
            """
        else:
            # Fallback to general list
            rows_html = ""
            for cand in eligible_candidates[:20]:
                sym = cand.get("symbol", "")
                dte = cand.get("dte", 15)
                spread = cand.get("bid_ask_spread_pct", 0.8)
                rows_html += f"""
                <tr style="border-bottom: 1px solid #21262d;">
                    <td style="padding: 8px; font-weight: bold; color: #ffffff; font-size: 13px;">{sym}</td>
                    <td style="padding: 8px; color: #58a6ff; font-size: 12px;">{dte} Days</td>
                    <td style="padding: 8px; color: #3fb950; font-size: 12px;">{spread:.2f}%</td>
                    <td style="padding: 8px; color: #8b949e; font-size: 12px;">Eligible Weekly F&O</td>
                </tr>
                """
            focus_section_html = f"""
            <div style="background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px;">
                <h3 style="margin: 0 0 15px 0; color: #58a6ff; font-size: 14px; text-transform: uppercase;">Weekly Eligible Candidates</h3>
                <table width="100%" style="border-collapse: collapse; text-align: left;">
                    <tbody>{rows_html}</tbody>
                </table>
            </div>
            """

        html = f"""
        <html>
        <body style="font-family: 'Inter', 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #0b0e14; padding: 20px; color: #e1e8ed;">
            <div style="max-width: 650px; margin: auto; background: #151921; padding: 35px; border-radius: 16px; border: 1px solid #2d333b;">
                <div style="text-align: center; border-bottom: 1px solid #2d333b; padding-bottom: 20px; margin-bottom: 25px;">
                    <span style="font-size: 11px; font-weight: bold; color: #3fb950; letter-spacing: 2px; text-transform: uppercase; background: rgba(63, 185, 80, 0.1); padding: 4px 10px; border-radius: 12px; border: 1px solid #30363d; display: inline-block; margin-bottom: 10px;">WEEKLY UNIVERSE BUILD</span>
                    <h1 style="color: #ffffff; font-size: 26px; margin: 0; letter-spacing: 1px;">SOVEREIGN INTELLIGENCE</h1>
                    <p style="color: #8b949e; font-size: 13px; margin-top: 5px;">Week Ahead Candidate Universe &bull; {today_str}</p>
                </div>

                <div style="background: #1f242d; border-radius: 8px; padding: 18px; margin-bottom: 25px; border: 1px solid #30363d;">
                    <table width="100%">
                        <tr>
                            <td>
                                <p style="margin: 0; font-size: 11px; color: #8b949e; text-transform: uppercase;">Weekly Eligible Pool</p>
                                <p style="margin: 2px 0 0 0; color: #ffffff; font-size: 18px; font-weight: bold;">{len(eligible_candidates)} Tickers</p>
                            </td>
                            <td style="text-align: right;">
                                <p style="margin: 0; font-size: 11px; color: #8b949e; text-transform: uppercase;">F&O Ban Filtered</p>
                                <p style="margin: 2px 0 0 0; color: #f85149; font-size: 18px; font-weight: bold;">{banned_count} Excluded</p>
                            </td>
                        </tr>
                    </table>
                    <div style="margin-top: 12px; border-top: 1px solid #30363d; padding-top: 12px; font-size: 12px; color: #c9d1d9;">
                        <p style="margin: 3px 0;"><b>🟢 Institutional Tailwinds:</b> {strong_sectors}</p>
                        <p style="margin: 3px 0;"><b>🔴 Pressured Sectors:</b> {weak_sectors}</p>
                    </div>
                    <p style="margin: 12px 0 0 0; border-top: 1px solid #30363d; padding-top: 10px; color: #8b949e; font-size: 13px; line-height: 1.4;">
                        <b>Executive Brief:</b> {narrative}
                    </p>
                </div>

                {focus_section_html}

                <div style="text-align: center; margin-top: 35px; border-top: 1px solid #2d333b; padding-top: 18px;">
                    <p style="font-size: 11px; color: #484f58; margin: 0;">
                        Daily 09:08 AM Pre-Open Discovery filters this pool into 8-12 Active Focus Tickers.
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
                server.sendmail(self.username, self.target_list, msg.as_string())
                logging.info(f"✅ Weekly Universe Email sent to {self.target_string}")
        except Exception as e:
            logging.error(f"❌ Failed to send weekly universe email: {e}")

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
