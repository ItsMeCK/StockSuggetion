from alerts.email_notifier import SovereignEmailer
import os

print(f"MAIL_SERVER: {os.getenv('MAIL_SERVER')}")
print(f"TARGET: {os.getenv('TARGET_EMAIL')}")

emailer = SovereignEmailer()
mock_trades = [
    {
        "ticker": "RELIANCE",
        "score": 92.5,
        "passed": ["High Volume Breakout", "RSI Golden Cross"]
    },
    {
        "ticker": "HDFCBANK",
        "score": 85.0,
        "passed": ["Institutional Accumulation Phase"]
    }
]

print("Attempting to send test email...")
try:
    emailer.send_scorecard("TEST: Live Hourly Execution (09:26)", mock_trades)
    print("SUCCESS: Test email sent!")
except Exception as e:
    print(f"ERROR: {e}")
