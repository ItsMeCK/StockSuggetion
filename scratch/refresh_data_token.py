import sys
from kiteconnect import KiteConnect

api_key = "anywvvfkcyjhhqiy"
api_secret = "7gp1xufivozw5xhcaqe1k9hbu3mnqnk1"
request_token = "vUgoxkyYExbdsxebDz2tWStYqsALdaFC"

try:
    kite = KiteConnect(api_key=api_key)
    data = kite.generate_session(request_token, api_secret=api_secret)
    access_token = data["access_token"]
    print(f"SUCCESS_TOKEN:{access_token}")
except Exception as e:
    print(f"ERROR: {e}")
