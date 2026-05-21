import os
import sys
from dotenv import load_dotenv

# Add workspace root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

load_dotenv()

from agents.news_catalyst_agent import NewsCatalystAgent

def main():
    agent = NewsCatalystAgent()
    
    # Test symbols
    test_symbols = ["APOLLOHOSP", "HONAUT"]
    
    for symbol in test_symbols:
        print(f"\n==========================================")
        print(f"Testing Catalyst Detection for: {symbol}")
        print(f"==========================================")
        
        # 1. Test headlines fetching
        headlines = agent.fetch_recent_headlines(symbol)
        print(f"\nRetrieved {len(headlines)} headlines:")
        for idx, headline in enumerate(headlines[:5], 1):
            print(f"{idx}. {headline}")
            
        # 2. Test OpenAI classification
        if headlines:
            print("\nEvaluating catalysts via OpenAI...")
            result = agent.evaluate_news_catalysts(symbol)
            print(f"Result: {result}")
        else:
            print("\nNo headlines to evaluate.")

if __name__ == "__main__":
    main()
