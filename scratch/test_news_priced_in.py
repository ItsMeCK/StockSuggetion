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
    target_date = "2026-05-20"  # Test date matching historical context
    
    for symbol in test_symbols:
        print(f"\n==========================================")
        print(f"Testing Catalyst Detection for: {symbol}")
        print(f"==========================================")
        
        # 1. Test tech context retrieval
        print("Retrieving technical context from database...")
        tech_ctx = agent.fetch_technical_context(symbol, target_date)
        print(f"Technical Context: {tech_ctx}")
        
        # 2. Test headlines fetching (with age parsing)
        headlines = agent.fetch_recent_headlines(symbol, target_date)
        print(f"\nRetrieved {len(headlines)} headlines with ages:")
        for idx, headline in enumerate(headlines[:5], 1):
            print(f"{idx}. Age: {headline['age_hours']} hrs | Headline: {headline['title']}")
            
        # 3. Test OpenAI classification
        if headlines:
            print("\nEvaluating catalysts via OpenAI...")
            result = agent.evaluate_news_catalysts(symbol, target_date)
            import json
            print(f"Result:\n{json.dumps(result, indent=2)}")
        else:
            print("\nNo headlines to evaluate.")

if __name__ == "__main__":
    main()
