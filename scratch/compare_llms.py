import os
import json
import logging
from agents.pattern_agent import VisionPatternAgent

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def compare_providers(symbol, target_date):
    print(f"\n{'='*60}")
    print(f"HEAD-TO-HEAD AUDIT: {symbol} on {target_date}")
    print(f"{'='*60}")
    
    results = {}
    
    # Temporarily override config by setting env var
    providers = ["GROQ", "OPENAI"]
    
    for provider in providers:
        import core.config
        core.config.LLM_PROVIDER = provider
        # Force re-init the agent to pick up the provider
        agent = VisionPatternAgent()
        
        # Bypass cache to get fresh results
        print(f"\n--- Running via {provider} ---")
        
        # We need to reach into the agent and bypass cache
        # Let's temporarily clear the cache for this symbol in the instance
        cache_key_start = f"{symbol}"
        keys_to_del = [k for k in agent.cache if k.startswith(cache_key_start)]
        for k in keys_to_del:
            del agent.cache[k]
            
        res = agent.analyze_chart(symbol, "hint", target_date)
        results[provider] = res
        print(f"Score: {res['vision_score']}")
        print(f"Pattern: {res['identified_pattern']}")
        print(f"Reason: {res['reason'][:200]}...")

    print(f"\n{'='*60}")
    print(f"VERDICT for {symbol}:")
    diff = results["OPENAI"]["vision_score"] - results["GROQ"]["vision_score"]
    print(f"Score Delta (OpenAI - Groq): {diff}")
    if results["OPENAI"]["vision_score"] < 70 and results["GROQ"]["vision_score"] >= 70:
        print("RESULT: OpenAI would have correctly REJECTED this setup.")
    elif results["OPENAI"]["vision_score"] >= 70 and results["GROQ"]["vision_score"] < 70:
        print("RESULT: Groq correctly REJECTED this setup while OpenAI approved.")
    else:
        print("RESULT: Both providers agreed on the signal quality.")
    print(f"{'='*60}")

if __name__ == "__main__":
    # Test on a known loss symbol from early Feb
    test_cases = [
        ("3MINDIA", "2026-02-26"),
        ("COSMOFIRST", "2026-03-05"),
        ("VALIANTORG", "2026-02-05")
    ]
    for sym, dt in test_cases:
        compare_providers(sym, dt)
