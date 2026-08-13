import os
from dotenv import load_dotenv
load_dotenv()
from agents.llm_ranking_agent import LLMRankingAgent

symbols = ['KEI', 'CAMS', 'GODFRYPHLP', 'SIEMENS', 'BLUESTARCO']
print(f"Running LLM on: {symbols}")

agent = LLMRankingAgent()
rankings = agent.rank_trades(symbols)

print("\n🚀 FINAL LLM EXECUTION SIGNALS 🚀")
if rankings:
    for i, rank in enumerate(rankings[:2]):
        print(f"Rank {i+1}: {rank['symbol']} | Score: {rank['conviction_score']}")
        print(f"Catalyst: {rank['catalyst_summary']}\n")
else:
    print("LLM rejected all candidates.")
