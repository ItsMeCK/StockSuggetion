import os
import json
from openai import OpenAI

from dotenv import load_dotenv
load_dotenv()

def brainstorm_architecture():
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    
    # Load Context
    with open("scratch/pring_extracted_rules.txt", "r") as f:
        pring_rules = f.read()[:10000] # Take first 10k chars for context
        
    with open("docs/weekly_winner_miss_analysis.csv", "r") as f:
        missed_data = f.read()
        
    with open("pipeline/screener.py", "r") as f:
        screener_code = f.read()

    prompt = f"""
    You are the Sovereign Architect. 
    Our current system missed +12% to +24% winners this week (ADANIENT, SOLARINDS, SAREGAMA) because it flagged them as 'Over-Extended' (>12% from 50 SMA).
    
    CONTEXT:
    1. MARTIN PRING'S PHILOSOPHY (Extracted):
    {pring_rules}
    
    2. MISSED WINNERS DATA:
    {missed_data}
    
    3. CURRENT SCREENER LOGIC:
    {screener_code}
    
    TASK:
    Analyze if we need a new Agent or a 'Decision Agent' (Conditional Edge) to handle these momentum leaders.
    Should we have a 'Dynamic Momentum Gate' that relaxes the extension limit if certain Pring conditions (Volume dry-up followed by thrust) are met?
    
    PROPOSE:
    1. A new Agent or a change in the LangGraph flow (Decision Node).
    2. Specific logic for 'Conditional Thresholds' based on Market Regime.
    3. How to use Martin Pring's 'Price Pattern' wisdom to distinguish between 'Dangerous Chasing' and 'Early Stage 2 Momentum'.
    
    Return a structured proposal.
    """
    
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}]
    )
    
    with open("docs/architecture_v4_proposal.md", "w") as f:
        f.write(response.choices[0].message.content)
    
    print("Architecture Proposal generated: docs/architecture_v4_proposal.md")

if __name__ == "__main__":
    brainstorm_architecture()
